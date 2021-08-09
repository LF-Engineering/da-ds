package pipermail

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	httpNative "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	lib "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

// Fetcher contains piper mail datasource fetch logic
type Fetcher struct {
	DSName                string
	IncludeArchived       bool
	HTTPClientProvider    *http.ClientProvider
	ElasticSearchProvider *elastic.ClientProvider
	BackendVersion        string
	Debug                 int
	DateFrom              time.Time
}

// Params required parameters for piper mail fetcher
type Params struct {
	FromDate       time.Time
	BackendVersion string
	Project        string
	Debug          int
	ProjectSlug    string
	GroupName      string
}

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// ESClientProvider used in connecting to ES Client server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []elastic.BulkData) ([]byte, error)
}

// NewFetcher initiates a new pipermail fetcher
func NewFetcher(params *Params, httpClientProvider *http.ClientProvider, esClientProvider *elastic.ClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Pipermail,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
		Debug:                 params.Debug,
	}
}

//Fetch the mbox files from the remote archiver.
//
//Stores the archives in the path given during the initialization
//of this object. Those archives which don't have not valid extensions will
//be ignored.
//
//Pipermail archives have on their file names the date of
//the archive is stored following the schema year-month. When fromDate
//property is called, it will return the mboxes for which their year
//and month are equal or after that date.
//
//fromDate: fetch archives that store messages equal or after the given date; only year and month values
//are compared
//
//returns a map of links and their paths of the fetched archives
func (f *Fetcher) Fetch(url string, fromDate *time.Time) (map[string]string, error) {

	dirpath := filepath.Join(ArchiveDownloadsPath, url)
	lib.Printf("\nDownloading mboxes from %s since %s\n", url, fromDate.String())

	statusCode, _, err := f.HTTPClientProvider.Request(url, "GET", nil, nil, nil)
	if err != nil || statusCode != httpNative.StatusOK {
		return nil, err
	}

	links, err := f.ParseArchiveLinks(url, fromDate)
	if err != nil {
		return nil, err
	}

	fetched := make(map[string]string)

	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		err := os.MkdirAll(dirpath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	for _, link := range links {
		fileName := filepath.Base(link)

		mboxDT := ParseDateFromFilePath(fileName)

		if fromDate.Year() == mboxDT.Year() && fromDate.Month() == mboxDT.Month() || fromDate.Before(mboxDT) {
			filePath := filepath.Join(dirpath, fileName)

			if err := DownloadFile(link, filePath); err != nil {
				lib.Printf("error: %+v", err)
				continue
			}
			fetched[link] = filePath
		}
	}
	return fetched, nil
}

// FetchItem extracts data from archives
func (f *Fetcher) FetchItem(slug, groupName, endpoint string, fromDate time.Time, limit int, now time.Time) ([]*RawMessage, error) {
	var allMsgs []*RawMessage
	archives, err := f.Fetch(endpoint, &fromDate)
	if err != nil {
		return nil, err
	}
	var messages [][]byte

	statusCode, _, err := f.HTTPClientProvider.Request(endpoint, "GET", nil, nil, nil)
	if err != nil || statusCode != httpNative.StatusOK {
		return nil, err
	}

	for _, filePath := range archives {
		fl, err := os.Open(filePath)
		if err != nil {
			lib.Printf("os.Open: %+v", err)
			return nil, err
		}

		filename := filepath.Base(filePath)
		baseExtension := filepath.Ext(filePath)
		filename = strings.TrimSuffix(filename, filepath.Ext(filename))

		var decompressedFileContentReader *gzip.Reader
		var content []byte
		var byts []byte

		// Create new reader to decompress gzip.
		if baseExtension == ".gz" {
			decompressedFileContentReader, err = gzip.NewReader(fl)
			if err != nil {
				return nil, err
			}
		} else {
			content, err = ioutil.ReadFile(filePath)
			if err != nil {
				continue
			}

		}

		if decompressedFileContentReader != nil {
			// Read in data.
			byts, err = ioutil.ReadAll(decompressedFileContentReader)
			if err != nil {
				return nil, err
			}
		} else {
			// Read in data.
			byts, err = ioutil.ReadAll(bytes.NewReader(content))
			if err != nil {
				return nil, err
			}
		}

		// Create a new zip archive.
		buf := new(bytes.Buffer)
		zipWriter := zip.NewWriter(buf)
		zipFile, err := zipWriter.Create(filename)
		lib.Printf("%+v", filename)
		if err != nil {
			return nil, err
		}
		_, err = zipFile.Write(byts)
		if err != nil {
			return nil, err
		}
		err = zipWriter.Close()
		if err != nil {
			return nil, err
		}

		nBytes := int64(len(buf.Bytes()))
		bytesReader := bytes.NewReader(buf.Bytes())
		var zipReader *zip.Reader
		zipReader, err = zip.NewReader(bytesReader, nBytes)
		if err != nil {
			return nil, err
		}

		for _, file := range zipReader.File {
			var rc io.ReadCloser
			rc, err = file.Open()
			if err != nil {
				return nil, err
			}
			var data []byte
			data, err = ioutil.ReadAll(rc)
			err = rc.Close()
			if err != nil {
				return nil, err
			}
			fmt.Printf("%s uncompressed %d bytes\n", file.Name, len(data))
			ary := bytes.Split(data, MessageSeparator)
			fmt.Printf("%s # of messages: %d\n", file.Name, len(ary))
			messages = append(messages, ary...)
		}
		fmt.Printf("number of messages to parse: %d\n", len(messages))

		var (
			statMtx *sync.Mutex
		)
		thrN := 3
		empty := 0
		warns := 0
		invalid := 0
		filtered := 0
		if thrN > 1 {
			statMtx = &sync.Mutex{}
		}
		stat := func(emp, warn, valid, oor bool) {
			if thrN > 1 {
				statMtx.Lock()
			}
			if emp {
				empty++
			}
			if warn {
				warns++
			}
			if !valid {
				invalid++
			}
			if oor {
				filtered++
			}
			if thrN > 1 {
				statMtx.Unlock()
			}
		}
		processMsg := func(c chan error, msg []byte, link string) (wch chan error, e error) {
			defer func() {
				if c != nil {
					c <- e
				}
			}()
			nBytes := len(msg)
			if nBytes < len(MessageSeparator) {
				stat(true, false, false, false)
				return
			}
			if !bytes.HasPrefix(msg, MessageSeparator[1:]) {
				msg = append(MessageSeparator[1:], msg...)
			}
			var (
				valid   bool
				warn    bool
				message map[string]interface{}
			)
			message, valid, warn = ParseMBoxMsg(2, groupName, msg)
			stat(false, warn, valid, false)
			if !valid {
				return
			}
			from := f.DateFrom
			updatedOn := f.ItemUpdatedOn(message)
			if &f.DateFrom != nil && updatedOn.Before(from) {
				stat(false, false, false, true)
				return
			}
			rawMessage := f.AddMetadata(message, endpoint, slug, groupName)
			allMsgs = append(allMsgs, rawMessage)
			return
		}

		for _, message := range messages {
			_, err = processMsg(nil, message, endpoint)
			if err != nil {
				return nil, err
			}
		}

		if empty > 0 {
			lib.Printf("%d empty messages\n", empty)
		}
		if warns > 0 {
			lib.Printf("%d parse message warnings\n", warns)
		}
		if invalid > 0 {
			lib.Printf("%d invalid messages\n", invalid)
		}
		if filtered > 0 {
			lib.Printf("%d filtered messages (updated before %+v)\n", invalid, f.DateFrom)
		}

	}

	return allMsgs, nil
}

// AddMetadata - add metadata to the raw message
func (f *Fetcher) AddMetadata(msg interface{}, endpoint, slug, groupName string) *RawMessage {
	timestamp := time.Now().UTC()
	rawMessage := new(RawMessage)

	rawMessage.BackendName = f.DSName
	rawMessage.BackendVersion = PiperBackendVersion
	rawMessage.Timestamp = timeLib.ConvertTimeToFloat(timestamp)
	rawMessage.Origin = endpoint
	rawMessage.Tag = endpoint
	rawMessage.UpdatedOn = timeLib.ConvertTimeToFloat(timestamp)
	rawMessage.Category = f.ItemCategory(msg)
	rawMessage.SearchFields = &MessageSearchFields{
		ItemID: f.ItemID(msg),
	}
	rawMessage.GroupName = groupName
	rawMessage.MetadataUpdatedOn = timestamp
	rawMessage.MetadataTimestamp = timestamp
	rawMessage.ProjectSlug = slug
	rawMessage.ChangedAt = timestamp

	// handle message data
	var mData RawMessageData
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		fmt.Println(err)
	}
	err = json.Unmarshal(messageBytes, &mData)
	if err != nil {
		fmt.Println(err)
	}
	rawMessage.Data = &mData

	// generate UUID
	uuID, err := uuid.Generate(Pipermail, rawMessage.Data.MessageID, groupName)
	if err != nil {
		fmt.Println(err)
	}
	rawMessage.UUID = uuID
	return rawMessage
}

// HandleMapping updates piper mail raw mapping
func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, PipermailRawMapping)
	return err
}

// GetLastDate gets fetching lastDate
func (f *Fetcher) GetLastDate(ESIndex string, now time.Time) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", ESIndex), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return now.UTC(), err
	}

	return lastDate, nil
}

// ItemID - return unique identifier for an item
func (f *Fetcher) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})[MessageIDField].(string)
	if !ok {
		lib.Fatalf("%s: ItemID() - cannot extract %s from %+v", f.DSName, MessageIDField, lib.DumpKeys(item))
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (f *Fetcher) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := lib.Dig(item, []string{MessageDateField}, true, false)
	updated, ok := iUpdated.(time.Time)
	if !ok {
		lib.Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %+v", f.DSName, MessageDateField, lib.DumpKeys(item))
	}
	return updated
}

// ItemCategory - return unique identifier for an item
func (f *Fetcher) ItemCategory(item interface{}) string {
	return Message
}

// ElasticRawMapping - Raw index mapping definition
func (f *Fetcher) ElasticRawMapping() []byte {
	return PiperRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (f *Fetcher) ElasticRichMapping() []byte {
	return PiperRichMapping
}

// Query query saved raw data from ES
func (f *Fetcher) Query(index string, query map[string]interface{}) (*RawHits, error) {

	var hits RawHits

	err := f.ElasticSearchProvider.Get(index, query, &hits)
	if err != nil {
		return nil, err
	}

	return &hits, err
}
