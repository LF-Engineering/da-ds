package pipermail

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	lib "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/utils"
)

// Fetcher contains pipermail datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	Project               string
	Token                 string
	BackendVersion        string
}

// Params required parameters for piper mail fetcher
type Params struct {
	FromDate       time.Time
	BackendVersion string
}

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// ESClientProvider used in connecting to ES Client server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []*utils.BulkData) ([]byte, error)
}

// NewFetcher initiates a new pipermail fetcher
func NewFetcher(params *Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Pipermail,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
	}
}

/*
   Fetch the mbox files from the remote archiver.

   Stores the archives in the path given during the initialization
   of this object. Those archives which don't have not valid extensions will
   be ignored.

   Pipermail archives usually have on their file names the date of
   the archives stored following the schema year-month. When fromDate
   property is called, it will return the mboxes for which their year
   and month are equal or after that date.

   fromDate: fetch archives that store messages equal or after the given date; only year and month values
   are compared

   returns a map of links and their paths of the fetched archives

*/
func (f *Fetcher) Fetch(url string, fromDate *time.Time) (map[string]string, error) {
	if fromDate == nil {
		fromDate = &DefaultDateTime
	}

	dirpath := filepath.Join(ArchiveDownloadsPath, url)
	lib.Printf("\nDownloading mboxes from %s since %s\n", url, fromDate.String())

	statusCode, _, err := f.HTTPClientProvider.Request(url, "GET", nil, nil, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}

	links, err := f.ParseArchiveLinks(url)
	if err != nil {
		return nil, err
	}

	fetched := make(map[string]string)

	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		os.MkdirAll(dirpath, os.ModePerm)
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

// FetchItem pulls image data
func (f *Fetcher) FetchItem(owner string, link string, now time.Time) (interface{}, error) {
	//url := fmt.Sprintf("%s/%s/%s", ArchiveDownloadsPath, owner, link)

	archives, err := f.Fetch(link, &DefaultDateTime)
	if err != nil {
		return nil, err
	}
	fmt.Printf("\n\n %+v \n\n", archives)
	var messages [][]byte

	statusCode, _, err := f.HTTPClientProvider.Request(link, "GET", nil, nil, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}
	fmt.Printf("\n\n statusCode: %+v \n\n", statusCode)

	for _, filePath := range archives {
		f, err := os.Open(filePath)
		if err != nil {
			lib.Printf("os.Open: %+v", err)
			return nil, err
		}

		filename := filepath.Base(filePath)
		baseExtension := filepath.Ext(filePath)
		filename = strings.TrimSuffix(filename, filepath.Ext(filename))
		fmt.Printf("\n\n filename: %+v\n\n", filename)

		var decompressedFileContentReader *gzip.Reader
		var content []byte
		var byts []byte

		// Create new reader to decompress gzip.
		if baseExtension == ".gz" {
			decompressedFileContentReader, err = gzip.NewReader(f)
			if err != nil {
				lib.Printf("\nfilePath: %+v\n", filePath)
				lib.Printf("\ngzip.NewReader: %+v\n", err)
				return nil, err
			}
		} else {
			content, err = ioutil.ReadFile("thermopylae.txt")
			if err != nil {
				lib.Printf("content ioutil.ReadFile: %+v", err)
				continue
			}

		}

		if decompressedFileContentReader != nil {
			// Read in data.
			byts, err = ioutil.ReadAll(decompressedFileContentReader)
			if err != nil {
				lib.Printf("decompressedFileContentReader ioutil.ReadAll: %+v", err)
				return nil, err
			}
		} else {
			// Read in data.
			byts, err = ioutil.ReadAll(bytes.NewReader(content))
			if err != nil {
				lib.Printf("content ioutil.ReadAll: %+v", err)
				return nil, err
			}
		}

		// Create a new zip archive.
		buf := new(bytes.Buffer)
		zipWriter := zip.NewWriter(buf)
		zipFile, err := zipWriter.Create(filename)
		if err != nil {
			lib.Printf("zipWriter.Create: %+v", err)
			return nil, err
		}
		_, err = zipFile.Write(byts)
		if err != nil {
			lib.Printf("zipFile.Write: %+v", err)
			return nil, err
		}
		err = zipWriter.Close()
		if err != nil {
			lib.Printf("zipWriter.Close: %+v", err)
			return nil, err
		}
		ioutil.WriteFile(filename+".zip", buf.Bytes(), 0777)

		nBytes := int64(len(buf.Bytes()))
		bytesReader := bytes.NewReader(buf.Bytes())
		var zipReader *zip.Reader
		zipReader, err = zip.NewReader(bytesReader, nBytes)
		if err != nil {
			lib.Printf("zip.NewReader: %+v", err)
			return nil, err
		}

		for _, file := range zipReader.File {
			var rc io.ReadCloser
			rc, err = file.Open()
			if err != nil {
				lib.Printf("file.Open: %+v", err)
				return nil, err
			}
			var data []byte
			data, err = ioutil.ReadAll(rc)
			err = rc.Close()
			if err != nil {
				lib.Printf("rc.Close: %+v", err)
				return nil, err
			}
			fmt.Printf("%s uncompressed %d bytes\n", file.Name, len(data))
			ary := bytes.Split(data, []byte(Separator))
			fmt.Printf("%s # of messages: %d\n", file.Name, len(ary))
			messages = append(messages, ary...)
		}
	}

	return nil, nil
}

// HandleMapping updates pipermail raw mapping
func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, PipermailRawMapping)
	return err
}

// GetLastDate gets fetching lastDate
func (f *Fetcher) GetLastDate(link *Link, now time.Time) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", link.ESIndex), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return now.UTC(), err
	}

	return lastDate, nil
}

