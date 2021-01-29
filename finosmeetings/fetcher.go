package finosmeetings

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

// Fetcher contains dockerhub datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

type Params struct {
	BackendVersion string
	BackendName    string
}

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
	RequestCSV(url string) ([][]string, error)
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

// NewFetcher initiates a new dockerhub fetcher
func NewFetcher(params *Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Finosmeetings,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
	}
}

// FetchItem pulls image data
func (f *Fetcher) FetchItem(URI string, now time.Time) (raw []*FinosmeetingsRaw, err error) {
	uriType := ""
	csvData := [][]string{}
	if strings.HasPrefix(URI, "file://") {
		uriType = FileURI
	} else {
		uriType = HttpURI
	}

	processCSV := func(body io.Reader) (data [][]string, err error) {
		reader := csv.NewReader(body)
		data, err = reader.ReadAll()
		if err != nil {
			fmt.Errorf("could not parse csv file from URI: %s", URI)
			return
		}
		return
	}

	if uriType == HttpURI {
		csvData, err = f.HTTPClientProvider.RequestCSV(URI)
		if err != nil {
			return nil, err
		}

	} else {
		// open the file specified
		fileName := strings.Split(URI, "file://")[1]
		fmt.Println("filename is:", fileName)
		f, err := os.Open(fileName)

		if err != nil {
			return nil, fmt.Errorf("could not open file from URI: %s", URI)
		}
		defer f.Close()

		csvData, err = processCSV(f)
		if err != nil {
			return nil, err
		}
	}

	headerMap := map[string]int{
		"email":      0,
		"name":       1,
		"org":        2,
		"githubid":   3,
		"cm_program": 4,
		"cm_title":   5,
		"cm_type":    6,
		"date":       7,
	}

	//fmt.Println(csvData)

	raw = []*FinosmeetingsRaw{}

	for index, thisRow := range csvData {
		//fmt.Println(i)

		if index == 0 {
			continue
		}

		layout := "2006-01-02"
		dateStr := strings.TrimSpace(thisRow[headerMap["date"]])
		t, err := time.Parse(layout, dateStr)

		if err != nil {
			fmt.Errorf("could not parse date field")
		}
		dateIsoFormat := t

		finosData := &FinosMeetingCSV{
			CMProgram:     strings.TrimSpace(thisRow[headerMap["cm_program"]]),
			CMTitle:       strings.TrimSpace(thisRow[headerMap["cm_title"]]),
			CMType:        strings.TrimSpace(thisRow[headerMap["cm_type"]]),
			Date:          strings.TrimSpace(thisRow[headerMap["date"]]),
			DateIsoFormat: dateIsoFormat,
			Email:         strings.TrimSpace(thisRow[headerMap["email"]]),
			GithubID:      strings.TrimSpace(thisRow[headerMap["githubid"]]),
			Name:          strings.TrimSpace(thisRow[headerMap["name"]]),
			Org:           strings.TrimSpace(thisRow[headerMap["org"]]),
			Timestamp:     4.4555,
		}

		fmt.Println("CSV parsed and uploaded in ES")

		thisRaw := &FinosmeetingsRaw{}
		thisRaw.Data = finosData
		thisRaw.BackendName = strings.Title(f.DSName)
		thisRaw.BackendVersion = f.BackendVersion
		thisRaw.Category = Category
		thisRaw.ClassifiedFieldsFiltered = nil
		now = now.UTC()
		thisRaw.Timestamp = utils.ConvertTimeToFloat(now)
		thisRaw.Data.FetchedOn = thisRaw.Timestamp
		thisRaw.MetadataTimestamp = now
		thisRaw.Origin = URI
		// raw.SearchFields = &RepositorySearchFields{repository, fmt.Sprintf("%f", raw.Timestamp), owner}
		thisRaw.Tag = URI
		thisRaw.UpdatedOn = thisRaw.Timestamp
		thisRaw.MetadataUpdatedOn = now

		// generate UUID
		uid, err := uuid.Generate(thisRaw.Origin, strconv.Itoa(index), strconv.FormatFloat(thisRaw.Data.FetchedOn, 'f', -1, 64))
		if err != nil {
			return nil, err
		}
		thisRaw.UUID = uid

		raw = append(raw, thisRaw)
	}

	return raw, nil
}

// HandleMapping updates dockerhub raw mapping
func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, FinosmeetingsRawMapping)
	return err
}

// GetLastDate gets fetching lastDate
func (f *Fetcher) GetLastDate(ESIndex, now time.Time) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", ESIndex), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return now.UTC(), err
	}

	return lastDate, nil
}
