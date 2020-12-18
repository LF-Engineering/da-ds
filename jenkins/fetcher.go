package jenkins

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
	"net/http"
	"time"
)

// Fetcher contains Jenkins datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

// Params required parameters for Jenkins fetcher
type Params struct {
	JenkinsURL     string
	Username       string
	Password       string
	Depth          int
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

// NewFetcher initiates a new jenkins fetcher
func NewFetcher(params *Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Jenkins,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
	}
}

func (f *Fetcher) FetchJobs(params *Params) (*JobResponse,error) {
	var header = make(map[string]string)
	if params.Username != "" && params.Password != "" {
		auth := params.Username + ":" + params.Password
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		header["Authorization"] = auth
	}
	url := fmt.Sprintf("%s/api/json", params.JenkinsURL)
	statusCode, body, err := f.HTTPClientProvider.Request(url,"GET", header,nil, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}
	var jobResponse JobResponse
	if err := jsoniter.Unmarshal(body, &jobResponse); err != nil {
		return nil, errors.New("unable to unmarshal the job response")
	}
	return &jobResponse, nil
}


func (f *Fetcher) FetchBuilds(params *Params, jobURL string) (*BuildResponse, error) {
	var header = make(map[string]string)
	if params.Username != "" && params.Password != "" {
		auth := params.Username + ":" + params.Password
		auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
		header["Authorization"] = auth
	}
	if params.Depth == 0 {
		params.Depth = Depth
	}
	url := fmt.Sprintf("%s/api/json?depth=%d", jobURL, params.Depth)
	statusCode, body, err := f.HTTPClientProvider.Request(url,"GET", header,nil, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}
	var buildResponse BuildResponse
	if err := jsoniter.Unmarshal(body, &buildResponse); err != nil {
		return nil, errors.New("unable to unmarshal the job response")
	}
	return &buildResponse, nil
}

// FetchItem pulls image data
func (f *Fetcher) FetchItem(params *Params) ([]JenkinsRaw, error) {
	var raw = make([]JenkinsRaw, 0)
	// Fetch all jobs
	jobResponse, err := f.FetchJobs(params)
	if err != nil {
		return raw, err
	}
	params.JenkinsURL = jobResponse.URL
	for _,job := range jobResponse.Jobs {
		// For every job fetch all the builds
		builds, err := f.FetchBuilds(params, job.URL)
		if err != nil {
			continue
		}
		// append the fetched builds to the JenkinsRaw slice
		data := f.MapToJenkinsRaw(builds, params)
		raw = append(raw, data...)
	}
	return raw, nil
}

func (f *Fetcher) MapToJenkinsRaw(response *BuildResponse, params *Params) []JenkinsRaw {
	var data = make([]JenkinsRaw, 0)
	for _,build := range response.Builds {
		var raw JenkinsRaw
		raw.Data = build
		raw.MetadataUpdatedOn = time.Now()
		raw.MetadataTimestamp = time.Now()
		raw.Tag = params.JenkinsURL
		raw.SearchFields.ItemID = build.URL
		raw.SearchFields.Number = build.Number
		raw.BackendVersion = params.BackendVersion
		raw.Category = BuildCategory
		raw.Origin = params.JenkinsURL
		raw.UpdatedOn = float64(build.Timestamp)/1000
		raw.BackendName = f.DSName
		uuid, err := uuid.Generate(params.JenkinsURL,build.URL)
		if err != nil {
			continue
		}
		raw.UUID = uuid
		data = append(data, raw)
	}
	return data
}

// HandleMapping updates Jenkins raw mapping
func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, JenkinsRawMapping)
	return err
}

// GetLastDate gets fetching lastDate
func (f *Fetcher) GetLastDate(buildServer BuildServer, now time.Time) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", buildServer.Index), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return now.UTC(), err
	}

	return lastDate, nil
}
