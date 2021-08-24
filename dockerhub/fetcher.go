package dockerhub

import (
	"errors"
	"fmt"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"

	"net/http"
	"strings"
	"time"

	dads "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
)

// Fetcher contains dockerhub datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	Username              string
	Password              string
	Token                 string
	BackendVersion        string
}

// Params required parameters for dockerhub fetcher
type Params struct {
	Username       string
	Password       string
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
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []elastic.BulkData) ([]byte, error)
	DelayOfCreateIndex(ex func(str string, b []byte) ([]byte, error), uin uint, du time.Duration, index string, data []byte) error
	BulkUpdate(data []elastic.BulkData) ([]byte, error)
}

// NewFetcher initiates a new dockerhub fetcher
func NewFetcher(params *Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Dockerhub,
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		Username:              params.Username,
		Password:              params.Password,
		BackendVersion:        params.BackendVersion,
	}
}

// Login dockerhub in order to obtain access token for fetching private repositories
func (f *Fetcher) Login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s/%s/%s", APIURL, APIVersion, APIRepositories, APILogin)

	payload := make(map[string]interface{})
	payload["username"] = username
	payload["password"] = password

	p, err := jsoniter.Marshal(payload)
	if err != nil {
		return "", err
	}

	dads.Printf("dockerhub login via: %s\n", url)

	statusCode, resBody, err := f.HTTPClientProvider.Request(url, "Post", nil, p, nil)

	if statusCode == http.StatusOK {
		res := LoginResponse{}
		err = jsoniter.Unmarshal(resBody, &res)
		if err != nil {
			return "", fmt.Errorf("cannot unmarshal result from %s", string(resBody))
		}

		// Set token into the object fetcher object
		f.Token = res.Token

		return res.Token, nil
	}

	return "", errors.New("invalid login credentials")
}

// FetchItem pulls image data
func (f *Fetcher) FetchItem(owner string, repository string, now time.Time) (*RepositoryRaw, error) {
	org := owner
	if org == "_" {
		org = "library"
	}
	requestURL := fmt.Sprintf("%s/%s/%s/%s/%s", APIURL, APIVersion, APIRepositories, org, repository)
	url := fmt.Sprintf("%s/%s/%s", APIURL, owner, repository)
	headers := map[string]string{}
	if f.Token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", f.Token)
	}

	statusCode, resBody, err := f.HTTPClientProvider.Request(requestURL, "GET", headers, nil, nil)
	if err != nil || statusCode != http.StatusOK {
		dads.Printf("[dads-dockerhub] FetchItem get repository error : %+v, status code : %v, response body : %s\n", err, statusCode, resBody)
		return nil, err
	}

	repoRes := &RepositoryResponse{}
	if err := jsoniter.Unmarshal(resBody, &repoRes); err != nil {
		dads.Printf("[dads-dockerhub] FetchItem unmarshal error : %+v\n", err)
		return nil, errors.New("unable to resolve json request")
	}

	raw := &RepositoryRaw{}
	raw.Data = repoRes
	raw.BackendName = strings.Title(f.DSName)
	raw.BackendVersion = f.BackendVersion
	raw.Category = Category
	raw.ClassifiedFieldsFiltered = nil
	now = now.UTC()
	raw.Timestamp = timeLib.ConvertTimeToFloat(now)
	raw.Data.FetchedOn = raw.Timestamp
	raw.MetadataTimestamp = now
	raw.Origin = url
	raw.SearchFields = &RepositorySearchFields{repository, fmt.Sprintf("%f", raw.Timestamp), owner}
	raw.Tag = url
	raw.UpdatedOn = raw.Timestamp
	raw.MetadataUpdatedOn = now

	// generate UUID
	dStr := now.Format("02-01-2006")
	uid, err := uuid.Generate(raw.Origin, dStr)
	if err != nil {
		dads.Printf("[dads-dockerhub] FetchItem Generate uuid error : %+v\n", err)
		return nil, err
	}
	raw.UUID = uid

	return raw, nil
}

// GetLastDate gets fetching lastDate
func (f *Fetcher) GetLastDate(repo *Repository, now time.Time) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", repo.ESIndex), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return now.UTC(), err
	}

	return lastDate, nil
}
