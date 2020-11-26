package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Fetcher contains dockerhub datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HttpClientProvider    HttpClientProvider
	ElasticSearchProvider ESClientProvider
	Username              string
	Password              string
	Token                 string
	BackendVersion        string
	Now                  func() time.Time
}

// Params required parameters for dockerhub fetcher
type Params struct {
	Username       string
	Password       string
	BackendVersion string
}

// HttpClientProvider used in connecting to remote http server
type HttpClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte) (statusCode int, resBody []byte, err error)
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
func NewFetcher(params *Params, httpClientProvider HttpClientProvider, esClientProvider ESClientProvider, timer func() time.Time) *Fetcher {
	return &Fetcher{
		DSName:                Dockerhub,
		HttpClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		Username:              params.Username,
		Password:              params.Password,
		BackendVersion:        params.BackendVersion,
		Now:                  timer,
	}
}

func (f *Fetcher) Login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s/%s/%s", APIUrl, APIVersion, APIRepositories, APILogin)

	payload := make(map[string]interface{})
	payload["username"] = username
	payload["password"] = password

	p, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	_, err = dads.Printf("dockerhub login via: %s\n", url)

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "Post", nil, p)

	if statusCode == http.StatusOK {
		res := LoginResponse{}
		err = json.Unmarshal(resBody, &res)
		if err != nil {
			return "", errors.New(fmt.Sprintf("Cannot unmarshal result from %s\n", string(resBody)))
		}

		// Set token into the object fetcher object
		f.Token = res.Token

		return res.Token, nil
	}

	return "", errors.New("invalid login credentials")
}

// FetchItems ...
func (f *Fetcher) FetchItem(owner string, repository string) (*RepositoryRaw, error) {
	requestUrl := fmt.Sprintf("%s/%s/%s/%s/%s", APIUrl, APIVersion, APIRepositories, owner, repository)
	url := fmt.Sprintf("%s/%s/%s", APIUrl, owner, repository)
	headers := map[string]string{}
	if f.Token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", f.Token)
	}

	statusCode, resBody, err := f.HttpClientProvider.Request(requestUrl, "GET", headers, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, err
	}

	repoRes := &RepositoryResponse{}
	if err := json.Unmarshal(resBody, &repoRes); err != nil {
		return nil, errors.New("unable to resolve json request")
	}

	raw := &RepositoryRaw{}
	raw.Data = repoRes
	raw.BackendName = strings.Title(f.DSName)
	raw.BackendVersion = f.BackendVersion
	raw.Category = Category
	raw.ClassifiedFieldsFiltered = nil
	now := f.Now().UTC()
	raw.Timestamp = utils.ConvertTimeToFloat(now)
	raw.Data.FetchedOn = raw.Timestamp
	raw.MetadataTimestamp = now
	raw.Origin = url
	raw.SearchFields = &RepositorySearchFields{repository, fmt.Sprintf("%f", raw.Timestamp), owner}
	raw.Tag = url
	lastUpdated := raw.Data.LastUpdated
	raw.UpdatedOn = utils.ConvertTimeToFloat(lastUpdated)
	raw.MetadataUpdatedOn = lastUpdated

	// generate UUID
	uid, err := uuid.Generate(raw.Origin, strconv.FormatFloat(raw.Data.FetchedOn, 'f', -1, 64))
	if err != nil {
		return nil, err
	}
	raw.UUID = uid

	return raw, nil
}

func (f *Fetcher) Insert(index string, data *RepositoryRaw) ([]byte, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	resData, err := f.ElasticSearchProvider.Add(index, data.UUID, body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, DockerhubRawMapping)
	return err
}

func (f *Fetcher) GetLastDate(repo *Repository) (time.Time, error) {
	lastDate, err := f.ElasticSearchProvider.GetStat(fmt.Sprintf("%s-raw", repo.ESIndex), "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return f.Now().UTC(), err
	}

	return lastDate, nil
}
