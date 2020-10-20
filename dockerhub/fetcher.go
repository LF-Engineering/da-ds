package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/utils/uuid"
	"net/http"
	"strings"
	"time"
)

const (
	// DockerhubAPIURL - dockerhub API URL
	DockerhubAPIURL          = "https://hub.docker.com/v2"
	DockerhubAPILogin        = "users/login"
	DockerhubAPIRepositories = "repositories"
	DockerhubCategory        = "dockerhub-data"

	// Dockerhub - common constant string
	Dockerhub string = "dockerhub"
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
	BackendVersion        string
}

// DockerHubParams ...
type DockerhubParams struct {
	Username       string
	Password       string
	BackendVersion string
}

// HttpClientProvider ...
type HttpClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte) (statusCode int, resBody []byte, err error)
}

// ESClientProvider ...
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
}

// NewFetcher initiates a new dockerhub fetcher
func NewFetcher(params *DockerhubParams, httpClientProvider HttpClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Dockerhub,
		HttpClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		Username:              params.Username,
		Password:              params.Password,
		BackendVersion:        params.BackendVersion,
	}
}

// Validate dockerhub datasource configuration
func (f *Fetcher) Validate() error {

	return nil
}

// todo: to be reviewed
// Name - return data source name
func (f *Fetcher) Name() string {
	return f.DSName
}

// Info - return DS configuration in a human readable form
func (f *Fetcher) Info() string {
	return fmt.Sprintf("%+v", f)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (f *Fetcher) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (f *Fetcher) FetchRaw() (err error) {
	dads.Printf("%s should use generic FetchRaw()\n", f.DSName)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (f *Fetcher) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (f *Fetcher) Enrich() (err error) {
	dads.Printf("%s should use generic Enrich()\n", f.DSName)
	return
}

func (f *Fetcher) login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s", DockerhubAPIURL, DockerhubAPILogin)

	payload := make(map[string]interface{})
	payload["username"] = username
	payload["password"] = password

	p, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	dads.Printf("dockerhub login via: %s\n", url)

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "Post", nil, p)

	if statusCode == http.StatusOK {
		res := LoginResponse{}
		err = json.Unmarshal(resBody, &res)
		if err != nil {
			fmt.Printf("Cannot unmarshal result from %s\n", string(resBody))
			return "", err
		}
	}

	return "", errors.New("invalid login credentials")
}

// FetchItems ...
func (f *Fetcher) FetchItems(owner string, repository string) error {
	// login
	token := ""

	if f.Password != "" {
		t, err := f.login(f.Username, f.Password)
		if err != nil {
			return err
		}
		token = t
	}

	url := fmt.Sprintf("%s/%s/%s/%s", DockerhubAPIURL, DockerhubAPIRepositories, owner, repository)
	headers := map[string]string{}
	if token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", token)
	}

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "GET", headers, nil)
	if err != nil || statusCode != http.StatusOK {
		return errors.New("invalid request")
	}

	// todo: is there any required process before saving into ES?
	index := fmt.Sprintf("sds-%s-%s-dockerhub-raw", owner, repository)

	_, err = f.ElasticSearchProvider.CreateIndex(index, DockerhubRawMapping)
	if err != nil {
		return err
	}

	repoRes := RepositoryResponse{}
	if err := json.Unmarshal(resBody, &repoRes); err != nil {
		return errors.New("unable to resolve json request")
	}

	b := RepositoryRaw{}
	b.Data = repoRes
	b.BackendName = strings.Title(Dockerhub)
	b.BackendVersion = f.BackendVersion
	b.Category = DockerhubCategory
	b.ClassifiedFieldsFiltered = nil
	timestamp := time.Now()
	b.Timestamp = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	b.Data.FetchedOn = b.Timestamp
	b.MetadataTimestamp = dads.ToESDate(timestamp)
	b.Origin = url
	b.SearchFields = RepositorySearchFields{repository, fmt.Sprintf("%v", b.Timestamp), owner}
	b.Tag = url
	b.UpdatedOn = b.Data.LastUpdated

	// generate UUID
	generatedUUID, err := uuid.Generate(b.Data.FetchedOn)
	if err != nil {
		return err
	}

	b.UUID = generatedUUID

	body, err := json.Marshal(b)
	if err != nil || statusCode != http.StatusOK {
		return errors.New("unable to convert body to json")
	}

	// todo: save result to elastic search (raw document)
	esRes, err := f.ElasticSearchProvider.Add(index, b.UUID, body)
	if err != nil {
		return err
	}

	fmt.Println(string(esRes))
	return nil
}
