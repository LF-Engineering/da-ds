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
}

// NewFetcher initiates a new dockerhub fetcher
func NewFetcher(params *Params, httpClientProvider HttpClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Dockerhub,
		HttpClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		Username:              params.Username,
		Password:              params.Password,
		BackendVersion:        params.BackendVersion,
	}
}

func (f *Fetcher) login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s", APIURL, APILogin)

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

// FetchItem fetch dockerhub repository
func (f *Fetcher) FetchItem(owner string, repository string) error {
	// login
	token := ""

	if f.Password != "" {
		t, err := f.login(f.Username, f.Password)
		if err != nil {
			return err
		}
		token = t
	}

	url := fmt.Sprintf("%s/%s/%s/%s", APIURL, APIRepositories, owner, repository)
	headers := map[string]string{}
	if token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", token)
	}

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "GET", headers, nil)
	if err != nil || statusCode != http.StatusOK {
		return errors.New("invalid request")
	}

	index := fmt.Sprintf("sds-%s-%s-dockerhub-raw", owner, repository)

	// todo: should be moved to upstart
	_, err = f.ElasticSearchProvider.CreateIndex(index, DockerhubRawMapping)
	if err != nil {
		return err
	}

	repoRes := RepositoryResponse{}
	if err := json.Unmarshal(resBody, &repoRes); err != nil {
		return errors.New("unable to resolve json request")
	}

	raw := RepositoryRaw{}
	raw.Data = repoRes
	raw.BackendName = strings.Title(f.DSName)
	raw.BackendVersion = f.BackendVersion
	raw.Category = Category
	raw.ClassifiedFieldsFiltered = nil
	timestamp := time.Now()
	raw.Timestamp = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	raw.Data.FetchedOn = raw.Timestamp
	raw.MetadataTimestamp = dads.ToESDate(timestamp)
	raw.Origin = url
	raw.SearchFields = RepositorySearchFields{repository, fmt.Sprintf("%v", raw.Timestamp), owner}
	raw.Tag = url
	raw.UpdatedOn = raw.Data.LastUpdated

	// generate UUID
	uid, err := uuid.Generate(raw.Data.FetchedOn)
	if err != nil {
		return err
	}
	raw.UUID = uid

	body, err := json.Marshal(raw)
	if err != nil || statusCode != http.StatusOK {
		return errors.New("unable to convert body to json")
	}

	esRes, err := f.ElasticSearchProvider.Add(index, raw.UUID, body)
	if err != nil {
		return err
	}

	fmt.Println("Index created: %S", string(esRes))

	return nil
}