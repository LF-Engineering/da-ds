package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/utils/uuid"
	"net/http"
	"regexp"
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
}

// Params ...
type Params struct {
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
	DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
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

func (f *Fetcher) Login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s", APIURL, APILogin)

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
	url := fmt.Sprintf("%s/%s/%s/%s", APIURL, APIRepositories, owner, repository)
	headers := map[string]string{}
	if f.Token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", f.Token)
	}

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "GET", headers, nil)
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
	timestamp := time.Now()
	raw.Timestamp = fmt.Sprintf("%v", timestamp.UnixNano()/1.0e3)
	raw.Data.FetchedOn = raw.Timestamp
	raw.MetadataTimestamp = dads.ToESDate(timestamp)
	raw.Origin = url
	raw.SearchFields = &RepositorySearchFields{repository, fmt.Sprintf("%v", raw.Timestamp), owner}
	raw.Tag = url
	raw.UpdatedOn = raw.Data.LastUpdated

	// generate UUID
	uid, err := uuid.Generate(raw.Data.FetchedOn)
	if err != nil {
		return nil, err
	}

	raw.UUID = uid

	return raw, nil
}

func (f *Fetcher) Insert(data *RepositoryRaw) ([]byte, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	resData, err := f.ElasticSearchProvider.Add(fmt.Sprintf("sds-%s-%s-dockerhub-raw", data.Data.Namespace, data.Data.Name), data.UUID, body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (f *Fetcher) BulkInsert(data []*RepositoryRaw) ([]byte, error) {
	raw := make([]interface{}, 0)

	for _, item := range data {

		index := map[string]interface{}{
			"index": map[string]string{
				"_index": fmt.Sprintf("sds-%s-%s-dockerhub-raw", item.Data.Namespace, item.Data.Name),
				"_id":    item.UUID,
			},
		}
		raw = append(raw, index)
		raw = append(raw, "\n")
		raw = append(raw, item)
		raw = append(raw, "\n")
	}

	body, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	var re = regexp.MustCompile(`(}),"\\n",?`)
	body = []byte(re.ReplaceAllString(strings.TrimSuffix(strings.TrimPrefix(string(body), "["), "]"), "$1\n"))

	resData, err := f.ElasticSearchProvider.Bulk(body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, DockerhubRawMapping)
	return err
}
