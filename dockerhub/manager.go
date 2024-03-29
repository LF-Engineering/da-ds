package dockerhub

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/LF-Engineering/da-ds/build"

	"github.com/LF-Engineering/dev-analytics-libraries/slack"

	dads "github.com/LF-Engineering/da-ds"

	"github.com/LF-Engineering/dev-analytics-libraries/auth0"

	"github.com/LF-Engineering/da-ds/util"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
)

// Manager describes dockerhub manager
type Manager struct {
	Username               string
	Password               string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	EnrichOnly             bool
	Enrich                 bool
	ESUrl                  string
	ESUsername             string
	ESPassword             string
	HTTPTimeout            time.Duration
	Repositories           []*Repository
	FromDate               *time.Time
	NoIncremental          bool

	AffAPI           string
	ProjectSlug      string
	AffBaseURL       string
	ESCacheURL       string
	ESCacheUsername  string
	ESCachePassword  string
	AuthGrantType    string
	AuthClientID     string
	AuthClientSecret string
	AuthAudience     string
	Auth0URL         string
	Environment      string
	Slug             string

	Retries    uint
	Delay      time.Duration
	GapURL     string
	WebHookURL string
}

// Param required for creating a new instance of Bugzilla manager
type Param struct {
	Username               string
	Password               string
	EndPoint               string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	Fetch                  bool
	Enrich                 bool
	ESUrl                  string
	EsUser                 string
	EsPassword             string
	EsIndex                string
	FromDate               *time.Time
	Project                string
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	AffAPI                 string
	ProjectSlug            string
	AffBaseURL             string
	ESCacheURL             string
	ESCacheUsername        string
	ESCachePassword        string
	AuthGrantType          string
	AuthClientID           string
	AuthClientSecret       string
	AuthAudience           string
	Auth0URL               string
	Environment            string
	Slug                   string
	EnrichOnly             bool
	HTTPTimeout            time.Duration
	Repositories           []*Repository
	NoIncremental          bool
	SlackWebHookURL        string
}

// Repository represents dockerhub repository data
type Repository struct {
	Owner      string
	Repository string
	Project    string
	ESIndex    string
}

// Auth0Client ...
type Auth0Client interface {
	GetToken() (string, error)
}

// NewManager initiates dockerhub manager instance
func NewManager(param Param) *Manager {
	mng := &Manager{
		Username:               param.Username,
		Password:               param.Password,
		FetcherBackendVersion:  param.FetcherBackendVersion,
		EnricherBackendVersion: param.EnricherBackendVersion,
		EnrichOnly:             param.EnrichOnly,
		Enrich:                 param.Enrich,
		ESUrl:                  param.ESUrl,
		HTTPTimeout:            param.HTTPTimeout,
		Repositories:           param.Repositories,
		FromDate:               param.FromDate,
		NoIncremental:          param.NoIncremental,
		Retries:                param.Retries,
		Delay:                  param.Delay,
		GapURL:                 param.GapURL,
		AffAPI:                 param.AffAPI,
		ProjectSlug:            param.ProjectSlug,
		AffBaseURL:             param.AffBaseURL,
		ESCacheURL:             param.ESCacheURL,
		ESCacheUsername:        param.ESCacheUsername,
		ESCachePassword:        param.ESCachePassword,
		AuthGrantType:          param.AuthGrantType,
		AuthClientID:           param.AuthClientID,
		AuthClientSecret:       param.AuthClientSecret,
		AuthAudience:           param.AuthAudience,
		Auth0URL:               param.Auth0URL,
		Environment:            param.Environment,
		Slug:                   param.Slug,
	}

	return mng
}

// Sync runs dockerhub fetch and enrich according to passed parameters
func (m *Manager) Sync() error {

	if len(m.Repositories) == 0 {
		return errors.New("no repositories found")
	}

	fetcher, enricher, esClientProvider, auth0Client, err := buildServices(m)
	if err != nil {
		dads.Printf("[dads-dockerhub] Sync buildServices error : %+v\n", err)
		return err
	}

	// Get dockerhub token if needed to get data from private repos
	if m.Password != "" {
		_, err := fetcher.Login(m.Username, m.Password)
		if err != nil {
			dads.Printf("[dads-dockerhub] Login fetcher error : %+v\n", err)
			return err
		}
	}

	if !m.EnrichOnly {
		data := make([]elastic.BulkData, 0)

		// fetch data
		for _, repo := range m.Repositories {
			var raw *RepositoryRaw
			// Fetch data for single repo
			raw, err = fetcher.FetchItem(repo.Owner, repo.Repository, time.Now())
			if err != nil {
				return fmt.Errorf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository)
			}
			data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", repo.ESIndex), ID: raw.UUID, Data: raw})

			// set mapping and create index if not exists
			err = fetcher.ElasticSearchProvider.DelayOfCreateIndex(fetcher.ElasticSearchProvider.CreateIndex, m.Retries, m.Delay, fmt.Sprintf("%s-raw", repo.ESIndex), DockerhubRawMapping)
			if err != nil {
				err = util.HandleGapData(m.GapURL, fetcher.HTTPClientProvider, data, auth0Client, m.Environment)
				if err != nil {
					return err
				}
				continue
			}
		}

		if len(data) > 0 {
			// Insert raw data to elasticsearch
			esRes, err := esClientProvider.BulkInsert(data)
			if err != nil {
				err = util.HandleGapData(m.GapURL, fetcher.HTTPClientProvider, data, auth0Client, m.Environment)
				return err
			}

			failedData, err := util.HandleFailedData(data, esRes)
			if len(failedData) != 0 {
				err = util.HandleGapData(m.GapURL, fetcher.HTTPClientProvider, failedData, auth0Client, m.Environment)
			}
		}
	}

	if m.Enrich || m.EnrichOnly {
		data := make([]elastic.BulkData, 0)
		needUpdateData := make([]elastic.BulkData, 0)

		for _, repo := range m.Repositories {
			var fromDate *time.Time
			var lastDate time.Time
			if m.FromDate == nil || (*m.FromDate).IsZero() {
				lastDate, err = fetcher.GetLastDate(repo, time.Now())
				if err != nil {
					dads.Printf("[dads-dockerhub] GetLastDate fetcher error : %+v\n", err)
					log.Println("[GetLastDate] could not get last date")
				}
			} else {
				fromDate = m.FromDate
			}

			esData, err := enricher.GetFetchedDataItem(repo, fromDate, &lastDate, m.NoIncremental)
			if err != nil {
				return err
			}

			query := map[string]interface{}{
				"size": 1,
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{},
					},
				},
				"sort": []map[string]interface{}{
					{
						"metadata__enriched_on": map[string]string{
							"order": "desc",
						},
					},
				},
			}
			mustTerm := map[string]interface{}{
				"term": map[string]interface{}{
					"is_docker_image": map[string]interface{}{
						"value": 1,
					},
				},
			}

			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]map[string]interface{}), mustTerm)

			needUpdateHits := &TopHits{}
			id := ""
			if len(esData.Hits.Hits) > 0 {
				// Enrich data for single repo
				enriched, err := enricher.EnrichItem(*esData.Hits.Hits[0].Source, repo.Project, time.Now())
				if err != nil {
					return fmt.Errorf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository)
				}
				data = append(data, elastic.BulkData{IndexName: repo.ESIndex, ID: enriched.UUID, Data: enriched})
				_ = enricher.HandleMapping(repo.ESIndex)
				id = enriched.ID
			}
			mustTerm2 := map[string]interface{}{
				"term": map[string]interface{}{
					"id.keyword": map[string]interface{}{
						"value": id,
					},
				},
			}

			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = append(query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]map[string]interface{}), mustTerm2)
			err = fetcher.ElasticSearchProvider.Get(repo.ESIndex, query, needUpdateHits)
			if err != nil {
				dads.Printf("[dads-dockerhub] Sync no elastic enriched data exist warning : %+v\n", err)
			}

			if len(needUpdateHits.Hits.Hits) > 0 {
				upData := map[string]map[string]int{
					"doc": {
						"is_docker_image": 0,
						"is_event":        1,
					},
				}
				needUpdateData = append(needUpdateData, elastic.BulkData{IndexName: repo.ESIndex, ID: needUpdateHits.Hits.Hits[0].ID, Data: upData})
			}
		}
		if len(needUpdateData) > 0 {
			esRes, err := esClientProvider.BulkUpdate(needUpdateData)
			if err != nil {
				dads.Printf("[dads-dockerhub] Sync bulk update elastic data error : %+v\n", err)
				log.Printf("[BulkUpdate] elastic response :%+v\n", esRes)
			}
		}

		if len(data) > 0 {
			// Insert enriched data to elasticsearch
			esRes, err := esClientProvider.BulkInsert(data)
			if err != nil {
				err = util.HandleGapData(m.GapURL, fetcher.HTTPClientProvider, data, auth0Client, m.Environment)
				return err
			}

			failedData, err := util.HandleFailedData(data, esRes)
			if len(failedData) != 0 {
				err = util.HandleGapData(m.GapURL, fetcher.HTTPClientProvider, failedData, auth0Client, m.Environment)
			}

		}
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, Auth0Client, error) {
	httpClientProvider := http.NewClientProvider(m.HTTPTimeout)
	params := &Params{
		Username:       m.Username,
		Password:       m.Password,
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	esCacheClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESCacheURL,
		Username: m.ESCacheUsername,
		Password: m.ESCachePassword,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)
	slackProvider := slack.New(m.WebHookURL)

	appNameVersion := fmt.Sprintf("%s-%v", build.AppName, strconv.FormatInt(time.Now().Unix(), 10))
	auth0Client, err := auth0.NewAuth0Client(
		m.Environment,
		m.AuthGrantType,
		m.AuthClientID,
		m.AuthClientSecret,
		m.AuthAudience,
		m.Auth0URL,
		httpClientProvider,
		esCacheClientProvider,
		&slackProvider,
		appNameVersion)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return fetcher, enricher, esClientProvider, auth0Client, err
}
