package dockerhub

import (
	"errors"
	"fmt"
	"log"
	"time"

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
	AuthURL          string
	Environment      string
	Slug             string

	Retries uint
	Delay   time.Duration
	GapURL  string
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
	AuthURL                string
	Environment            string
	Slug                   string
	EnrichOnly             bool
	HTTPTimeout            time.Duration
	Repositories           []*Repository
	NoIncremental          bool
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
	ValidateToken(env string) (string, error)
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
		AuthURL:                param.AuthURL,
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

			if len(esData.Hits.Hits) > 0 {
				// Enrich data for single repo
				enriched, err := enricher.EnrichItem(*esData.Hits.Hits[0].Source, repo.Project, time.Now())
				if err != nil {
					return fmt.Errorf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository)
				}
				data = append(data, elastic.BulkData{IndexName: repo.ESIndex, ID: enriched.UUID, Data: enriched})
				_ = enricher.HandleMapping(repo.ESIndex)

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

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)

	auth0Client, err := auth0.NewAuth0Client(m.ESCacheURL, m.ESUsername, m.ESCachePassword, m.Environment, m.AuthGrantType, m.AuthClientID, m.AuthClientSecret, m.AuthAudience, m.AuthURL)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return fetcher, enricher, esClientProvider, auth0Client, err
}
