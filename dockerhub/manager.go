package dockerhub

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"log"
	"time"
)

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
	HttpTimeout            time.Duration
	Repositories           []*Repository
	FromDate               string
	FromOffset             int64
	NoIncremental          bool
}

type Repository struct {
	Owner      string
	Repository string
}

func NewManager(Username string,
	Password string,
	FetcherBackendVersion string,
	EnricherBackendVersion string,
	EnrichOnly bool,
	Enrich bool,
	ESUrl string,
	ESUsername string,
	ESPassword string,
	HttpTimeout time.Duration,
	Repositories []*Repository,
	FromDate string,
	FromOffset int64,
	NoIncremental bool,
) *Manager {
	mng := &Manager{
		Username:               Username,
		Password:               Password,
		FetcherBackendVersion:  FetcherBackendVersion,
		EnricherBackendVersion: EnricherBackendVersion,
		EnrichOnly:             EnrichOnly,
		Enrich:                 Enrich,
		ESUrl:                  ESUrl,
		ESUsername:             ESUsername,
		ESPassword:             ESPassword,
		HttpTimeout:            HttpTimeout,
		Repositories:           Repositories,
		FromDate:               FromDate,
		FromOffset:             FromOffset,
		NoIncremental:          NoIncremental,
	}

	return mng
}

func (m *Manager) Sync() error {
	fetcher, enricher, err := buildServices(m)
	if err != nil {
		return err
	}

	if m.FromDate != "" && m.FromOffset > 0 {
		return errors.New("can not feed using from_date and from_offset")
	}

	// Repo array
	rawData := make([]*RepositoryRaw, 0)
	enrichData := make([]*RepositoryEnrich, 0)

	// Get dockerhub token if needed to get data from private repos
	if m.Password != "" {
		_, err := fetcher.Login(m.Username, m.Password)
		if err != nil {
			return err
		}
	}

	if !m.EnrichOnly{
		// fetch data
		for _, repo := range m.Repositories {
			var raw *RepositoryRaw
			// Fetch data for single repo
			raw, err = fetcher.FetchItem(repo.Owner, repo.Repository)
			if err != nil {
				return errors.New(fmt.Sprintf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository))
			}
			rawData = append(rawData, raw)
		}

		// Insert raw data to elasticsearch
		_, err = fetcher.BulkInsert(rawData)
		if err != nil {
			return err
		}
	}

	if m.Enrich || m.EnrichOnly {
		for _, repo := range m.Repositories {
			var lastDate *time.Time
			if m.FromDate == "" {
				lastDate, err = fetcher.GetLastDate(repo)
				if err != nil {
					log.Println("[GetLastDate] could not get last date")
				}
			}

			d, err := time.Parse(time.RFC3339, m.FromDate)

			esData, err := enricher.GetPreviouslyFetchedDataItem(*repo, &d, lastDate, m.NoIncremental)
			if err != nil {
				return err
			}

			if len(esData.Hits.Hits) > 0 {
				err = m.enrich(enricher, esData.Hits.Hits[0].Source, repo, enrichData)
				if err != nil {
					return err
				}
			}

		}

		// Insert enriched data to elasticsearch
		_, err = enricher.BulkInsert(enrichData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) enrich(enricher *Enricher, raw *RepositoryRaw, repo *Repository, enrichData []*RepositoryEnrich) error {

	// Enrich data for single repo
	enriched, err := enricher.EnrichItem(*raw)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository))
	}
	enrichData = append(enrichData, enriched)
	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, error) {
	httpClientProvider := utils.NewHttpClientProvider(m.HttpTimeout)
	params := &Params{
		Username:       m.Username,
		Password:       m.Password,
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, err
	}

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)

	return fetcher, enricher, err
}
