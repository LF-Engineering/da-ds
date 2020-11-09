package dockerhub

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
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
	}

	return mng
}

func (m *Manager) Sync() error {
	fetcher, enricher, err := buildServices(m)
	if err != nil {
		return err
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

	if m.EnrichOnly {
		hits, err := enricher.GetPreviouslyFetchedData(m.Repositories)
		if err != nil {
			return err
		}

		for _, hit := range hits.Hits.Hits {
			err = m.enrich(enricher, hit.Source, &Repository{hit.Source.Data.Namespace, hit.Source.Data.Name}, enrichData)
			if err != nil {
				return err
			}
		}
	}

	for _, repo := range m.Repositories {
		var raw *RepositoryRaw

		if m.EnrichOnly == false {
			// Fetch data for single repo
			raw, err = fetcher.FetchItem(repo.Owner, repo.Repository)
			if err != nil {
				return errors.New(fmt.Sprintf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository))
			}
			rawData = append(rawData, raw)
		}

		err = m.enrich(enricher, raw, repo, enrichData)
		if err != nil {
			return err
		}
	}

	// Insert raw data to elasticsearch
	_, err = fetcher.BulkInsert(rawData)
	if err != nil {
		return err
	}

	// Insert enriched data to elasticsearch
	_, err = enricher.BulkInsert(enrichData)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) enrich(enricher *Enricher, raw *RepositoryRaw, repo *Repository, enrichData []*RepositoryEnrich) error {
	if m.Enrich {
		// Enrich data for single repo
		enriched, err := enricher.EnrichItem(*raw)
		if err != nil {
			return errors.New(fmt.Sprintf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository))
		}
		enrichData = append(enrichData, enriched)
	}
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
