package dockerhub

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"time"
)

type Manager struct {
	Username       string
	Password       string
	FetcherBackendVersion string
	EnricherBackendVersion string
	ESUrl          string
	ESUsername     string
	ESPassword     string
	HttpTimeout    time.Duration
	Repositories   Repositories
}

type Repositories []*struct {
	Owner      string
	Repository string
}

func NewManager(Username string,
	Password string,
	FetcherBackendVersion string,
	EnricherBackendVersion string,
	ESUrl string,
	ESUsername string,
	ESPassword string,
	HttpTimeout time.Duration) *Manager {
	mng := &Manager{Username: Username, Password: Password, FetcherBackendVersion: FetcherBackendVersion,
		EnricherBackendVersion: EnricherBackendVersion,
		ESUrl: ESUrl, ESUsername: ESUsername, ESPassword: ESPassword, HttpTimeout: HttpTimeout}

	return mng
}

func (m *Manager) Do() error {
	fetcher, enricher, err := prepare(m)
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

	for _, repo := range m.Repositories {
		// Fetch data for single repo
		raw, err := fetcher.FetchItem(repo.Owner, repo.Repository)
		if err != nil {
			return errors.New(fmt.Sprintf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository))
		}
		rawData = append(rawData, raw)

		// Enrich data for single repo
		enriched, err := enricher.EnrichItem(*raw)
		if err != nil {
			return errors.New(fmt.Sprintf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository))
		}
		enrichData = append(enrichData, enriched)
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

func prepare(m *Manager) (*Fetcher, *Enricher, error) {
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
