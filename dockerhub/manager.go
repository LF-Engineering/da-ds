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
	FromDate               *time.Time
	NoIncremental          bool
}

type Repository struct {
	Owner      string
	Repository string
	ESIndex    string
}

func NewManager(Username string,
	Password string,
	FetcherBackendVersion string,
	EnricherBackendVersion string,
	EnrichOnly bool,
	Enrich bool,
	ESUrl string,
	HttpTimeout time.Duration,
	Repositories []*Repository,
	FromDate *time.Time,
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
		HttpTimeout:            HttpTimeout,
		Repositories:           Repositories,
		FromDate:               FromDate,
		NoIncremental:          NoIncremental,
	}

	return mng
}

func (m *Manager) Sync() error {

	if len(m.Repositories) == 0 {
		return errors.New("no repositories found")
	}

	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}

	// Get dockerhub token if needed to get data from private repos
	if m.Password != "" {
		_, err := fetcher.Login(m.Username, m.Password)
		if err != nil {
			return err
		}
	}

	if !m.EnrichOnly {
		data := make([]*utils.BulkData, 0)

		// fetch data
		for _, repo := range m.Repositories {
			var raw *RepositoryRaw
			// Fetch data for single repo
			raw, err = fetcher.FetchItem(repo.Owner, repo.Repository)
			if err != nil {
				return errors.New(fmt.Sprintf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository))
			}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", repo.ESIndex), ID: raw.UUID, Data: raw})

			_ = fetcher.HandleMapping(fmt.Sprintf("%s-raw", repo.ESIndex))
		}

		if len(data) > 0 {
			// Insert raw data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}
	}

	if m.Enrich || m.EnrichOnly {
		data := make([]*utils.BulkData, 0)

		for _, repo := range m.Repositories {
			var fromDate *time.Time
			var lastDate time.Time
			if m.FromDate == nil || (*m.FromDate).IsZero() {
				lastDate, err = fetcher.GetLastDate(repo)
				if err != nil {
					log.Println("[GetLastDate] could not get last date")
				}
			} else {
				fromDate = m.FromDate
			}

			esData, err := enricher.GetPreviouslyFetchedDataItem(repo, fromDate, &lastDate, m.NoIncremental)
			if err != nil {
				return err
			}

			if len(esData.Hits.Hits) > 0 {
				// Enrich data for single repo
				enriched, err := enricher.EnrichItem(*esData.Hits.Hits[0].Source)
				if err != nil {
					return errors.New(fmt.Sprintf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository))
				}
				data = append(data, &utils.BulkData{IndexName: repo.ESIndex, ID: fmt.Sprintf("%s_%s", enriched.ID, enriched.RepositoryType), Data: enriched})

				_ = enricher.HandleMapping(repo.ESIndex)

			}
		}

		if len(data) > 0 {
			// Insert enriched data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
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
		return nil, nil, nil, err
	}

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)

	return fetcher, enricher, esClientProvider, err
}
