package dockerhub

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
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
}

// Repository represents dockerhub repository data
type Repository struct {
	Owner      string
	Repository string
	Project    string
	ESIndex    string
}

// NewManager initiates dockerhub manager instance
func NewManager(username string,
	password string,
	fetcherBackendVersion string,
	enricherBackendVersion string,
	enrichOnly bool,
	enrich bool,
	eSUrl string,
	httpTimeout time.Duration,
	repositories []*Repository,
	fromDate *time.Time,
	noIncremental bool,
) *Manager {
	mng := &Manager{
		Username:               username,
		Password:               password,
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		EnrichOnly:             enrichOnly,
		Enrich:                 enrich,
		ESUrl:                  eSUrl,
		HTTPTimeout:            httpTimeout,
		Repositories:           repositories,
		FromDate:               fromDate,
		NoIncremental:          noIncremental,
	}

	return mng
}

// Sync runs dockerhub fetch and enrich according to passed parameters
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
			raw, err = fetcher.FetchItem(repo.Owner, repo.Repository, time.Now())
			if err != nil {
				return fmt.Errorf("could not fetch data from repository: %s-%s", repo.Owner, repo.Repository)
			}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", repo.ESIndex), ID: raw.UUID, Data: raw})

			// set mapping and create index if not exists
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
				lastDate, err = fetcher.GetLastDate(repo, time.Now())
				if err != nil {
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
				data = append(data, &utils.BulkData{IndexName: repo.ESIndex, ID: enriched.UUID, Data: enriched})
				_ = enricher.HandleMapping(repo.ESIndex)

			}
		}

		if len(data) > 0 {
			// Insert enriched data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}

			// Add/Update latest document in each origin
			for _, repo := range data {
				repo.ID = fmt.Sprintf("%s_%s", repo.Data.(*RepositoryEnrich).ID, repo.Data.(*RepositoryEnrich).RepositoryType)
				repo.Data.(*RepositoryEnrich).IsDockerImage = 1
				repo.Data.(*RepositoryEnrich).IsEvent = 0

				data = append(data, repo)
			}

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
	httpClientProvider := utils.NewHTTPClientProvider(m.HTTPTimeout)
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
