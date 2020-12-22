package jenkins

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"log"
	"time"
)

// Manager describes Jenkins manager
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
	BuildServers		   []*BuildServer
	FromDate               *time.Time
	NoIncremental          bool
}

type BuildServer struct {
	Username    string	`json:"username"`
	Password    string  `json:"password"`
	URL  		string	`json:"url"`
	Project 	string  `json:"project"`
	Index       string  `json:"index"`
}

// NewManager initiates piper mail manager instance
func NewManager(
	fetcherBackendVersion string,
	enricherBackendVersion string,
	enrichOnly bool,
	enrich bool,
	eSUrl string,
	httpTimeout time.Duration,
	buildServers  []*BuildServer,
	fromDate *time.Time,
	noIncremental bool,
) *Manager {
	mng := &Manager{
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		EnrichOnly:             enrichOnly,
		Enrich:                 enrich,
		ESUrl:                  eSUrl,
		HTTPTimeout:            httpTimeout,
		BuildServers:           buildServers,
		FromDate:               fromDate,
		NoIncremental:          noIncremental,
	}

	return mng
}

// Sync runs dockerhub fetch and enrich according to passed parameters
func (m *Manager) Sync() error {

	if len(m.BuildServers) == 0 {
		return errors.New("no repositories found")
	}

	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}
	if !m.EnrichOnly {
		data := make([]*utils.BulkData, 0)

		// fetch data
		for _, buildServer := range m.BuildServers {
			var raw []JenkinsRaw
			// Fetch data for single repo
			raw, err = fetcher.FetchItem(&Params{
				JenkinsURL:     buildServer.URL,
				Username:       m.Username,
				Password:       m.Password,
				Depth:          Depth,
				BackendVersion: "0.1.0",
			})
			if err != nil {
				return fmt.Errorf("could not fetch data from repository: %s-%s", buildServer.URL, buildServer.Project)
			}
			for _, builds := range raw{
				data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", buildServer.Index), ID: builds.UUID, Data: builds})
			}

			// set mapping and create index if not exists
			_ = fetcher.HandleMapping(fmt.Sprintf("%s-raw", buildServer.Index))
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

		for _, buildServer := range m.BuildServers {
			var fromDate *time.Time
			var lastDate time.Time
			if m.FromDate == nil || (*m.FromDate).IsZero() {
				lastDate, err = fetcher.GetLastDate(*buildServer, time.Now())
				if err != nil {
					log.Println("[GetLastDate] could not get last date")
				}
			} else {
				fromDate = m.FromDate
			}
			esData, err := enricher.GetFetchedDataItem(buildServer, fromDate, &lastDate, m.NoIncremental)
			if err != nil {
				return err
			}
			if len(esData.Hits.Hits) > 0 {
				// Enrich data for single repo
				enriched, err := enricher.EnrichItem(*esData.Hits.Hits[0].Source, buildServer.Project, time.Now())
				if err != nil {
					return fmt.Errorf("could not enrich data from repository: %s-%s", buildServer.Project, buildServer.URL)
				}
				data = append(data, &utils.BulkData{IndexName: buildServer.Index, ID: enriched.UUID, Data: *enriched})
				_ = enricher.HandleMapping(buildServer.Index)
			}
		}

		if len(data) > 0 {
			// Insert enriched data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
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
