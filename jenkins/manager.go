package jenkins

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"

	"github.com/LF-Engineering/dev-analytics-libraries/http"
)

// Manager describes Jenkins manager
type Manager struct {
	FetcherBackendVersion  string
	EnricherBackendVersion string
	EnrichOnly             bool
	Enrich                 bool
	ESUrl                  string
	ESUsername             string
	ESPassword             string
	HTTPTimeout            time.Duration
	BuildServers           []*BuildServer
	FromDate               *time.Time
	NoIncremental          bool
	BulkSize               int
}

// BuildServer is a single Jenkins
// Configuration for a single project
type BuildServer struct {
	Username string `json:"username"`
	Password string `json:"password"`
	URL      string `json:"url"`
	Project  string `json:"project"`
	Index    string `json:"index"`
}

// NewManager initiates Jenkins manager instance
func NewManager(
	fetcherBackendVersion string,
	enricherBackendVersion string,
	enrichOnly bool,
	enrich bool,
	eSUrl string,
	httpTimeout time.Duration,
	buildServers []*BuildServer,
	fromDate *time.Time,
	noIncremental bool,
	bulkSize int,
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
		BulkSize:               bulkSize,
	}

	return mng
}

// Sync runs jenkins fetch and enrich according to passed parameters
func (m *Manager) Sync() error {

	if len(m.BuildServers) == 0 {
		return errors.New("no repositories found")
	}

	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}
	if !m.EnrichOnly {
		log.Printf("Jenkins: Fetching data for %v\n", m.BuildServers)
		data := make([]elastic.BulkData, 0)
		// fetch data
		for _, buildServer := range m.BuildServers {
			log.Printf("Fetching data for %s project: %s endpoint", buildServer.Project, buildServer.URL)
			var raw []BuildsRaw
			// Fetch data for single build
			raw, err = fetcher.FetchItem(&Params{
				JenkinsURL: buildServer.URL,
				Username:   buildServer.Username,
				Password:   buildServer.Password,
				Depth:      Depth,
			})
			if err != nil {
				return fmt.Errorf("could not fetch data from repository: %s-%s", buildServer.URL, buildServer.Project)
			}
			for _, builds := range raw {
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", buildServer.Index), ID: builds.UUID, Data: builds})
			}

			log.Printf("Successfully fetched %d documents for %s project : %s endpoint\n", len(raw), buildServer.Project, buildServer.URL)
			// set mapping and create index if not exists
			_ = fetcher.HandleMapping(fmt.Sprintf("%s-raw", buildServer.Index))
		}

		if len(data) > 0 {
			// Insert raw data to elasticsearch
			// Process in bulk
			log.Println("Exporting fetched builds to raw indices")
			for start:=0; start<len(data); start+=m.BulkSize {
				end := start + m.BulkSize
				if end > len(data) {
					end = len(data)
				}
				batch := data[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					log.Println("Error while BulkInsert in Jenkins enrich index: ", err)
					return err
				}
				log.Printf("Jenkins: Exported %d documents to raw index\n", len(batch))
			}
			log.Println("Completed the export for jenkins raw builds.")
		}
	}

	if m.Enrich || m.EnrichOnly {
		log.Printf("Jenkins: Enriching data for %v\n", m.BuildServers)
		data := make([]elastic.BulkData, 0)
		for _, buildServer := range m.BuildServers {
			log.Printf("Enriching data for %s project: %s endpoint\n", buildServer.Project, buildServer.URL)
			var fromDate *time.Time
			var lastDate time.Time
			if m.FromDate == nil || (*m.FromDate).IsZero() {
				lastDate, err = fetcher.GetLastDate(*buildServer, time.Now())
				if err != nil {
					log.Println("[GetLastDate] could not get last date")
					lastDate = DefaultDateTime
				}
			} else {
				fromDate = m.FromDate
			}
			esData, err := enricher.GetFetchedDataItem(buildServer, fromDate, &lastDate, m.NoIncremental)
			if err != nil {
				return err
			}
			if len(esData.Hits.Hits) > 0 {
				// Enrich data for all the builds fetched from raw
				for _, hit := range esData.Hits.Hits {
					enriched, err := enricher.EnrichItem(*hit.Source, buildServer.Project, time.Now())
					if err != nil {
						log.Printf("could not enrich data from repository: %s-%s", buildServer.Project, buildServer.URL)
						continue
					}
					data = append(data, elastic.BulkData{IndexName: buildServer.Index, ID: enriched.UUID, Data: *enriched})
				}
				_ = enricher.HandleMapping(buildServer.Index)
			}
			log.Printf("Successfully enriched %d documents for %s project : %s endpoint\n", len(data), buildServer.Project, buildServer.URL)
		}

		if len(data) > 0 {
			// Insert enriched data to elasticsearch
			log.Println("Exporting enriched builds to enriched indices")

			// Process in bulk
			for start:=0; start<len(data); start+=m.BulkSize {
				end := start + m.BulkSize
				if end > len(data) {
					end = len(data)
				}
				batch := data[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					log.Println("Error while BulkInsert in Jenkins enrich index: ", err)
					return err
				}
				log.Printf("Exported %d documents", len(batch))
			}
			log.Println("Completed the export for jenkins enriched builds.")
		}
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := http.NewClientProvider(m.HTTPTimeout)
	params := &Params{
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Initialize fetcher object to get data from jenkins api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)

	return fetcher, enricher, esClientProvider, err
}
