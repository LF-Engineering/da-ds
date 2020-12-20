package bugzillarest

import (
"fmt"
	"strconv"
	"time"

"github.com/LF-Engineering/da-ds/affiliation"
db "github.com/LF-Engineering/da-ds/db"
"github.com/LF-Engineering/da-ds/utils"
)


// ESClientProvider used in connecting to ES server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []*utils.BulkData) ([]byte, error)
}

// Manager describes bugzilla manager
type Manager struct {
	Endpoint               string
	SHConnString           string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	Fetch                  bool
	Enrich                 bool
	ESUrl                  string
	ESUsername             string
	ESPassword             string
	ESIndex                string
	FromDate               *time.Time
	HTTPTimeout            time.Duration
	Project                string
	FetchSize              int
	EnrichSize             int

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
}

// NewManager initiates bugzilla manager instance
func NewManager(endPoint string, shConnStr string, fetcherBackendVersion string, enricherBackendVersion string, fetch bool, enrich bool, eSUrl string, esUser string, esPassword string, esIndex string, fromDate *time.Time, project string, fetchSize int, enrichSize int) (*Manager, error) {

	mgr := &Manager{
		Endpoint:               endPoint,
		SHConnString:           shConnStr,
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		Fetch:                  fetch,
		Enrich:                 enrich,
		ESUrl:                  eSUrl,
		ESUsername:             esUser,
		ESPassword:             esPassword,
		ESIndex:                esIndex,
		FromDate:               fromDate,
		HTTPTimeout:            60 * time.Second,
		Project:                project,
		FetchSize:              fetchSize,
		EnrichSize:             enrichSize,
	}

	fetcher, enricher, esClientProvider, err := buildServices(mgr)
	if err != nil {
		return nil, err
	}

	mgr.fetcher = fetcher
	mgr.enricher = enricher
	mgr.esClientProvider = esClientProvider

	return mgr, nil
}

// TopHits result
type TopHits struct {
	Hits Hits `json:"hits"`
}

// Hits result
type Hits struct {
	Hits []NestedHits `json:"hits"`
}

// NestedHits is the actual hit data
type NestedHits struct {
	ID     string    `json:"_id"`
	Source HitSource `json:"_source"`
}

// HitSource is the document _source data
type HitSource struct {
	ID        string    `json:"id"`
	ChangedAt time.Time `json:"changed_at"`
}

// Sync starts fetch and enrich processes
func (m *Manager) Sync() error {
	lastActionCachePostfix := "-last-action-date-cache"

	// register disabled job as done
	doneJobs := make(map[string]bool)
	doneJobs["doneFetch"] = !m.Fetch
	doneJobs["doneEnrich"] = !m.Enrich

	fetchCh := m.fetch(m.fetcher, lastActionCachePostfix)

	for doneJobs["doneFetch"] == false || doneJobs["doneEnrich"] == false {
		select {
		case err := <-fetchCh:
			if err == nil {
				doneJobs["doneFetch"] = true
			}
		//case err := <-m.enrich(m.enricher, lastActionCachePostfix):
		//	if err == nil {
		//		doneJobs["doneEnrich"] = true
		//	}
		//	time.Sleep(5*time.Second)
		}
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := utils.NewHTTPClientProvider(m.HTTPTimeout)
	//params := &Params{
	//	Endpoint:       m.Endpoint,
	//	BackendVersion: m.FetcherBackendVersion,
	//}
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher( httpClientProvider)

	dataBase, err := db.NewConnector("mysql", m.SHConnString)
	if err != nil {
		return nil, nil, nil, err
	}
	identityProvider := affiliation.NewIdentityProvider(dataBase)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, m.Project)

	return fetcher, enricher, esClientProvider, err
}

func (m *Manager) fetch(fetcher *Fetcher, lastActionCachePostfix string) <-chan error {
	ch := make(chan error)
	go func() {
		fetchID := "fetch"

		query := map[string]interface{}{
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"id": map[string]string{
						"value": fetchID},
				},
			},
		}
		result := m.FetchSize

		val := &TopHits{}
		err := m.esClientProvider.Get(fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), query, val)

		now := time.Now().UTC()
		var lastFetch *time.Time

		if err == nil && len(val.Hits.Hits) > 0 {
			lastFetch = &val.Hits.Hits[0].Source.ChangedAt
		}

		from := utils.GetOldestDate(m.FromDate, lastFetch)
		date := from.Format("2006-01-02T15:04:05")

		offset := 0
		for result == m.FetchSize {
			data := make([]*utils.BulkData, 0)
			bugs, err := fetcher.FetchAll(m.Endpoint, date,strconv.Itoa(m.FetchSize), strconv.Itoa(offset) , now)
			if err != nil {
				ch <- err
				return
			}

			result = len(bugs)
			offset += result
			if result != 0 {
				from = &bugs[len(bugs)-1].Data.LastChangeTime
			}


				for _, bug := range bugs {
					data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
				}


			// set mapping and create index if not exists
			//_, err = m.esClientProvider.CreateIndex(fmt.Sprintf("%s-raw", m.ESIndex), BugzillaRawMapping)

			if err != nil {
				ch <- err
				return
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(BugzillaRestRaw)
				updateChan := HitSource{ID: fetchID, ChangedAt: cacheDoc.Data.LastChangeTime}
				data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

				// Insert raw data to elasticsearch
				_, err = m.esClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err
					return
				}
			}

		}

		ch <- nil
	}()

	return ch
}


