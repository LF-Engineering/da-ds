package bugzilla

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
)

// Manager describes bugzilla manager
type Manager struct {
	Endpoint               string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	Fetch                  bool
	Enrich                 bool
	ESUrl                  string
	ESUsername             string
	ESPassword             string
	ESIndex                string
	HTTPTimeout            time.Duration
}

// NewManager initiates bugzilla manager instance
func NewManager(
	endPoint string,
	fetcherBackendVersion string,
	enricherBackendVersion string,
	fetch bool,
	enrich bool,
	eSUrl string,
	esUser string,
	esPassword string,
	esIndex string,
) *Manager {
	mng := &Manager{
		Endpoint:               endPoint,
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		Fetch:                  fetch,
		Enrich:                 enrich,
		ESUrl:                  eSUrl,
		ESUsername:             esUser,
		ESPassword:             esPassword,
		ESIndex:                esIndex,
		HTTPTimeout:            50 * time.Second,
	}

	return mng
}

// TopHits result
type TopHits struct {
	Hits Hits `json:"hits"`
}

// Hits result
type Hits struct {
	Hits []NestedHits `json:"hits"`
}

// Nestedhits is the actual hit data
type NestedHits struct {
	Id     string    `json:"_id"`
	Source HitSource `json:"_source"`
}

// HitSource is the document _source data
type HitSource struct {
	Id        string    `json:"id"`
	ChangedAt time.Time `json:"changed_at"`
}

func (m *Manager) Sync() error {

	fetcher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}

	if m.Fetch {

		query := map[string]interface{}{
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"id": map[string]string{
						"value": "1"},
				},
			},
		}

		limit := 25
		result := 25
		cachePostfix := "-lastfetchingdate-cache"

		val := &TopHits{}
		err = esClientProvider.Get(m.ESIndex+cachePostfix, query, val)

		now := time.Now()
		var from time.Time

		if err != nil {
			// Todo : update date to 1970
			from, err = time.Parse("2006-01-02 15:04:05", "2020-12-04 10:54:21")
			if err != nil {
				return err
			}
		} else {
			from = val.Hits.Hits[0].Source.ChangedAt
		}

		data := make([]*utils.BulkData, 0)
		round := false
		for result == limit {
			bugs, err := fetcher.FetchItem(from, limit, now)
			if err != nil {
				return err
			}

			from = bugs[len(bugs)-1].ChangedAt
			result = len(bugs)

			if result < 2 {
				bugs = nil
			} else if round {
				for _, bug := range bugs {
					data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
				}
				round = true
			} else {
				bugs = bugs[1:result]
				for _, bug := range bugs {
					data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
				}
			}
		}

		// set mapping and create index if not exists
		//ind := m.ESIndex + cachePostfix
		//_ = fetcher.HandleMapping(fmt.Sprintf(ind))

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*BugRaw)
			cacheId := "1"
			updateChan := HitSource{Id: cacheId, ChangedAt: cacheDoc.ChangedAt}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, cachePostfix), ID: cacheId, Data: updateChan})

			// Insert raw data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}

	}

	if m.Enrich {

	}
	return nil
}

func buildServices(m *Manager) (*Fetcher, ESClientProvider, error) {
	httpClientProvider := utils.NewHTTPClientProvider(m.HTTPTimeout)
	params := &Params{
		Endpoint:       m.Endpoint,
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

	return fetcher, esClientProvider, err
}
