package bugzilla

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	db "github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/da-ds/utils"
)

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

	errCH := make(chan error)
	if m.Fetch {
		go func() {
			err := m.fetch(m.fetcher, lastActionCachePostfix)
			if err != nil {
				errCH <- err
			}
		}()

	}

	if m.Enrich {
		go func() {
			err := m.enrich(m.enricher, lastActionCachePostfix)
			if err != nil {
				errCH <- err
			}
		}()
	}

	select {
	case err := <-errCH:
		return err
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
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
		return nil, nil, nil, err
	}

	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	dataBase, err := db.NewConnector("mysql", m.SHConnString)
	if err != nil {
		return nil, nil, nil, err
	}
	identityProvider := affiliation.NewIdentityProvider(dataBase)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, m.Project)

	return fetcher, enricher, esClientProvider, err
}

func (m *Manager) fetch(fetcher *Fetcher, lastActionCachePostfix string) error {
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

	round := false
	for result == m.FetchSize {
		data := make([]*utils.BulkData, 0)
		bugs, err := fetcher.FetchItem(*from, m.FetchSize, now)
		if err != nil {
			return err
		}

		result = len(bugs)
		if result != 0 {
			from = &bugs[len(bugs)-1].ChangedAt
		}

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

		// set mapping and create index if not exists
		_, err = m.esClientProvider.CreateIndex(fmt.Sprintf("%s-raw", m.ESIndex), BugzillaRawMapping)

		if err != nil {
			return err
		}

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*BugRaw)
			updateChan := HitSource{ID: fetchID, ChangedAt: cacheDoc.MetadataUpdatedOn}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

			// Insert raw data to elasticsearch
			_, err = m.esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (m *Manager) enrich(enricher *Enricher, lastActionCachePostfix string) error {
	enrichID := "enrich"

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"id": map[string]string{
					"value": enrichID},
			},
		},
	}

	val := &TopHits{}
	err := m.esClientProvider.Get(fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), query, val)

	query = map[string]interface{}{
		"size": 10000,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{},
			},
		},
		"sort": []map[string]interface{}{
			{
				"metadata__updated_on": map[string]string{
					"order": "desc",
				},
			},
		},
	}

	var topHits *RawHits
	var lastEnrich time.Time
	if err == nil && len(val.Hits.Hits) > 0 {
		lastEnrich = val.Hits.Hits[0].Source.ChangedAt
	}

	from := utils.GetOldestDate(m.FromDate, &lastEnrich)

	conditions := map[string]interface{}{
		"range": map[string]interface{}{
			"metadata__updated_on": map[string]interface{}{
				"gte": (from).Format(time.RFC3339),
			},
		},
	}

	query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = conditions

	results := m.EnrichSize
	offset := 0
	query["size"] = m.EnrichSize

	for results == m.EnrichSize {

		// make pagination to get the specified size of documents with offset
		query["from"] = offset
		topHits, err = m.fetcher.Query(fmt.Sprintf("%s-raw", m.ESIndex), query)
		if err != nil {
			return err
		}

		data := make([]*utils.BulkData, 0)
		for _, hit := range topHits.Hits.Hits {
			enrichedItem, err := enricher.EnrichItem(hit.Source, time.Now().UTC())
			if err != nil {
				return err
			}
			data = append(data, &utils.BulkData{IndexName: m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem})
		}

		results = len(data)
		offset += results

		// setting mapping and create index if not exists
		if offset == 0 {
			_, err := m.esClientProvider.CreateIndex(m.ESIndex, BugzillaEnrichMapping)

			if err != nil {
				return err
			}
		}

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*BugEnrich)
			updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.MetadataEnrichedOn}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

			// Insert enriched data to elasticsearch
			_, err = m.esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}

	}

	return nil
}
