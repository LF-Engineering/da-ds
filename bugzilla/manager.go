package bugzilla

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/affiliation"
	"github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
	"time"
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
	Retries                uint
	Delay                  time.Duration
	GabURL                 string

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
}

// NewManager initiates bugzilla manager instance
func NewManager(endPoint string, shConnStr string, fetcherBackendVersion string, enricherBackendVersion string, fetch bool, enrich bool, eSUrl string, esUser string, esPassword string, esIndex string, fromDate *time.Time, project string, fetchSize int, enrichSize int, retries uint, delay time.Duration, gapURL string) (*Manager, error) {

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
		Retries:                retries,
		Delay:                  delay,
		GabURL:                 gapURL,
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
		case err := <-m.enrich(m.enricher, lastActionCachePostfix):
			if err == nil {
				doneJobs["doneEnrich"] = true
			}
			time.Sleep(5 * time.Second)
		}
	}

	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := http.NewHTTPClientProvider(m.HTTPTimeout)
	params := &Params{
		Endpoint:       m.Endpoint,
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := utils.NewClientProvider(&utils.Params{
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

		from := timeLib.GetOldestDate(m.FromDate, lastFetch)

		round := false
		for result == m.FetchSize {
			data := make([]utils.BulkData, 0)
			bugs, err := fetcher.FetchItem(*from, m.FetchSize, now)
			if err != nil {
				ch <- err
				return
			}

			result = len(bugs)
			if result != 0 {
				from = &bugs[len(bugs)-1].ChangedAt
			}

			if result < 2 {
				bugs = nil
			} else if round {
				for _, bug := range bugs {
					data = append(data, utils.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
				}
				round = true
			} else {
				bugs = bugs[1:result]
				for _, bug := range bugs {
					data = append(data, utils.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
				}
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*BugRaw)
				updateChan := HitSource{ID: fetchID, ChangedAt: cacheDoc.ChangedAt}
				data = append(data, utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

				err := utils.DelayOfCreateIndex(m.esClientProvider.CreateIndex, m.Retries, m.Delay, fmt.Sprintf("%s-raw", m.ESIndex), BugzillaRawMapping)
				if err != nil {
					ch <- err

					byteData, err := json.Marshal(data)
					if err != nil {
						ch <- err
						return
					}
					dataEnc := b64.StdEncoding.EncodeToString(byteData)
					gabBody := map[string]string{"payload": dataEnc}
					bData, err := json.Marshal(gabBody)
					if err != nil {
						ch <- err
						return
					}

					c, e, err := m.fetcher.HTTPClientProvider.Request(m.GabURL, "POST", nil, bData, nil)
					if err != nil {
						ch <- err
						return
					}
					fmt.Println(c, string(e))
					continue
				}

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

func (m *Manager) enrich(enricher *Enricher, lastActionCachePostfix string) <-chan error {
	ch := make(chan error)

	go func() {
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

		from := timeLib.GetOldestDate(m.FromDate, &lastEnrich)

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
				ch <- nil
				return
			}

			data := make([]utils.BulkData, 0)
			for _, hit := range topHits.Hits.Hits {
				enrichedItem, err := enricher.EnrichItem(hit.Source, time.Now().UTC())
				if err != nil {
					ch <- err
					return
				}
				data = append(data, utils.BulkData{IndexName: m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem})
			}

			results = len(data)
			offset += results

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*BugEnrich)
				updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.ChangedDate}
				data = append(data, utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

				// setting mapping and create index if not exists
				if offset == 0 {
					_, err := m.esClientProvider.CreateIndex(m.ESIndex, BugzillaEnrichMapping)

					if err != nil {
						ch <- err
						return
					}
				}

				// Insert enriched data to elasticsearch
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
