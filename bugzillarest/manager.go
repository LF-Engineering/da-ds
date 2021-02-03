package bugzillarest

import (
	"fmt"
	"strconv"
	"time"

	"github.com/LF-Engineering/da-ds/util"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// ESClientProvider used in connecting to ES server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []elastic.BulkData) ([]byte, error)
	DelayOfCreateIndex(ex func(str string, b []byte) ([]byte, error), uin uint, du time.Duration, index string, data []byte) error
}

// AuthClientProvider interacts with auth0 server
type AuthClientProvider interface {
	ValidateToken(env string) (string, error)
}

// FetchProvider contains fetch functionalities
type FetchProvider interface {
	FetchAll(origin string, date string, limit string, offset string, now time.Time) ([]Raw, *time.Time, error)
	FetchItem(origin string, bugID int, fetchedBug BugData, now time.Time) (*Raw, error)
	Query(index string, query map[string]interface{}) (*RawHits, error)
}

// EnrichProvider enrich Bugzilla raw
type EnrichProvider interface {
	EnrichItem(rawItem Raw, now time.Time) (*BugRestEnrich, error)
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
	ProjectSlug            string
	AffBaseURL             string
	ESCacheURL             string
	ESCacheUsername        string
	ESCachePassword        string
	AuthGrantType          string
	AuthClientID           string
	AuthClientSecret       string
	AuthAudience           string
	AuthURL                string
	Environment            string
	Slug                   string

	EsClientProvider    ESClientProvider
	Fetcher             FetchProvider
	Enricher            EnrichProvider
	Auth0ClientProvider Auth0ClientProvider
	HTTPClientProvider  HTTPClientProvider

	Retries uint
	Delay   time.Duration
	GapURL  string
}

// MgrParams required for creating a new instance of Bugzillarest manager
type MgrParams struct {
	EndPoint               string
	ShConnStr              string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	Fetch                  bool
	Enrich                 bool
	ESUrl                  string
	EsUser                 string
	EsPassword             string
	EsIndex                string
	FromDate               *time.Time
	HTTPTimeout            time.Duration
	Project                string
	FetchSize              int
	EnrichSize             int
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	ProjectSlug            string
	AffBaseURL             string
	ESCacheURL             string
	ESCacheUsername        string
	ESCachePassword        string
	AuthGrantType          string
	AuthClientID           string
	AuthClientSecret       string
	AuthAudience           string
	AuthURL                string
	Environment            string
	Slug                   string

	Fetcher             FetchProvider
	Enricher            EnrichProvider
	ESClientProvider    ESClientProvider
	Auth0ClientProvider Auth0ClientProvider
	HTTPClientProvider  HTTPClientProvider
}

// NewManager initiates bugzilla manager instance
func NewManager(params *MgrParams) (*Manager, error) {

	mgr := &Manager{
		Endpoint:               params.EndPoint,
		SHConnString:           params.ShConnStr,
		FetcherBackendVersion:  params.FetcherBackendVersion,
		EnricherBackendVersion: params.EnricherBackendVersion,
		Fetch:                  params.Fetch,
		Enrich:                 params.Enrich,
		ESUrl:                  params.ESUrl,
		ESUsername:             params.EsUser,
		ESPassword:             params.EsPassword,
		ESIndex:                params.EsIndex,
		FromDate:               params.FromDate,
		HTTPTimeout:            params.HTTPTimeout,
		Project:                params.Project,
		FetchSize:              params.FetchSize,
		EnrichSize:             params.EnrichSize,
		Retries:                params.Retries,
		Delay:                  params.Delay,
		GapURL:                 params.GapURL,
		ProjectSlug:            params.ProjectSlug,
		AffBaseURL:             params.AffBaseURL,
		ESCacheURL:             params.ESCacheURL,
		ESCacheUsername:        params.ESCacheUsername,
		ESCachePassword:        params.ESCachePassword,
		AuthGrantType:          params.AuthGrantType,
		AuthClientID:           params.AuthClientID,
		AuthClientSecret:       params.AuthClientSecret,
		AuthAudience:           params.AuthAudience,
		AuthURL:                params.AuthURL,
		Environment:            params.Environment,
		Slug:                   params.Slug,
		EsClientProvider:       params.ESClientProvider,
		Auth0ClientProvider:    params.Auth0ClientProvider,
		HTTPClientProvider:     params.HTTPClientProvider,
		Fetcher:                params.Fetcher,
		Enricher:               params.Enricher,
	}

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

// Auth0ClientProvider ...
type Auth0ClientProvider interface {
	ValidateToken(env string) (string, error)
}

// Sync starts fetch and enrich processes
func (m *Manager) Sync() error {
	lastActionCachePostfix := "-last-action-date-cache"
	// register disabled job as done
	doneJobs := make(map[string]bool)
	doneJobs["doneFetch"] = !m.Fetch
	doneJobs["doneEnrich"] = !m.Enrich
	fetchCh := m.fetch(lastActionCachePostfix)

	for doneJobs["doneFetch"] == false || doneJobs["doneEnrich"] == false {
		select {
		case err := <-fetchCh:
			if err == nil {
				doneJobs["doneFetch"] = true
			}
		case err := <-m.enrich(lastActionCachePostfix):
			if err == nil {
				doneJobs["doneEnrich"] = true
			}
			time.Sleep(5 * time.Second)
		}
	}
	return nil
}

func (m *Manager) fetch(lastActionCachePostfix string) <-chan error {
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
		err := m.EsClientProvider.Get(fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), query, val)

		now := time.Now().UTC()
		var lastFetch *time.Time

		if err == nil && len(val.Hits.Hits) > 0 {
			lastFetch = &val.Hits.Hits[0].Source.ChangedAt
		}

		from := timeLib.GetOldestDate(m.FromDate, lastFetch)
		fromStr := from.Format("2006-01-02T15:04:05")

		offset := 0
		for result == m.FetchSize {
			data := make([]elastic.BulkData, 0)
			bugs, lastChange, err := m.Fetcher.FetchAll(m.Endpoint, fromStr, strconv.Itoa(m.FetchSize), strconv.Itoa(offset), now)
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
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				updateChan := HitSource{ID: fetchID, ChangedAt: *lastChange}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})
				//set mapping and create index if not exists
				err := m.EsClientProvider.DelayOfCreateIndex(m.EsClientProvider.CreateIndex, m.Retries, m.Delay, fmt.Sprintf("%s-raw", m.ESIndex), BugzillaRestRawMapping)
				if err != nil {
					ch <- err
					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, data, m.Auth0ClientProvider, m.Environment)
					if err != nil {
						return
					}

					continue
				}
				// Insert raw data to elasticsearch
				esRes, err := m.EsClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err
					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, data, m.Auth0ClientProvider, m.Environment)
					return
				}

				failedData, err := util.HandleFailedData(data, esRes)
				if len(failedData) != 0 {
					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, failedData, m.Auth0ClientProvider, m.Environment)
				}
			}

		}
		ch <- nil
	}()

	return ch
}

func (m *Manager) enrich(lastActionCachePostfix string) <-chan error {
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
		err := m.EsClientProvider.Get(fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), query, val)

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
			topHits, err = m.Fetcher.Query(fmt.Sprintf("%s-raw", m.ESIndex), query)
			if err != nil {
				ch <- nil
				return
			}

			data := make([]elastic.BulkData, 0)
			for _, hit := range topHits.Hits.Hits {
				enrichedItem, err := m.Enricher.EnrichItem(hit.Source, time.Now().UTC())
				if err != nil {
					ch <- err
					return
				}
				data = append(data, elastic.BulkData{IndexName: m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem})
			}

			results = len(data)
			offset += results

			// setting mapping and create index if not exists
			if offset == 0 {
				_, err := m.EsClientProvider.CreateIndex(m.ESIndex, BugzillaRestEnrichMapping)
				if err != nil {
					ch <- err
					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, data, m.Auth0ClientProvider, m.Environment)
					return
				}
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*BugRestEnrich)
				updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.ChangedDate}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

				// Insert enriched data to elasticsearch
				esRes, err := m.EsClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err

					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, data, m.Auth0ClientProvider, m.Environment)
					return
				}

				failedData, err := util.HandleFailedData(data, esRes)
				if len(failedData) != 0 {
					err = util.HandleGapData(m.GapURL, m.HTTPClientProvider, data, m.Auth0ClientProvider, m.Environment)
				}
			}

		}

		ch <- nil
	}()

	return ch
}
