package bugzilla

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/build"

	"github.com/LF-Engineering/dev-analytics-libraries/slack"

	"github.com/LF-Engineering/dev-analytics-libraries/auth0"

	"github.com/LF-Engineering/da-ds/util"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
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
	FromDate               *time.Time
	HTTPTimeout            time.Duration
	Project                string
	FetchSize              int
	EnrichSize             int
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	AffAPI                 string
	ProjectSlug            string
	AffBaseURL             string
	ESCacheURL             string
	ESCacheUsername        string
	ESCachePassword        string
	AuthGrantType          string
	AuthClientID           string
	AuthClientSecret       string
	AuthAudience           string
	Auth0URL               string
	Environment            string
	Slug                   string
	WebHookURL             string

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
	Auth0            Auth0Client
}

// Param required for creating a new instance of Bugzilla manager
type Param struct {
	EndPoint               string
	FetcherBackendVersion  string
	EnricherBackendVersion string
	Fetch                  bool
	Enrich                 bool
	ESUrl                  string
	EsUser                 string
	EsPassword             string
	EsIndex                string
	FromDate               *time.Time
	Project                string
	FetchSize              int
	EnrichSize             int
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	AffAPI                 string
	ProjectSlug            string
	AffBaseURL             string
	ESCacheURL             string
	ESCacheUsername        string
	ESCachePassword        string
	AuthGrantType          string
	AuthClientID           string
	AuthClientSecret       string
	AuthAudience           string
	Auth0URL               string
	Environment            string
	Slug                   string
	WebHookURL             string
}

// NewManager initiates bugzilla manager instance
func NewManager(param Param) (*Manager, error) {

	mgr := &Manager{
		Endpoint:               param.EndPoint,
		FetcherBackendVersion:  param.FetcherBackendVersion,
		EnricherBackendVersion: param.EnricherBackendVersion,
		Fetch:                  param.Fetch,
		Enrich:                 param.Enrich,
		ESUrl:                  param.ESUrl,
		ESUsername:             param.EsUser,
		ESPassword:             param.EsPassword,
		ESIndex:                param.EsIndex,
		FromDate:               param.FromDate,
		HTTPTimeout:            60 * time.Second,
		Project:                param.Project,
		FetchSize:              param.FetchSize,
		EnrichSize:             param.EnrichSize,
		Retries:                param.Retries,
		Delay:                  param.Delay,
		GapURL:                 param.GapURL,
		AffAPI:                 param.AffAPI,
		ProjectSlug:            param.ProjectSlug,
		AffBaseURL:             param.AffBaseURL,
		ESCacheURL:             param.ESCacheURL,
		ESCacheUsername:        param.ESCacheUsername,
		ESCachePassword:        param.ESCachePassword,
		AuthGrantType:          param.AuthGrantType,
		AuthClientID:           param.AuthClientID,
		AuthClientSecret:       param.AuthClientSecret,
		AuthAudience:           param.AuthAudience,
		Auth0URL:               param.Auth0URL,
		Environment:            param.Environment,
		Slug:                   param.Slug,
		WebHookURL:             param.WebHookURL,
	}

	fetcher, enricher, esClientProvider, auth0Client, err := buildServices(mgr)
	if err != nil {
		return nil, err
	}

	mgr.fetcher = fetcher
	mgr.enricher = enricher
	mgr.esClientProvider = esClientProvider
	mgr.Auth0 = auth0Client

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

// Auth0Client ...
type Auth0Client interface {
	GetToken() (string, error)
}

// SlackProvider ...
type SlackProvider interface {
	SendText(text string) error
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

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, Auth0Client, error) {
	httpClientProvider := http.NewClientProvider(m.HTTPTimeout)
	params := &Params{
		Endpoint:       m.Endpoint,
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	esCacheClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESCacheURL,
		Username: m.ESCacheUsername,
		Password: m.ESCachePassword,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	slackProvider := slack.New(m.WebHookURL)
	// Initialize fetcher object to get data from dockerhub api
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)
	auth0Client, err := auth0.NewAuth0Client(m.ESCacheURL,
		m.ESUsername,
		m.ESCachePassword,
		m.Environment,
		m.AuthGrantType,
		m.AuthClientID,
		m.AuthClientSecret,
		m.AuthAudience,
		m.Auth0URL,
		httpClientProvider,
		esCacheClientProvider,
		&slackProvider,
		build.AppName)

	affiliationsClientProvider, err := libAffiliations.NewAffiliationsClient(m.AffBaseURL, m.Slug, httpClientProvider, esCacheClientProvider, auth0Client, &slackProvider)

	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, m.Project, affiliationsClientProvider)

	if err != nil {
		return nil, nil, nil, nil, err
	}

	return fetcher, enricher, esClientProvider, auth0Client, err
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

		for result == m.FetchSize {
			data := make([]elastic.BulkData, 0)
			bugs, err := fetcher.FetchItem(*from, m.FetchSize, now)
			if err != nil {
				ch <- err
				return
			}

			result = len(bugs)
			if result != 0 {
				from = &bugs[len(bugs)-1].ChangedAt
			}

			for _, bug := range bugs {
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: bug.UUID, Data: bug})
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*BugRaw)
				updateChan := HitSource{ID: fetchID, ChangedAt: cacheDoc.ChangedAt}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

				err := m.esClientProvider.DelayOfCreateIndex(m.esClientProvider.CreateIndex, m.Retries, m.Delay, fmt.Sprintf("%s-raw", m.ESIndex), BugzillaRawMapping)
				if err != nil {
					ch <- err

					err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, data, m.Auth0, m.Environment)
					if err != nil {
						return
					}

					continue
				}

				esRes, err := m.esClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err
					err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, data, m.Auth0, m.Environment)
					return
				}

				failedData, err := util.HandleFailedData(data, esRes)
				if len(failedData) != 0 {
					err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, failedData, m.Auth0, m.Environment)
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
					"changed_at": map[string]string{
						"order": "asc",
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
				"changed_at": map[string]interface{}{
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

			data := make([]elastic.BulkData, 0)
			for _, hit := range topHits.Hits.Hits {
				enrichedItem, err := enricher.EnrichItem(hit.Source, time.Now().UTC())
				if err != nil {
					ch <- err
					return
				}
				data = append(data, elastic.BulkData{IndexName: m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem})
			}

			results = len(data)
			offset += results

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*BugEnrich)
				updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.ChangedDate}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

				// setting mapping and create index if not exists
				if offset == 0 {
					_, err := m.esClientProvider.CreateIndex(m.ESIndex, BugzillaEnrichMapping)
					if err != nil {
						ch <- err
						err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, data, m.Auth0, m.Environment)
						return
					}
				}

				// Insert enriched data to elasticsearch
				esRes, err := m.esClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err
					err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, data, m.Auth0, m.Environment)
					return
				}

				failedData, err := util.HandleFailedData(data, esRes)
				if len(failedData) != 0 {
					err = util.HandleGapData(m.GapURL, m.fetcher.HTTPClientProvider, failedData, m.Auth0, m.Environment)
				}
			}

		}

		ch <- nil
	}()

	return ch
}
