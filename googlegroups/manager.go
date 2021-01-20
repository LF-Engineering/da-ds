package googlegroups

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

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

// Manager describes google groups manager
type Manager struct {
	Endpoint               string
	Slug                   string
	GroupName              string
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

	esClientProvider *elastic.ClientProvider
	enricher         *Enricher
}

// NewManager initiates google groups manager instance
func NewManager(endPoint, slug, groupName, shConnStr, fetcherBackendVersion, enricherBackendVersion string, fetch bool, enrich bool, eSUrl string, esUser string, esPassword string, esIndex string, fromDate *time.Time, project string, fetchSize int, enrichSize int, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, authURL, env string) (*Manager, error) {
	mng := &Manager{
		Endpoint:               endPoint,
		Slug:                   slug,
		GroupName:              groupName,
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
		HTTPTimeout:            time.Minute,
		Project:                project,
		FetchSize:              fetchSize,
		EnrichSize:             enrichSize,
		AffBaseURL:             affBaseURL,
		ESCacheURL:             esCacheURL,
		ESCacheUsername:        esCacheUsername,
		ESCachePassword:        esCachePassword,
		AuthGrantType:          authGrantType,
		AuthClientID:           authClientID,
		AuthClientSecret:       authClientSecret,
		AuthAudience:           authAudience,
		AuthURL:                authURL,
		Environment:            env,
		esClientProvider:       nil,
		enricher:               nil,
	}

	enricher, esClientProvider, err := buildServices(mng)
	if err != nil {
		return nil, err
	}

	mng.enricher = enricher
	mng.esClientProvider = esClientProvider

	return mng, nil
}

// Sync runs google groups fetch and enrich according to passed parameters
func (m *Manager) Sync() error {
	lastActionCachePostfix := "-last-action-date-cache"

	status := make(map[string]bool)
	status["doneFetch"] = !m.Fetch
	status["doneEnrich"] = !m.Enrich

	for status["doneFetch"] == false || status["doneEnrich"] == false {
		select {
		case err := <-m.enrich(m.enricher, lastActionCachePostfix):
			if err == nil {
				status["doneEnrich"] = true
			}
			time.Sleep(5 * time.Second)
		}
	}

	return nil
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
			data := make([]elastic.BulkData, 0)
			path := ""
			enrichedMessages, err := enricher.EnrichMessage(path, time.Now().UTC())
			if err != nil {
				ch <- err
				return
			}

			for _, enrichedMessage := range enrichedMessages {
				data = append(data, elastic.BulkData{IndexName: m.ESIndex, ID: enrichedMessage.UUID, Data: enrichedMessage})
			}

			results = len(data)
			offset += results

			// setting mapping and create index if not exists
			if offset == 0 {
				_, err := m.esClientProvider.CreateIndex(m.ESIndex, GoogleGroupRichMapping)
				if err != nil {
					ch <- err
					return
				}
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				cacheDoc, _ := data[len(data)-1].Data.(*EnrichedMessage)
				updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.ChangedDate}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

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

func buildServices(m *Manager) (*Enricher, *elastic.ClientProvider, error) {
	//httpClientProvider := http.NewClientProvider(m.HTTPTimeout)

	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, err
	}

	affiliationsClientProvider, err := affiliation.NewAffiliationsClient(m.AffBaseURL, m.Slug, m.ESCacheURL, m.ESCacheUsername, m.ESCachePassword, m.Environment, m.AuthGrantType, m.AuthClientID, m.AuthClientSecret, m.AuthAudience, m.AuthURL)
	if err != nil {
		return nil, nil, err
	}

	//Initialize enrich object to enrich raw data
	enricher := NewEnricher(esClientProvider, affiliationsClientProvider)

	return enricher, esClientProvider, err
}
