package googlegroups

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/LF-Engineering/da-ds/build"
	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/slack"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// AuthClientProvider interacts with auth0 server
type AuthClientProvider interface {
	GetToken(env string) (string, error)
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

// Manager describes google groups manager
type Manager struct {
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
	Auth0URL               string
	Environment            string
	WebHookURL             string

	esClientProvider *elastic.ClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
}

// NewManager initiates google groups manager instance
func NewManager(slug, groupName, shConnStr, fetcherBackendVersion, enricherBackendVersion string, fetch bool, enrich bool, eSUrl string, esUser string, esPassword string, esIndex string, fromDate *time.Time, project string, fetchSize int, enrichSize int, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, authURL, env string) (*Manager, error) {
	mng := &Manager{
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
		Auth0URL:               authURL,
		Environment:            env,
		esClientProvider:       nil,
		enricher:               nil,
		fetcher:                nil,
	}

	fetcher, enricher, esClientProvider, err := buildServices(mng)
	if err != nil {
		return nil, err
	}

	mng.fetcher = fetcher
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

	fetchCh := m.fetch(m.fetcher, lastActionCachePostfix)

	var err error
	if status["doneFetch"] == false {
		err = <-fetchCh
		if err == nil {
			status["doneFetch"] = true
		}
		time.Sleep(5 * time.Second)
	}

	if status["doneEnrich"] == false {
		err = <-m.enrich(m.enricher, lastActionCachePostfix)
		if err == nil {
			status["doneEnrich"] = true
		}
		time.Sleep(5 * time.Second)
	}

	return err
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

		val := &TopHits{}
		err := m.esClientProvider.Get(fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), query, val)

		now := time.Now().UTC()
		var lastFetch *time.Time

		if err == nil && len(val.Hits.Hits) > 0 {
			lastFetch = &val.Hits.Hits[0].Source.ChangedAt
		}

		fromDate := m.FromDate
		if fromDate == nil {
			fromDate = &DefaultDateTime
		}

		from := timeLib.GetOldestDate(fromDate, lastFetch)

		data := make([]elastic.BulkData, 0)
		raw, err := fetcher.Fetch(from, &now)
		if err != nil {
			ch <- err
			return
		}

		result := len(raw)
		if result != 0 {
			from = &raw[len(raw)-1].ChangedAt
		}

		for _, message := range raw {
			data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: message.UUID, Data: message})
		}

		// set mapping and create index if not exists
		_, err = m.esClientProvider.CreateIndex(fmt.Sprintf("%s-raw", m.ESIndex), GoogleGroupRawMapping)
		if err != nil {
			ch <- err
			return
		}

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*RawMessage)
			updateChan := HitSource{ID: fetchID, ChangedAt: cacheDoc.ChangedAt}
			data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

			// Insert raw data to elasticsearch
			sizeOfData := len(data)

			limit := 1000
			if m.EnrichSize <= 1000 {
				limit = m.EnrichSize
			}

			lastIndex := 0
			remainingItemsLength := 0
			log.Println("LEN RAW DATA : ", len(data))
			// rate limit items to push to es to avoid the 413 error
			if len(data) > m.EnrichSize {
				for lastIndex < sizeOfData {
					if lastIndex == 0 && limit <= len(data) {
						_, err = m.esClientProvider.BulkInsert(data[:limit])
						if err != nil {
							ch <- err
							return
						}
						lastIndex = limit
						continue
					}
					if lastIndex > 0 && limit <= len(data[lastIndex:]) && remainingItemsLength == 0 {
						_, err = m.esClientProvider.BulkInsert(data[lastIndex : lastIndex+limit])
						if err != nil {
							ch <- err
							return
						}

						if lastIndex+limit < len(data[lastIndex:]) {
							lastIndex += limit
						} else {
							remainingItemsLength = len(data[lastIndex:])
						}
					} else {
						// handle cases where remaining messages are less than the limit
						_, err = m.esClientProvider.BulkInsert(data[lastIndex:])
						if err != nil {
							ch <- err
							return
						}
						// invalidate loop
						lastIndex = sizeOfData + 1
					}
				}

			}

			// handle data for small docs
			// es bulk upload limit is 1000
			if m.EnrichSize >= sizeOfData {
				if sizeOfData <= 1000 {
					_, err = m.esClientProvider.BulkInsert(data)
					if err != nil {
						ch <- err
						return
					}
				}
			}
			log.Println("DONE WITH RAW ENRICHMENT")
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

		var lastEnrich time.Time
		if err == nil && len(val.Hits.Hits) > 0 {
			lastEnrich = val.Hits.Hits[0].Source.ChangedAt
		}

		from := timeLib.GetOldestDate(m.FromDate, &lastEnrich)
		log.Println("From: ", from)

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
			bites, err := m.fetcher.ElasticSearchProvider.Search(fmt.Sprintf("%s-raw", m.ESIndex), query)
			if err != nil {
				ch <- nil
				return
			}
			var topHits *RawHits
			err = json.Unmarshal(bites, &topHits)
			if err != nil {
				ch <- nil
				return
			}

			data := make([]elastic.BulkData, 0)
			for _, hit := range topHits.Hits.Hits {
				if lastEnrich.Before(hit.Source.ChangedAt) {
					enrichedItem, err := enricher.EnrichMessage(&hit.Source, time.Now().UTC())
					if err != nil {
						ch <- err
						return
					}
					data = append(data, elastic.BulkData{IndexName: m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem})
				}
			}

			log.Println("LEN ENRICH DATA : ", len(data))
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
				updateChan := HitSource{ID: enrichID, ChangedAt: cacheDoc.ChangedAt}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: enrichID, Data: updateChan})

				// Insert enriched data to elasticsearch
				_, err = m.esClientProvider.BulkInsert(data)
				if err != nil {
					ch <- err
					return
				}
			}

		}
		log.Println("DONE WITH RICH ENRICHMENT")

		ch <- nil
	}()

	return ch
}

func buildServices(m *Manager) (*Fetcher, *Enricher, *elastic.ClientProvider, error) {
	httpClientProvider := http.NewClientProvider(m.HTTPTimeout)

	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	esCacheClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESCacheURL,
		Username: m.ESCacheUsername,
		Password: m.ESCachePassword,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	slackProvider := slack.New(m.WebHookURL)

	appNameVersion := fmt.Sprintf("%s-%v", build.AppName, strconv.FormatInt(time.Now().Unix(), 10))
	auth0Client, err := auth0.NewAuth0Client(
		m.Environment,
		m.AuthGrantType,
		m.AuthClientID,
		m.AuthClientSecret,
		m.AuthAudience,
		m.Auth0URL,
		httpClientProvider,
		esCacheClientProvider,
		&slackProvider,
		appNameVersion)

	affiliationsClientProvider, err := affiliation.NewAffiliationsClient(m.AffBaseURL, m.Slug, httpClientProvider, esCacheClientProvider, auth0Client, &slackProvider)
	if err != nil {
		return nil, nil, nil, err
	}

	//Initialize fetcher object to fetch raw data
	fetcher := NewFetcher(m.GroupName, m.Slug, m.Project, httpClientProvider, esClientProvider)

	//Initialize enrich object to enrich raw data
	enricher := NewEnricher(esClientProvider, affiliationsClientProvider)

	return fetcher, enricher, esClientProvider, err
}
