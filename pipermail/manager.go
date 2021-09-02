package pipermail

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/build"

	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/slack"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"

	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"

	"github.com/LF-Engineering/dev-analytics-libraries/http"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
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

// Manager describes piper mail manager
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
	Auth0URL               string
	Environment            string
	WebHookURL             string
	MaxWorkers             int
	NumberOfRawMessages    int

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
	workerPool       *workerPool
}

// workerPool ...
type workerPool struct {
	MaxWorker   int
	queuedTaskC chan func()
}

// result worker pool result struct
type result struct {
	id           int
	enrichedItem *EnrichedMessage
}

// NewManager initiates piper mail manager instance
func NewManager(endPoint, slug, shConnStr, fetcherBackendVersion, enricherBackendVersion string, fetch bool, enrich bool, eSUrl string, esUser string, esPassword string, esIndex string, fromDate *time.Time, project string, fetchSize int, enrichSize int, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, auth0URL, env, webHookURL string) (*Manager, error) {
	mng := &Manager{
		Endpoint:               endPoint,
		Slug:                   slug,
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
		AffBaseURL:             affBaseURL,
		ESCacheURL:             esCacheURL,
		ESCacheUsername:        esCacheUsername,
		ESCachePassword:        esCachePassword,
		AuthGrantType:          authGrantType,
		AuthClientID:           authClientID,
		AuthClientSecret:       authClientSecret,
		AuthAudience:           authAudience,
		Auth0URL:               auth0URL,
		Environment:            env,
		esClientProvider:       nil,
		fetcher:                nil,
		enricher:               nil,
		WebHookURL:             webHookURL,
		MaxWorkers:             1000,
	}

	fetcher, enricher, esClientProvider, err := buildServices(mng)
	if err != nil {
		return nil, err
	}

	groupName, err := getGroupName(endPoint)
	if err != nil {
		return nil, err
	}

	mng.fetcher = fetcher
	mng.enricher = enricher
	mng.esClientProvider = esClientProvider
	mng.GroupName = groupName
	mng.workerPool = &workerPool{
		MaxWorker:   MaxConcurrentRequests,
		queuedTaskC: make(chan func()),
	}

	return mng, nil
}

// Sync runs piper mail fetch and enrich according to passed parameters
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

	return nil
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
		raw, err := fetcher.FetchItem(m.Slug, m.GroupName, m.Endpoint, *from, m.FetchSize, now)
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
		_, err = m.esClientProvider.CreateIndex(fmt.Sprintf("%s-raw", m.ESIndex), PipermailRawMapping)
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
			m.NumberOfRawMessages = sizeOfData
			log.Println("DONE WITH RAW ENRICHMENT")
		}
		ch <- nil
	}()

	return ch
}

func (m *Manager) enrich(enricher *Enricher, lastActionCachePostfix string) <-chan error {
	ch := make(chan error)
	//m.run()
	resultC := make(chan result, 0)
	numJobs := m.NumberOfRawMessages
	jobs := make(chan *RawMessage, m.NumberOfRawMessages)

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
			for w := 1; w <= m.MaxWorkers; w++ {
				go m.enrichWorker(w, jobs, resultC)
			}

			for _, hit := range topHits.Hits.Hits {
				nHitSource := hit.Source
				if lastEnrich.Before(hit.Source.ChangedAt) {
					jobs <- &nHitSource
				}
			}
			close(jobs)

			for a := 1; a <= numJobs; a++ {
				res := <-resultC
				log.Printf("[main] task %d has been finished with result message id %+v", res.id, res.enrichedItem.MessageID)
				data = append(data, elastic.BulkData{IndexName: m.ESIndex, ID: res.enrichedItem.UUID, Data: res.enrichedItem})
			}
			log.Println("LEN ENRICH DATA : ", len(data))
			results = len(data)

			// setting mapping and create index if not exists
			if offset == 0 {
				_, err := m.esClientProvider.CreateIndex(m.ESIndex, PiperRichMapping)
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
			results = len(data)
			offset += results
		}
		log.Println("DONE WITH RICH ENRICHMENT")
		ch <- nil
	}()

	return ch
}

// enrichWorker spins up workers to enrich messages
func (m *Manager) enrichWorker(workerID int, jobs <-chan *RawMessage, results chan<- result) {
	for j := range jobs {
		log.Printf("worker %+v started job %+v", workerID, j.UUID)
		enrichedItem, err := m.enricher.EnrichMessage(j, time.Now().UTC())
		// quit app if error isn't nil
		if err != nil {
			os.Exit(1)
		}
		time.Sleep(time.Second)
		log.Printf("worker %+v finished job %+v", workerID, j.UUID)
		results <- result{id: workerID, enrichedItem: enrichedItem}
	}
}

// AddTask adds task to worker pool
func (m *Manager) AddTask(task func()) {
	m.workerPool.queuedTaskC <- task
}

// run starts the tasks in the worker pool queue
func (m *Manager) run() {
	for i := 0; i < m.workerPool.MaxWorker; i++ {
		wID := i + 1
		//log.Printf("[workerPool] worker %d spawned", wID)
		go func(workerID int) {
			for task := range m.workerPool.queuedTaskC {
				log.Printf("[workerPool] worker %d is processing task", wID)
				task()
				log.Printf("[workerPool] worker %d has finished processing task", wID)
			}
		}(wID)
	}
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

	esCacheClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      m.ESCacheURL,
		Username: m.ESCacheUsername,
		Password: m.ESCachePassword,
	})

	// Initialize fetcher object to get data from piper mail archive link
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)
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

	affiliationsClientProvider, err := libAffiliations.NewAffiliationsClient(m.AffBaseURL, m.Slug, httpClientProvider, esCacheClientProvider, auth0Client, &slackProvider)
	if err != nil {
		return nil, nil, nil, err
	}

	//Initialize enrich object to enrich raw data
	enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider, affiliationsClientProvider)

	return fetcher, enricher, esClientProvider, err
}

// getGroupName extracts a pipermail group name from the given mailing list url
func getGroupName(targetURL string) (string, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return "", err
	}

	path := u.Path
	if strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	if strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	path = strings.ReplaceAll(path, "/", "-")
	return path, nil
}
