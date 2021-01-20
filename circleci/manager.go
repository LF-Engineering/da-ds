package circleci

import (
	b64 "encoding/base64"
	"fmt"
	"time"

	"encoding/json"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
)

// ESClientProvider used in connecting to ES server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []elastic.BulkData) ([]byte, error)
	DelayOfCreateIndex(ex func(str string, b []byte) ([]byte, error), uin uint, du time.Duration, index string, data []byte) error
}

// Manager describes circleci manager
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
	ProjectSlug            string
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

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	//enricher         *Enricher

	Retries uint
	Delay   time.Duration
	GapURL  string
	Token   string
}

// Param required for creating a new instance of CircleCI manager
type Param struct {
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
	Project                string
	ProjectSlug            string
	FetchSize              int
	EnrichSize             int
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	Token                  string
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
}

// NewManager initiates bugzilla manager instance
func NewManager(param Param) (*Manager, error) {

	mgr := &Manager{
		Endpoint:               param.EndPoint,
		SHConnString:           param.ShConnStr,
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
		ProjectSlug:            param.ProjectSlug,
		FetchSize:              param.FetchSize,
		EnrichSize:             param.EnrichSize,
		Retries:                param.Retries,
		Delay:                  param.Delay,
		GapURL:                 param.GapURL,
		Token:                  param.Token,
		AffBaseURL:             param.AffBaseURL,
		ESCacheURL:             param.ESCacheURL,
		ESCacheUsername:        param.ESCacheUsername,
		ESCachePassword:        param.ESCachePassword,
		AuthGrantType:          param.AuthGrantType,
		AuthClientID:           param.AuthClientID,
		AuthClientSecret:       param.AuthClientSecret,
		AuthAudience:           param.AuthAudience,
		AuthURL:                param.AuthURL,
		Environment:            param.Environment,
	}

	fetcher, esClientProvider, err := buildServices(mgr)
	if err != nil {
		return nil, err
	}

	mgr.fetcher = fetcher
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
			// case err := <-m.enrich(m.enricher, lastActionCachePostfix):
			// 	if err == nil {
			// 		doneJobs["doneEnrich"] = true
			// 	}
			//time.Sleep(5 * time.Second)
		}
	}
	return nil
}

func buildServices(m *Manager) (*Fetcher, ESClientProvider, error) {
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
		return nil, nil, err
	}

	affiliationsClientProvider, err := libAffiliations.NewAffiliationsClient(m.AffBaseURL, m.ProjectSlug, m.ESCacheURL, m.ESCacheUsername, m.ESCachePassword, m.Environment, m.AuthGrantType, m.AuthClientID, m.AuthClientSecret, m.AuthAudience, m.AuthURL)
	if err != nil {
		return nil, nil, err
	}

	// Initialize fetcher object to get data from bugzilla rest api
	cache := make(map[string]libAffiliations.AffIdentity)
	userCache := make(map[string]string)
	fetcher := NewFetcher(*params, httpClientProvider, esClientProvider, affiliationsClientProvider, cache, userCache)

	return fetcher, esClientProvider, err
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
		fromStr := from.Format("2006-01-02T15:04:05")

		offset := 0
		for result == m.FetchSize {
			data := make([]elastic.BulkData, 0)
			circleci, _, err := fetcher.FetchAll(m.Endpoint, m.Project, m.Token, lastFetch, now, fromStr)
			if err != nil {
				ch <- err
				return
			}

			result = len(circleci)
			offset += result
			if result != 0 {
				from = &circleci[len(circleci)-1].WorkflowStoppedAt
			}

			for _, payload := range circleci {
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s", m.ESIndex), ID: payload.UUID, Data: payload})
			}

			if len(data) > 0 {
				// Update changed at in elastic cache index
				updateChan := HitSource{ID: fetchID, ChangedAt: circleci[len(circleci)-1].WorkflowStoppedAt}
				data = append(data, elastic.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, lastActionCachePostfix), ID: fetchID, Data: updateChan})

				//set mapping and create index if not exists
				err := m.esClientProvider.DelayOfCreateIndex(m.esClientProvider.CreateIndex, m.Retries, m.Delay, fmt.Sprintf("%s", m.ESIndex), CircleCIRawMapping)
				if err != nil {
					ch <- err

					byteData, err := json.Marshal(data)
					if err != nil {
						ch <- err
						return
					}
					dataEnc := b64.StdEncoding.EncodeToString(byteData)
					gapBody := map[string]string{"payload": dataEnc}
					bData, err := json.Marshal(gapBody)
					if err != nil {
						ch <- err
						return
					}

					c, e, err := m.fetcher.HTTPClientProvider.Request(m.GapURL, "POST", nil, bData, nil)
					if err != nil {
						ch <- err
						return
					}
					fmt.Println(c, string(e))
					continue
				}

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
