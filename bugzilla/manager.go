package bugzilla

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/affiliation"
	db "github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/da-ds/utils"
	"time"
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
	fromDate *time.Time,
	httpTimeout time.Duration,
	project string,
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
		FromDate:               fromDate,
		HTTPTimeout:            httpTimeout,
		Project:                project,
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

	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}

	cachePostfix := "-last-action-date-cache"

	if m.Fetch {
		fetchId := "fetch"

		query := map[string]interface{}{
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"id": map[string]string{
						"value": fetchId},
				},
			},
		}
		limit := 25
		result := 25

		val := &TopHits{}
		err = esClientProvider.Get(m.ESIndex+cachePostfix, query, val)

		now := time.Now()
		var from time.Time

		if err != nil || len(val.Hits.Hits) < 1 {
			// Todo : update date to 1970
			from, err = time.Parse("2006-01-02 15:04:05", "2020-12-04 10:54:21")
			if err != nil {
				return err
			}

		} else {
			from = val.Hits.Hits[0].Source.ChangedAt

			if m.FromDate != nil && m.FromDate.Before(from) {
				from = *m.FromDate
			}
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
		ind := m.ESIndex + cachePostfix
		_ = fetcher.HandleMapping(fmt.Sprintf(ind))

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*BugRaw)
			updateChan := HitSource{Id: fetchId, ChangedAt: cacheDoc.ChangedAt}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, cachePostfix), ID: fetchId, Data: updateChan})

			// Insert raw data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}

	}

	if m.Enrich {

		enrichId := "enrich"

		query := map[string]interface{}{
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"id": map[string]string{
						"value": enrichId},
				},
			},
		}

		val := &TopHits{}
		err = esClientProvider.Get(m.ESIndex+cachePostfix, query, val)

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

		var tophits *RawHits
		{
		}
		var from time.Time
		if err == nil && len(val.Hits.Hits) > 0 {
			from = val.Hits.Hits[0].Source.ChangedAt
		}

		if (err != nil || len(val.Hits.Hits) < 1) && (m.FromDate == nil || (*m.FromDate).IsZero()) {
			tophits, err = m.GetFetchedData(m.ESIndex+"-raw", query)
			if err != nil {
				return err
			}
		} else if m.FromDate != nil {
			searchVal := m.FromDate
			if !from.IsZero() {
				if from.Before(*searchVal) {
					searchVal = &from
				}
			}
			conditions := map[string]interface{}{
				"range": map[string]interface{}{
					"metadata__updated_on": map[string]interface{}{
						"gte": (m.FromDate).Format(time.RFC3339),
					},
				},
			}
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = conditions

			tophits, err = m.GetFetchedData(m.ESIndex+"-raw", query)
			if err != nil {
				return err
			}

		} else {
			var searchVal time.Time

			if m.FromDate == nil {
				searchVal = from
			}else {
				searchVal = *m.FromDate
			}
			conditions := map[string]interface{}{
				"range": map[string]interface{}{
					"metadata__updated_on": map[string]interface{}{
						"gte": (searchVal).Format(time.RFC3339),
					},
				},
			}
			query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = conditions

			tophits, err = m.GetFetchedData(m.ESIndex+"-raw", query)
			if err != nil {
				return err
			}

		}

		data := make([]*utils.BulkData, 0)
		for _, hit := range tophits.Hits.Hits{
			enrichedItem, err := enricher.EnrichItem(hit.Source, time.Now())
			if err != nil {
				return err
			}
			data = append(data, &utils.BulkData{IndexName:  m.ESIndex, ID: enrichedItem.UUID, Data: enrichedItem })
		}

		// set mapping and create index if not exists
		ind := m.ESIndex + cachePostfix
		_ = fetcher.HandleMapping(fmt.Sprintf(ind))

		if len(data) > 0 {
			// Update changed at in elastic cache index
			cacheDoc, _ := data[len(data)-1].Data.(*EnrichedItem)
			fmt.Println("===")
			fmt.Println(cacheDoc.ChangedAt)

			fmt.Println(enrichId)
			updateChan := HitSource{Id: enrichId, ChangedAt: cacheDoc.MetadataEnrichedOn}
			data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s%s", m.ESIndex, cachePostfix), ID: enrichId, Data: updateChan})

			// Insert enriched data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}

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

	x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	dataBase, err := db.NewConnector("mysql", x)
	if err != nil {
		fmt.Println("jjjjjjj")
		fmt.Println(err)
	}
	identityProvider := affiliation.NewIdentityProvider(dataBase)
	// Initialize enrich object to enrich raw data
	enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, m.Project)

	return fetcher, enricher, esClientProvider, err
}



func (m *Manager) GetFetchedData(index string, query map[string]interface{} ) (*RawHits, error) {

	_, _, esClientProvider, err := buildServices(m)
	if err != nil {
		return nil, err
	}
	var hits RawHits

	err = esClientProvider.Get(index, query, &hits )
	if err != nil {
		return nil, err
	}
	return &hits, err
}
