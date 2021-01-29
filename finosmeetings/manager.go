package finosmeetings

import (
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	db "github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/da-ds/utils"
)

// Manager describes dockerhub manager
type Manager struct {
	FetcherBackendVersion  string
	EnricherBackendVersion string
	SHConnString           string
	EnrichOnly             bool
	Enrich                 bool
	ESURL                  string
	ESUsername             string
	ESPassword             string
	ESRawIndex             string
	ESEnrichIndex          string
	ESRichIndex            string
	URI                    string
	HTTPTimeout            time.Duration
	FromDate               *time.Time
	esClientProvider       ESClientProvider
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
	ID            string    `json:"id"`
	DateISOFormat time.Time `json:"date_iso_format"`
}

// NewManager initiates finosmeetings manager instance
func NewManager(
	shConnStr string,
	fetcherBackendVersion string,
	enricherBackendVersion string,
	enrichOnly bool,
	enrich bool,
	esURL string,
	esRawIndex string,
	esRichIndex string,
	uri string,
) *Manager {
	mng := &Manager{
		SHConnString:           shConnStr,
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		EnrichOnly:             enrichOnly,
		Enrich:                 enrich,
		ESURL:                  esURL,
		URI:                    uri,
		ESRawIndex:             esRawIndex,
		ESRichIndex:            esRichIndex,
	}

	return mng
}

// Sync runs dockerhub fetch and enrich according to passed parameters
func (m *Manager) Sync() error {

	var rawAry, rawForEnrichAry []*FinosmeetingsRaw

	// fetcher, enricher, esClientProvider, err := buildServices(m)
	fetcher, enricher, esClientProvider, err := buildServices(m)
	m.esClientProvider = esClientProvider

	if err != nil {
		return err
	}

	// get the last enriched meeting date. This is used to filter out
	// which row in the csv data should be processed

	fmt.Println("This is the latest date", m.getLatestMeetingDate())
	lastMeetingDate := m.getLatestMeetingDate()

	if !m.EnrichOnly {
		// set mapping and create index if not exists
		_ = fetcher.HandleMapping(fmt.Sprintf("%s", m.ESRawIndex))

		data := make([]*utils.BulkData, 0)

		// fetch data

		rawAry, err = fetcher.FetchItem(m.URI, time.Now())
		if err != nil {
			return fmt.Errorf("could not fetch data from URI: %s", m.URI)
		}

		fmt.Println(rawAry)

		for _, thisRaw := range rawAry {
			if lastMeetingDate.Before(thisRaw.Data.DateIsoFormat) {
				data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s", m.ESRawIndex), ID: thisRaw.UUID, Data: thisRaw})
				rawForEnrichAry = append(rawForEnrichAry, thisRaw)
			}

		}

		if len(data) > 0 {
			// Insert raw data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}
		}
	}

	if m.Enrich || m.EnrichOnly {
		data := make([]*utils.BulkData, 0)

		for _, thisRaw := range rawForEnrichAry {

			//Enrich data for single repo
			enriched, err := enricher.EnrichItem(*thisRaw, time.Now())
			if err != nil {
				return fmt.Errorf("could not enrich data from repository: %s-%s")
			}

			fmt.Println(enriched)
			data = append(data, &utils.BulkData{IndexName: m.ESRichIndex, ID: enriched.UUID, Data: enriched})

			// var fromDate *time.Time
			// var lastDate time.Time
			// if m.FromDate == nil || (*m.FromDate).IsZero() {
			// 	lastDate, err = fetcher.GetLastDate(repo, time.Now())
			// 	if err != nil {
			// 		log.Println("[GetLastDate] could not get last date")
			// 	}
			// } else {
			// 	fromDate = m.FromDate
			// }

			// esData, err := enricher.GetFetchedDataItem(m.ESEnrichIndex, fromDate, &lastDate, m.NoIncremental)
			// if err != nil {
			// 	return err
			// }

			// if len(esData.Hits.Hits) > 0 {
			// 	// Enrich data for single repo
			// 	enriched, err := enricher.EnrichItem(*esData.Hits.Hits[0].Source, repo.Project, time.Now())
			// 	if err != nil {
			// 		return fmt.Errorf("could not enrich data from repository: %s-%s", repo.Owner, repo.Repository)
			// 	}
			// 	data = append(data, &utils.BulkData{IndexName: repo.ESIndex, ID: enriched.UUID, Data: enriched})
			// 	_ = enricher.HandleMapping(repo.ESIndex)

			// }
		}

		fmt.Println("THIS IS DATA")
		fmt.Println(data)
		if len(data) > 0 {
			// Create index if not already existing
			_ = enricher.HandleMapping(m.ESRichIndex)

			// Insert enriched data to elasticsearch
			_, err = esClientProvider.BulkInsert(data)
			if err != nil {
				return err
			}

			// 	// Add/Update latest document in each origin
			// 	for _, repo := range data {
			// 		repo.ID = fmt.Sprintf("%s_%s", repo.Data.(*RepositoryEnrich).ID, repo.Data.(*RepositoryEnrich).RepositoryType)
			// 		repo.Data.(*RepositoryEnrich).IsDockerImage = 1
			// 		repo.Data.(*RepositoryEnrich).IsEvent = 0

			// 		data = append(data, repo)
			// 	}

			// 	// Insert enriched data to elasticsearch
			// 	_, err = esClientProvider.BulkInsert(data)
			// 	if err != nil {
			// 		return err
			// 	}

		}
	}

	return nil
}

func (m *Manager) getLatestMeetingDate() time.Time {

	query := map[string]interface{}{
		"size": 1,
		"sort": map[string]interface{}{
			"date_iso_format": map[string]interface{}{
				"order": "desc",
			},
		},
	}

	val := &TopHits{}
	fmt.Println(m.ESRichIndex, query, val)
	err := m.esClientProvider.Get(m.ESRichIndex, query, val)

	var lastMeetingDate time.Time

	if err == nil && len(val.Hits.Hits) > 0 {
		lastMeetingDate = val.Hits.Hits[0].Source.DateISOFormat
	}

	fmt.Println(lastMeetingDate)

	return lastMeetingDate

}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := utils.NewHTTPClientProvider(m.HTTPTimeout)
	params := &Params{
		BackendVersion: m.FetcherBackendVersion,
	}
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      m.ESURL,
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
	//enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, m.Project)
	enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, esClientProvider)

	return fetcher, enricher, esClientProvider, err
}
