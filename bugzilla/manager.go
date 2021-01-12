package bugzilla

import (
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	"github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
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
	GapURL                 string
	AffAPI                 string
	ProjectSlug            string

	esClientProvider ESClientProvider
	fetcher          *Fetcher
	enricher         *Enricher
}

// Param required for creating a new instance of Bugzilla manager
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
	FetchSize              int
	EnrichSize             int
	Retries                uint
	Delay                  time.Duration
	GapURL                 string
	AffAPI                 string
	ProjectSlug            string
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
		FetchSize:              param.FetchSize,
		EnrichSize:             param.EnrichSize,
		Retries:                param.Retries,
		Delay:                  param.Delay,
		GapURL:                 param.GapURL,
		AffAPI:                 param.AffAPI,
		ProjectSlug:            param.ProjectSlug,
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

					_, _, err = m.fetcher.HTTPClientProvider.Request(m.GapURL, "POST", nil, bData, nil)
					if err != nil {
						ch <- err
						return
					}
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

// CreateNewIdentity ...
//func (m *Manager) CreateNewIdentity(data *Person, source string) {
//	// add new identity to affiliation DB
//	var identity affiliation.Identity
//	if data == nil {
//		log.Print("Err : identity data is empty")
//		return
//	}
//	if data.Name != "" {
//		identity.Name.String = data.Name
//		identity.Name.Valid = true
//	}
//	if data.Username != "" {
//		identity.Username.String = data.Username
//		identity.Username.Valid = true
//	}
//	// if username exist and there is no name assume name = username
//	if data.Username != "" && data.Name == "" {
//		identity.Name.String = data.Username
//		identity.Name.Valid = true
//	}
//
//	authProvider, err := auth.NewAuth0Client(m.ESUrl, m.ESUsername, m.ESPassword, "",
//		"", "", "", "", "")
//	if err != nil {
//		log.Printf("Err : %s", err.Error())
//		return
//	}
//
//	token := authProvider.GenerateToken()
//	header := make(map[string]string)
//	header["Authorization"] = token
//
//	bData, err := json.Marshal(identity)
//	if err != nil {
//		log.Printf("Err : %s", err.Error())
//		return
//	}
//
//	createIdentityAPI := fmt.Sprintf("%s/v1/affiliation/%s/add_identity/%s", m.AffAPI, m.ProjectSlug, source)
//	_, _, err = m.fetcher.HTTPClientProvider.Request(createIdentityAPI, "Post", header, bData, nil)
//	if err != nil {
//		log.Printf("Err : %s", err.Error())
//		return
//	}
//	return
//
//}

// GetAffiliationIdentity gets author SH identity data
//func (m *Manager) GetAffiliationIdentity(key string, value string) (*AffIdentity, error) {
//	authProvider, err := auth.NewAuth0Client(m.ESUrl, m.ESUsername, m.ESPassword, "",
//		"", "", "", "", "")
//	if err != nil {
//		return nil, err
//	}
//
//	token := authProvider.GenerateToken()
//	header := make(map[string]string)
//	header["Authorization"] = token
//	var bData []byte
//	getIdentityAPI := fmt.Sprintf("%s/v1/affiliation/identity/%s/%s", m.AffAPI, key, value)
//	_, identityRes, err := m.fetcher.HTTPClientProvider.Request(getIdentityAPI, "GET", header, bData, nil)
//	if err != nil {
//		return nil, err
//	}
//
//	var ident IdentityData
//	err = json.Unmarshal(identityRes, &ident)
//	if err != nil {
//		return nil, err
//	}
//
//	getProfileAPI := fmt.Sprintf("%s/v1/affiliation/%s/get_profile/%v", m.AffAPI, m.ProjectSlug, ident.UUID)
//	_, profileRes, err := m.fetcher.HTTPClientProvider.Request(getProfileAPI, "GET", header, bData, nil)
//	if err != nil {
//		return nil, err
//	}
//
//	var profile UniqueIdentityFullProfile
//	err = json.Unmarshal(profileRes, &profile)
//	if err != nil {
//		return nil, err
//	}
//
//	var identity AffIdentity
//	identity.UUID = ident.UUID
//	identity.Name = *ident.Name
//	identity.Username = *ident.Username
//	identity.Email = *ident.Email
//	identity.ID = &ident.ID
//
//	identity.IsBot = profile.Profile.IsBot
//	identity.Gender = profile.Profile.Gender
//	identity.GenderACC = profile.Profile.GenderAcc
//
//	if len(profile.Enrollments) > 1 {
//		identity.OrgName = &profile.Enrollments[0].Organization.Name
//		for _, org := range profile.Enrollments {
//			identity.MultiOrgNames = append(identity.MultiOrgNames, org.Organization.Name)
//		}
//	} else if len(profile.Enrollments) == 1 {
//		identity.OrgName = &profile.Enrollments[0].Organization.Name
//		identity.MultiOrgNames = append(identity.MultiOrgNames, profile.Enrollments[0].Organization.Name)
//	}
//
//	return &identity, nil
//}

// IdentityData ...
//type IdentityData struct {
//	Email        *string    `json:"email,omitempty"`
//	ID           string     `json:"id,omitempty"`
//	LastModified *time.Time `json:"last_modified,omitempty"`
//	Name         *string    `json:"name,omitempty"`
//	Source       string     `json:"source,omitempty"`
//	Username     *string    `json:"username,omitempty"`
//	UUID         *string    `json:"uuid,omitempty"`
//}

//type UniqueIdentityFullProfile struct {
//	Enrollments []*Enrollments  `json:"enrollments"`
//	Identities  []*IdentityData `json:"identities"`
//	Profile     *Profile        `json:"profile,omitempty"`
//	UUID        string          `json:"uuid,omitempty"`
//}

// Enrollments ...
//type Enrollments struct {
//	Organization *Organization `json:"organization,omitempty"`
//}

//  Organization ...
//type Organization struct {
//	Name string `json:"name,omitempty"`
//}

// Profile ...
//type Profile struct {
//	Email     *string `json:"email,omitempty"`
//	Gender    *string `json:"gender,omitempty"`
//	GenderAcc *int64  `json:"gender_acc,omitempty"`
//	IsBot     *int64  `json:"is_bot,omitempty"`
//	Name      *string `json:"name,omitempty"`
//	UUID      string  `json:"uuid,omitempty"`
//}

// Identity contains affiliation user Identity
//type AffIdentity struct {
//	ID            *string `json:"id"`
//	UUID          *string
//	Name          string
//	Username      string
//	Email         string
//	Domain        string
//	Gender        *string  `json:"gender"`
//	GenderACC     *int64   `json:"gender_acc"`
//	OrgName       *string  `json:"org_name"`
//	IsBot         *int64   `json:"is_bot"`
//	MultiOrgNames []string `json:"multi_org_names"`
//}
