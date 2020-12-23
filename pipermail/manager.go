package pipermail

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/affiliation"
	"github.com/LF-Engineering/da-ds/db"
	"github.com/LF-Engineering/da-ds/utils"
	"time"
)

// Manager describes pipermail manager
type Manager struct {
	Username               string        `json:"username,omitempty"`
	SHConnString           string `json:"sh_conn_string,omitempty"`
	Password               string        `json:"password,omitempty"`
	FetcherBackendVersion  string        `json:"fetcher_backend_version,omitempty"`
	EnricherBackendVersion string        `json:"enricher_backend_version,omitempty"`
	EnrichOnly             bool          `json:"enrich_only,omitempty"`
	Enrich                 bool          `json:"enrich,omitempty"`
	ESUrl                  string        `json:"es_url,omitempty"`
	ESUsername             string        `json:"es_username,omitempty"`
	ESPassword             string        `json:"es_password,omitempty"`
	HTTPTimeout            time.Duration `json:"http_timeout,omitempty"`
	Links                  []*Link       `json:"links,omitempty"`
	FromDate               *time.Time    `json:"from_date,omitempty"`
	NoIncremental          bool          `json:"no_incremental,omitempty"`
}

// Link represents piper mail link data
type Link struct {
	Link        string `json:"Link"`
	Project     string `json:"Project"`
	ESIndex     string `json:"ESIndex"`
	GroupName   string `json:"GroupName"`
	ProjectSlug string `json:"ProjectSlug"`
}

// NewManager initiates piper mail manager instance
func NewManager(
	fetcherBackendVersion string,
	enricherBackendVersion string,
	enrichOnly bool,
	enrich bool,
	eSUrl string,
	httpTimeout time.Duration,
	links []*Link,
	fromDate *time.Time,
	noIncremental bool,
) *Manager {
	mng := &Manager{
		FetcherBackendVersion:  fetcherBackendVersion,
		EnricherBackendVersion: enricherBackendVersion,
		EnrichOnly:             enrichOnly,
		Enrich:                 enrich,
		ESUrl:                  eSUrl,
		HTTPTimeout:            httpTimeout,
		Links:                  links,
		FromDate:               fromDate,
		NoIncremental:          noIncremental,
	}

	return mng
}

// Sync runs pipermail fetch and enrich according to passed parameters
func (m *Manager) Sync() error {
	var rawMessages []*RawMessage

	if len(m.Links) == 0 {
		return errors.New("no links found")
	}

	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}

	if !m.EnrichOnly {
		data := make([]*utils.BulkData, 0)

		// fetch data
		for _, link := range m.Links {
			// Fetch data for single link
			raw, err := fetcher.FetchItem(link.ProjectSlug, link.GroupName, link.Link, time.Now())
			if err != nil {
				return fmt.Errorf("could not fetch data arcchives from link: %s-%s", err.Error(), link.Link)
			}

			for _, message := range raw {
				data = append(data, &utils.BulkData{IndexName: fmt.Sprintf("%s-raw", link.ESIndex), ID: message.UUID, Data: message})
				rawMessages = append(rawMessages, message)
			}

			// set mapping and create index if not exists
			_ = fetcher.HandleMapping(fmt.Sprintf("%s-raw", link.ESIndex))
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

		// fetch data
		for _, link := range m.Links {
			if len(rawMessages) > 0 {
				// Enrich data for single link
				for _, message := range rawMessages {
					enriched, err := enricher.EnrichMessage(message, time.Now())
					if err != nil {
						return fmt.Errorf("could not enrich data from link: %s", message.Origin)
					}
					data = append(data, &utils.BulkData{IndexName: link.ESIndex, ID: enriched.UUID, Data: enriched})
					_ = enricher.HandleMapping(link.ESIndex)
				}

			}
		}

		if len(data) > 0 {
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
		BackendVersion: m.FetcherBackendVersion,
		Links:          m.Links,
	}
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      m.ESUrl,
		Username: m.ESUsername,
		Password: m.ESPassword,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Initialize fetcher object to get data from pipermail archive link
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	dataBase, err := db.NewConnector("mysql", m.SHConnString)
	if err != nil {
		return nil, nil, nil, err
	}
	identityProvider := affiliation.NewIdentityProvider(dataBase)

	//Initialize enrich object to enrich raw data
	enricher := NewEnricher(identityProvider, m.EnricherBackendVersion, esClientProvider)

	return fetcher, enricher, esClientProvider, err
}
