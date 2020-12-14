package pipermail

import (
	"errors"
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"time"
)

// Manager describes pipermail manager
type Manager struct {
	Username               string        `json:"username,omitempty"`
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
	DS           string `json:"ds,omitempty"`
	Email        string `json:"email,omitempty"`
	NoSSLVerify  bool   `json:"no_ssl_verify,omitempty"`
	SaveArchives bool   `json:"save_archives,omitempty"`
	ArchPath     string `json:"arch_path,omitempty"`
	MultiOrigin  bool   `json:"multi_origin,omitempty"`
}

// Link represents piper mail link data
type Link struct {
	Link    string `json:"Link"`
	Project string `json:"Project"`
	ESIndex string `json:"ESIndex"`
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

	if len(m.Links) == 0 {
		return errors.New("no links found")
	}

	fetcher, _, _, err := buildServices(m)
	if err != nil {
		return err
	}


	if !m.EnrichOnly {
		//data := make([]*utils.BulkData, 0)

		// fetch data
		for _, link := range m.Links {
			// Fetch data for single link
			_, err = fetcher.FetchItem("", link.Link, time.Now())
			if err != nil {
				return fmt.Errorf("could not fetch data arcchives from link: %s-%s",err.Error(), link.Link)
			}
		}

	}


	return nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := utils.NewHTTPClientProvider(m.HTTPTimeout)
	params := &Params{
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

	// Initialize fetcher object to get data from pipermail archive link
	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)

	// Initialize enrich object to enrich raw data
	//enricher := NewEnricher(m.EnricherBackendVersion, esClientProvider)

	return fetcher, nil, esClientProvider, err
}