package gitlab

import (
	"fmt"
	"strconv"
	"time"

	"github.com/labstack/gommon/log"

	"github.com/LF-Engineering/da-ds/build"
	"github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/slack"
)

// Manager ...
type Manager struct {
	HTTPClientProvider HTTPClientProvider
	HTTPTimeout        time.Duration
	ESUrl              string
	ESUsername         string
	ESPassword         string
	ESCacheURL         string
	ESCacheUsername    string
	ESCachePassword    string
	ESIndex            string
	ESBulkSize         int
	AuthGrantType      string
	AuthClientID       string
	AuthClientSecret   string
	AuthAudience       string
	Auth0URL           string
	Environment        string
	WebHookURL         string
	AffBaseURL         string
	ProjectSlug        string
	Project            string
	Repo               string
	Fetch              bool
	Enrich             bool
	Token              string
}

// MgrParams ...
type MgrParams struct {
	HTTPClientProvider HTTPClientProvider
	HTTPTimeout        time.Duration
	ESUrl              string
	ESUsername         string
	ESPassword         string
	ESCacheURL         string
	ESCacheUsername    string
	ESCachePassword    string
	ESIndex            string
	ESBulkSize         int
	AuthGrantType      string
	AuthClientID       string
	AuthClientSecret   string
	AuthAudience       string
	Auth0URL           string
	Environment        string
	AffBaseURL         string
	ProjectSlug        string
	Project            string
	Repo               string
	Fetch              bool
	Enrich             bool
	Token              string
}

// NewManager initiates bugzilla manager instance
func NewManager(param *MgrParams) (*Manager, error) {
	mgr := &Manager{
		HTTPTimeout:      param.HTTPTimeout,
		ESUrl:            param.ESUrl,
		ESUsername:       param.ESUsername,
		ESPassword:       param.ESPassword,
		ESCacheURL:       param.ESCacheURL,
		ESCacheUsername:  param.ESCacheUsername,
		ESCachePassword:  param.ESCachePassword,
		ESIndex:          param.ESIndex,
		ESBulkSize:       param.ESBulkSize,
		AuthGrantType:    param.AuthGrantType,
		AuthClientID:     param.AuthClientID,
		AuthClientSecret: param.AuthClientSecret,
		AuthAudience:     param.AuthAudience,
		Auth0URL:         param.Auth0URL,
		Environment:      param.Environment,
		AffBaseURL:       param.AffBaseURL,
		ProjectSlug:      param.ProjectSlug,
		Project:          param.Project,
		Repo:             param.Repo,
		Fetch:            param.Fetch,
		Enrich:           param.Enrich,
		Token:            param.Token,
	}

	return mgr, nil
}

func buildServices(m *Manager) (*Fetcher, *Enricher, ESClientProvider, error) {
	httpClientProvider := http.NewClientProvider(m.HTTPTimeout)
	params := &FetcherParams{
		BackendVersion: "0.0.1",
		Project:        m.Project,
		ProjectSlug:    m.ProjectSlug,
		Repo:           m.Repo,
		Token:          m.Token,
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

	if err != nil {
		return nil, nil, nil, err
	}

	affiliationsClientProvider, err := affiliation.NewAffiliationsClient(m.AffBaseURL, m.ProjectSlug, httpClientProvider, esCacheClientProvider, auth0Client, &slackProvider)
	if err != nil {
		return nil, nil, nil, err
	}

	fetcher := NewFetcher(params, httpClientProvider, esClientProvider)
	enricher := NewEnricher(esClientProvider, affiliationsClientProvider)

	return fetcher, enricher, esClientProvider, nil
}

// Sync ...
func (m *Manager) Sync() error {
	issueIndex := fmt.Sprintf("%s-issue", m.ESIndex)
	mergeRequestIndex := fmt.Sprintf("%s-pull_request", m.ESIndex)
	fetcher, enricher, esClientProvider, err := buildServices(m)
	if err != nil {
		return err
	}

	var (
		rawIssues        []IssueRaw
		rawMergeRequests []MergeRequestRaw
	)

	if m.Fetch {
		issueData := make([]elastic.BulkData, 0)
		mergeRequestData := make([]elastic.BulkData, 0)

		if m.Repo == "" {
			return fmt.Errorf("GITLAB: Repo URL cannot be empty")
		}

		// Get gitlab project id from the repo url
		projID, err := fetcher.getProjectID(m.Repo)
		if err != nil {
			return fmt.Errorf("GITLAB: Failed to get projectID for %s", m.Repo)
		}

		// ISSUES
		// Get last fetch date
		lastFetchDateIssue := fetcher.GetLastFetchDate(fmt.Sprintf("%s-raw", issueIndex))
		log.Printf("GITLAB: Fetching Issue Data for %+v since %+v", m.Repo, lastFetchDateIssue)

		rawIssues, err = fetcher.FetchIssues(projID, lastFetchDateIssue)
		if err != nil {
			return fmt.Errorf("GITLAB: Could not fetch issue data from project: %s", m.Repo)
		}

		for _, issueRaw := range rawIssues {
			issueData = append(issueData, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", issueIndex), ID: issueRaw.UUID, Data: issueRaw})
		}

		// Merge Requests
		// Get last fetch date
		lastFetchDatePR := fetcher.GetLastFetchDate(fmt.Sprintf("%s-raw", mergeRequestIndex))
		log.Printf("GITLAB: Fetching Pull Request Data for %+v since %+v", m.Repo, lastFetchDatePR)

		rawMergeRequests, err = fetcher.FetchMergeRequests(projID, lastFetchDatePR)
		if err != nil {
			return fmt.Errorf("GITLAB: Could not fetch Pull Request data from project: %s", m.Repo)
		}

		for _, mergeRequestRaw := range rawMergeRequests {
			mergeRequestData = append(mergeRequestData, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", mergeRequestIndex), ID: mergeRequestRaw.UUID, Data: mergeRequestRaw})
		}

		// Push to Index
		_, err = fetcher.ElasticSearchProvider.CreateIndex(fmt.Sprintf("%s-raw", issueIndex), GitlabRawMapping)
		if err != nil {
			return fmt.Errorf("GITLAB: Could not create raw index - %s-raw", m.ESIndex)
		}

		_, err = fetcher.ElasticSearchProvider.CreateIndex(fmt.Sprintf("%s-raw", mergeRequestIndex), GitlabRawMapping)
		if err != nil {
			return fmt.Errorf("GITLAB: Could not create raw index - %s-raw", m.ESIndex)
		}

		if len(issueData) > 0 {
			log.Infof("GITLAB: Exporting fetched issues to raw indices")
			for start := 0; start < len(issueData); start += m.ESBulkSize {
				end := start + m.ESBulkSize
				if end > len(issueData) {
					end = len(issueData)
				}
				batch := issueData[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					return fmt.Errorf("GITLAB: Error while BulkInsert to %s-raw", issueIndex)
				}
				log.Printf("GITLAB: Exported %d documents to raw index\n", len(batch))
			}
			log.Infof("GITLAB: Done exporting issues to raw indices")
		}

		if len(mergeRequestData) > 0 {
			log.Infof("GITLAB: Exporting fetched merge requests to raw indices")
			for start := 0; start < len(mergeRequestData); start += m.ESBulkSize {
				end := start + m.ESBulkSize
				if end > len(mergeRequestData) {
					end = len(mergeRequestData)
				}
				batch := mergeRequestData[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					return fmt.Errorf("GITLAB: Error while BulkInsert to %s-raw", mergeRequestIndex)
				}
				log.Printf("GITLAB: Exported %d documents to raw index\n", len(batch))
			}
			log.Infof("GITLAB: Done exporting merge requests to raw indices")
		}
	}

	if m.Enrich {
		log.Printf("GITLAB: Enriching issues data for %+v", m.Repo)
		issueData := make([]elastic.BulkData, 0)
		mergeRequestData := make([]elastic.BulkData, 0)

		for _, issueRaw := range rawIssues {
			issueEnriched, err := enricher.EnrichIssue(issueRaw, time.Now().UTC())
			if err != nil {
				return fmt.Errorf("GITLAB: Could not fetch data from project: %s", m.Repo)
			}
			issueData = append(issueData, elastic.BulkData{IndexName: issueIndex, ID: issueEnriched.ID, Data: issueEnriched})
		}

		log.Printf("GITLAB: Enriching merge requests data for %+v", m.Repo)
		for _, mergeRequestRaw := range rawMergeRequests {
			mergeRequestEnriched, err := enricher.EnrichMergeRequest(mergeRequestRaw, time.Now().UTC())
			if err != nil {
				return fmt.Errorf("GITLAB: Could not fetch data from project: %s", m.Repo)
			}
			mergeRequestData = append(mergeRequestData, elastic.BulkData{IndexName: mergeRequestIndex, ID: mergeRequestEnriched.ID, Data: mergeRequestEnriched})
		}

		// Push to Index
		_, err = enricher.ElasticSearchProvider.CreateIndex(m.ESIndex, GitlabRichMapping)
		if err != nil {
			return fmt.Errorf("GITLAB: Could not create rich index - %s", m.ESIndex)
		}

		if len(issueData) > 0 {
			log.Infof("GITLAB: Exporting enriched issues to rich index")
			for start := 0; start < len(issueData); start += m.ESBulkSize {
				end := start + m.ESBulkSize
				if end > len(issueData) {
					end = len(issueData)
				}
				batch := issueData[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					return fmt.Errorf("GITLAB: Error while BulkInsert to %s", m.ESIndex)
				}
				log.Printf("GITLAB: Exported %d documents to rich index\n", len(batch))
			}
			log.Infof("GITLAB: Done exporting issues to rich indices")
		}

		if len(mergeRequestData) > 0 {
			log.Infof("GITLAB: Exporting enriched merge requests to rich index")
			for start := 0; start < len(mergeRequestData); start += m.ESBulkSize {
				end := start + m.ESBulkSize
				if end > len(mergeRequestData) {
					end = len(mergeRequestData)
				}
				batch := mergeRequestData[start:end]
				_, err := esClientProvider.BulkInsert(batch)
				if err != nil {
					return fmt.Errorf("GITLAB: Error while BulkInsert to %s", m.ESIndex)
				}
				log.Printf("GITLAB: Exported %d documents to rich index\n", len(batch))
			}
			log.Infof("GITLAB: Done exporting merge requests to rich indices")
		}

	}

	return nil
}

// func (m *Manager) Runit() {
// 	fmt.Println("let's do this")
// 	fetcher, enricher, esClientProvider, err := buildServices(m)

// 	//projID, err := fetcher.getProjectID("https://gitlab.com/neho-systems/test")
// 	projID, err := fetcher.getProjectID(m.Repo)
// 	fmt.Println("Got the project ID", projID)

// 	rawData, _ := fetcher.FetchIssues(projID, time.Now())
// 	//rawData, _ := fetcher.FetchMergeRequests("https://gitlab.com/api")

// 	rawdata := make([]elastic.BulkData, 0)
// 	enricheddata := make([]elastic.BulkData, 0)
// 	for _, issueRaw := range rawData {
// 		rawdata = append(rawdata, elastic.BulkData{IndexName: fmt.Sprintf("%s-raw", m.ESIndex), ID: issueRaw.UUID, Data: issueRaw})

// 		// enrich data
// 		issueEnriched, err := enricher.EnrichIssue(issueRaw, time.Now().UTC())
// 		if err != nil {
// 			fmt.Println("enrichment do yawa")
// 		}
// 		enricheddata = append(enricheddata, elastic.BulkData{IndexName: m.ESIndex, ID: issueEnriched.ID, Data: issueEnriched})
// 	}

// 	_, err = fetcher.ElasticSearchProvider.CreateIndex(fmt.Sprintf("%s-raw", m.ESIndex), GitlabRawMapping)
// 	if err != nil {
// 		fmt.Println("fetcher index creating", err)
// 	}
// 	_, err = enricher.ElasticSearchProvider.CreateIndex(m.ESIndex, GitlabRichMapping)
// 	if err != nil {
// 		fmt.Println("enricher index creating", err)
// 	}

// 	for start := 0; start < len(rawdata); start += m.ESBulkSize {
// 		end := start + m.ESBulkSize
// 		if end > len(rawdata) {
// 			end = len(rawdata)
// 		}
// 		batch := rawdata[start:end]
// 		_, err := esClientProvider.BulkInsert(batch)
// 		if err != nil {
// 			//log.Println("Error while BulkInsert in Gitlab enrich index: ", err)

// 		}
// 		log.Printf("Exported %d documents", len(batch))
// 	}

// 	for start := 0; start < len(enricheddata); start += m.ESBulkSize {
// 		end := start + m.ESBulkSize
// 		if end > len(enricheddata) {
// 			end = len(enricheddata)
// 		}
// 		batch := enricheddata[start:end]
// 		_, err := esClientProvider.BulkInsert(batch)
// 		if err != nil {
// 			//log.Println("Error while BulkInsert in Gitlab enrich index: ", err)

// 		}
// 		log.Printf("Exported %d documents", len(batch))
// 	}

// 	fmt.Println(len(rawData))
// }
