package jenkins

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Enricher contains jenkins datasource enrich logic
type Enricher struct {
	DSName                string // Datasource will be used as key for ES
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	ScrollSize            int
}

// TopHits result
type TopHits struct {
	ScrollID     string       `json:"_scroll_id"`
	Took         int          `json:"took"`
	Hits         Hits         `json:"hits"`
	Aggregations Aggregations `json:"aggregations"`
}

// Hits result
type Hits struct {
	Total    Total        `json:"total"`
	MaxScore float32      `json:"max_score"`
	Hits     []NestedHits `json:"hits"`
}

// Total result
type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// NestedHits result
type NestedHits struct {
	Index  string     `json:"_index"`
	Type   string     `json:"_type"`
	ID     string     `json:"_id"`
	Score  float64    `json:"_score"`
	Source *BuildsRaw `json:"_source"`
}

// Aggregations result
type Aggregations struct {
	LastDate LastDate `json:"last_date"`
}

// LastDate result
type LastDate struct {
	Value         float64 `json:"value"`
	ValueAsString string  `json:"value_as_string"`
}

// NewEnricher initiates a new Enricher
func NewEnricher(backendVersion string, esClientProvider ESClientProvider, scrollSize int) *Enricher {
	return &Enricher{
		DSName:                Jenkins,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        backendVersion,
		ScrollSize:            scrollSize,
	}
}

// EnrichItem enriches raw item
func (e *Enricher) EnrichItem(rawItem BuildsRaw, project string, now time.Time) (*BuildsEnrich, error) {

	enriched := BuildsEnrich{}
	enriched.Project = project
	enriched.UUID = rawItem.UUID
	enriched.FullDisplayName = rawItem.Data.FullDisplayName
	enriched.FullDisplayNameAnalyzed = enriched.FullDisplayName
	enriched.URL = rawItem.Data.URL
	enriched.Origin = rawItem.Origin
	enriched.Category = rawItem.Category
	enriched.Duration = rawItem.Data.Duration
	if rawItem.Data.BuiltOn == "" {
		enriched.BuiltOn = "main"
	} else {
		enriched.BuiltOn = rawItem.Data.BuiltOn
	}
	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn
	enriched.MetadataEnrichedOn = time.Now()
	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.MetadataBackendVersion = e.BackendVersion
	enriched.BuildDate = time.Unix(0, rawItem.Data.Timestamp*int64(time.Millisecond))
	enriched.Build = rawItem.Data.Number
	parts := strings.Split(rawItem.Data.DisplayName, " ")
	enriched.Tag = rawItem.Tag
	enriched.JobBuild = parts[0] + "/" + rawItem.Data.ID
	enriched.JobURL = strings.TrimRight(rawItem.Data.URL, "/"+rawItem.Data.ID)
	parts = strings.Split(enriched.JobURL, "/")
	enriched.JobName = parts[len(parts)-1]
	if rawItem.Data.Result != nil {
		enriched.Result = rawItem.Data.Result
	}
	enriched.GrimoireCreationDate = enriched.BuildDate
	enriched.IsJenkinsJob = 1
	// Calculate Duration
	secondsDay := float64(60 * 60 * 24)
	durationDays := float64(enriched.Duration) / (1000 * secondsDay)
	enriched.DurationDays = durationDays

	// Extract information from job_name
	jobParts := strings.Split(enriched.JobName, "-")
	if len(jobParts) >= 2 {
		kind := jobParts[1]
		if kind == "os" {
			enriched.Category = "parents/main"
			enriched.Scenario = strings.Join(jobParts[2:len(jobParts)-3], "-")
		} else if kind == "deploy" {
			enriched.Category = "deploy"
		} else {
			enriched.Category = "test"
			enriched.Testproject = jobParts[0]
		}
		enriched.Loop = jobParts[len(jobParts)-2]
		enriched.Branch = jobParts[len(jobParts)-1]
	}
	enriched.Installer = rawItem.Installer
	if len(jobParts) >= 3 {
		enriched.Pod = jobParts[len(jobParts)-3]
	}
	return &enriched, nil
}

// HandleMapping creates rich mapping
func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, JenkinsRichMapping)
	return err
}

// GetFetchedDataItem gets fetched data items starting from lastDate
func (e *Enricher) GetFetchedDataItem(buildServer *BuildServer, cmdLastDate *time.Time, lastDate *time.Time, noIncremental bool) (result *TopHits, err error) {
	rawIndex := fmt.Sprintf("%s-raw", buildServer.Index)
	var lastEnrichDate *time.Time

	// if the fetch needs to be incremental
	if noIncremental == false {
		if cmdLastDate != nil && !cmdLastDate.IsZero() {
			// if fromDate is given in the command
			lastEnrichDate = cmdLastDate
		} else if lastDate != nil {
			// lastRawDate exists
			lastEnrichDate = lastDate

			enrichLastDate, err := e.ElasticSearchProvider.GetStat(buildServer.Index, "metadata__enriched_on", "max", nil, nil)
			if err != nil {
				log.Printf("Warning: %v", err)
			} else {
				if lastDate.After(enrichLastDate) {
					lastEnrichDate = &enrichLastDate
				}
			}
		} else {
			lastEnrichDate = &DefaultDateTime
		}
	}

	url := buildServer.URL

	hits := &TopHits{}

	query := map[string]interface{}{
		"size": e.ScrollSize,
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

	conditions := []map[string]interface{}{
		{
			"term": map[string]interface{}{
				"origin": url,
			},
		},
	}

	if lastEnrichDate != nil {
		conditions = append(conditions,
			map[string]interface{}{
				"range": map[string]interface{}{
					"metadata__updated_on": map[string]interface{}{
						"gte": (*lastEnrichDate).Format(time.RFC3339),
					},
				},
			},
		)
	}

	query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = conditions

	scrollID := ""
	for {
		result := &TopHits{}
		err = e.ElasticSearchProvider.ReadWithScroll(rawIndex, query, result, scrollID)
		if err != nil {
			return nil, err
		}
		if len(result.Hits.Hits) < 1 {
			break
		}
		scrollID = result.ScrollID
		hits.Hits.Hits = append(hits.Hits.Hits, result.Hits.Hits...)
		if scrollID == "" {
			break
		}
	}
	return hits, nil
}
