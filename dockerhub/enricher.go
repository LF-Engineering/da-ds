package dockerhub

import (
	"fmt"
	"log"
	"strings"
	"time"

	dads "github.com/LF-Engineering/da-ds"
)

// Enricher contains dockerhub datasource enrich logic
type Enricher struct {
	DSName                string // Datasource will be used as key for ES
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

// TopHits result
type TopHits struct {
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
	Index  string         `json:"_index"`
	Type   string         `json:"_type"`
	ID     string         `json:"_id"`
	Score  float64        `json:"_score"`
	Source *RepositoryRaw `json:"_source"`
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
func NewEnricher(backendVersion string, esClientProvider ESClientProvider) *Enricher {
	return &Enricher{
		DSName:                Dockerhub,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        backendVersion,
	}
}

// EnrichItem enriches raw item
func (e *Enricher) EnrichItem(rawItem RepositoryRaw, project string, now time.Time) (*RepositoryEnrich, error) {

	enriched := RepositoryEnrich{}

	enriched.ID = fmt.Sprintf("%s-%s", rawItem.Data.Name, rawItem.Data.Namespace)
	enriched.IsEvent = 0
	enriched.IsDockerImage = 1
	enriched.IsDockerhubDockerhub = 1
	enriched.Description = rawItem.Data.Description
	enriched.DescriptionAnalyzed = rawItem.Data.Description

	// todo: in python description is used ??
	if rawItem.Data.FullDescription == "" {
		enriched.FullDescriptionAnalyzed = rawItem.Data.Description
	} else {
		enriched.FullDescriptionAnalyzed = rawItem.Data.FullDescription
	}

	enriched.Affiliation = rawItem.Data.Affiliation
	enriched.IsAutomated = rawItem.Data.IsAutomated
	enriched.RepositoryType = rawItem.Data.RepositoryType
	enriched.User = rawItem.Data.User

	if rawItem.Data.IsPrivate == nil {
		enriched.IsPrivate = false
	} else {
		enriched.IsPrivate = *rawItem.Data.IsPrivate
	}

	if rawItem.Data.PullCount == nil {
		enriched.PullCount = 0
	} else {
		enriched.PullCount = *rawItem.Data.PullCount
	}

	if rawItem.Data.Status == nil {
		enriched.Status = 0
	} else {
		enriched.Status = *rawItem.Data.Status
	}

	if rawItem.Data.StarCount == nil {
		enriched.StarCount = 0
	} else {
		enriched.StarCount = *rawItem.Data.StarCount
	}

	enriched.LastUpdated = rawItem.Data.LastUpdated
	enriched.Project = project

	enriched.MetadataBackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.BackendVersion = e.BackendVersion

	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	if rawItem.MetadataTimestamp.IsZero() {
		enriched.MetadataTimestamp = rawItem.MetadataUpdatedOn.UTC()
	}

	enriched.MetadataUpdatedOn = rawItem.Data.LastUpdated
	enriched.MetadataEnrichedOn = rawItem.MetadataUpdatedOn.UTC()
	enriched.CreationDate = rawItem.Data.LastUpdated

	// todo: the 3 following fields filling is vague
	enriched.RepositoryLabels = nil
	enriched.MetadataFilterRaw = nil
	enriched.Offset = nil

	enriched.Origin = rawItem.Origin
	enriched.Tag = rawItem.Origin
	enriched.UUID = rawItem.UUID

	return &enriched, nil
}

// HandleMapping creates rich mapping
func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, DockerhubRichMapping)
	return err
}

// GetFetchedDataItem gets fetched data items starting from lastDate
func (e *Enricher) GetFetchedDataItem(repo *Repository, cmdLastDate *time.Time, lastDate *time.Time, noIncremental bool) (result *TopHits, err error) {
	rawIndex := fmt.Sprintf("%s-raw", repo.ESIndex)

	var lastEnrichDate *time.Time

	if noIncremental == false {
		if cmdLastDate != nil && !cmdLastDate.IsZero() {
			lastEnrichDate = cmdLastDate
		} else if lastDate != nil {
			lastEnrichDate = lastDate

			enrichLastDate, err := e.ElasticSearchProvider.GetStat(repo.ESIndex, "metadata__enriched_on", "max", nil, nil)
			if err != nil {
				log.Printf("Warning: %v", err)
			} else {
				if lastDate.After(enrichLastDate) {
					lastEnrichDate = &enrichLastDate
				}
			}
		}
	}

	url := fmt.Sprintf("%s/%s/%s", APIURL, repo.Owner, repo.Repository)

	hits := &TopHits{}

	query := map[string]interface{}{
		"size": 10000,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{},
			},
		},
		"collapse": map[string]string{
			"field": "origin.keyword",
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
				"origin.keyword": url,
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

	err = e.ElasticSearchProvider.Get(rawIndex, query, hits)
	if err != nil {
		dads.Printf("[dads-dockerhub] GetFetchedDataItem get elastic data error : %+v\n", err)
		return nil, err
	}

	return hits, nil
}
