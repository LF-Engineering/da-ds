package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// Fetcher contains dockerhub datasource fetch logic
type Enricher struct {
	DSName                string // Datasource will be used as key for ES
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
}

type TopHitsStruct struct {
	Took         int          `json:"took"`
	Hits         Hits         `json:"hits"`
	Aggregations Aggregations `json:"aggregations"`
}

type Hits struct {
	Total    Total        `json:"total"`
	MaxScore float32      `json:"max_score"`
	Hits     []NestedHits `json:"hits"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type NestedHits struct {
	Index  string         `json:"_index"`
	Type   string         `json:"_type"`
	ID     string         `json:"_id"`
	Score  float64        `json:"_score"`
	Source *RepositoryRaw `json:"_source"`
}

type Aggregations struct {
	LastDate LastDate `json:"last_date"`
}

type LastDate struct {
	Value         float64 `json:"value"`
	ValueAsString string  `json:"value_as_string"`
}

func NewEnricher(backendVersion string, esClientProvider ESClientProvider) *Enricher {
	return &Enricher{
		DSName:                Dockerhub,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        backendVersion,
	}
}

func (e *Enricher) EnrichItem(rawItem RepositoryRaw, now time.Time) (*RepositoryEnrich, error) {

	enriched := RepositoryEnrich{}

	enriched.ID = fmt.Sprintf("%s-%s", rawItem.Data.Name, rawItem.Data.Namespace)
	enriched.IsEvent = 1
	enriched.IsDockerImage = 0
	enriched.IsDockerhubDockerhub = 1
	enriched.Description = rawItem.Data.Description
	enriched.DescriptionAnalyzed = rawItem.Data.Description

	// todo: in python description is used ??
	enriched.FullDescriptionAnalyzed = rawItem.Data.FullDescription
	enriched.Project = rawItem.Data.Name
	enriched.Affiliation = rawItem.Data.Affiliation
	enriched.IsPrivate = rawItem.Data.IsPrivate
	enriched.IsAutomated = rawItem.Data.IsAutomated
	enriched.PullCount = rawItem.Data.PullCount
	enriched.RepositoryType = rawItem.Data.RepositoryType
	enriched.User = rawItem.Data.User
	enriched.Status = rawItem.Data.Status
	enriched.StarCount = rawItem.Data.StarCount
	enriched.LastUpdated = rawItem.Data.LastUpdated

	enriched.BackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.BackendVersion = e.BackendVersion
	now = now.UTC()
	enriched.MetadataEnrichedOn = now

	enriched.MetadataTimestamp = rawItem.MetadataTimestamp
	enriched.MetadataUpdatedOn = rawItem.MetadataUpdatedOn
	enriched.CreationDate = rawItem.MetadataUpdatedOn

	// todo: the 3 following fields filling is vague
	enriched.RepositoryLabels = nil
	enriched.MetadataFilterRaw = nil
	enriched.Offset = nil

	enriched.Origin = rawItem.Origin
	enriched.Tag = rawItem.Origin
	enriched.UUID = rawItem.UUID

	return &enriched, nil
}

func (e *Enricher) Insert(index string, data *RepositoryEnrich) ([]byte, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	resData, err := e.ElasticSearchProvider.Add(index, data.UUID, body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, DockerhubRichMapping)
	return err
}

func (e *Enricher) GetPreviouslyFetchedDataItem(repo *Repository, cmdLastDate *time.Time, lastDate *time.Time, noIncremental bool) (result *TopHitsStruct, err error) {
	rawIndex := fmt.Sprintf("%s-raw", repo.ESIndex)

	var lastEnrichDate *time.Time = nil

	if noIncremental == false {
		if cmdLastDate != nil && !cmdLastDate.IsZero() {
			lastEnrichDate = cmdLastDate
		} else if lastDate != nil {
			lastEnrichDate = lastDate

			enrichLastDate, err := e.ElasticSearchProvider.GetStat(repo.ESIndex, "metadata__enriched_on", "max", nil, nil)
			if err != nil {
				log.Printf("Warning: %v", err)
			} else {
				// use the minimum date
				//esLD := enrichLastDate.(*float64)
				//sec, dec := math.Modf(*esLD)
				//esDate := time.Unix(int64(sec), int64(dec*(1e9)))

				if lastDate.After(enrichLastDate) {
					lastEnrichDate = &enrichLastDate
				}
			}
		}
	}

	url := fmt.Sprintf("%s/%s/%s", APIUrl, repo.Owner, repo.Repository)

	hits := &TopHitsStruct{}

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
		return nil, err
	}

	return hits, nil
}
