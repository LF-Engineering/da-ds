package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"regexp"
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
	Took   int    `json:"took"`
	Hits   Hits   `json:"hits"`
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
	Index  string           `json:"_index"`
	Type   string           `json:"_type"`
	ID     string           `json:"_id"`
	Score  float64          `json:"_score"`
	Source *RepositoryRaw `json:"_source"`
}

type Aggregations struct {
	LastDate LastDate `json:"last_date"`
}

type LastDate struct {
	Value float64 `json:"value"`
	ValueAsString string `json:"value_as_string"`
}

func NewEnricher(BackendVersion string, esClientProvider ESClientProvider) *Enricher {
	return &Enricher{
		DSName:                Dockerhub,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        BackendVersion,
	}
}

func (e *Enricher) EnrichItem(rawItem RepositoryRaw) (*RepositoryEnrich, error) {

	enriched := RepositoryEnrich{}

	enriched.ID = fmt.Sprintf("%s-%s", rawItem.Data.Namespace, rawItem.Data.Name)
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

	enriched.BackendName = fmt.Sprintf("%sEnrich", strings.Title(e.DSName))
	enriched.BackendVersion = e.BackendVersion
	timestamp := time.Now()
	enriched.MetadataEnrichedOn = dads.ToESDate(timestamp)

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

func (e *Enricher) Insert(data *RepositoryEnrich) ([]byte, error) {
	body, err := json.Marshal(data)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	resData, err := e.ElasticSearchProvider.Add(fmt.Sprintf("sds-%s-dockerhub-raw", data.ID), data.UUID, body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (e *Enricher) BulkInsert(data []*RepositoryEnrich) ([]byte, error) {
	enriched := make([]interface{}, 0)

	for _, item := range data {
		enriched = append(enriched, map[string]interface{}{"index": map[string]string{"_index": fmt.Sprintf("sds-%s-dockerhub", item.ID), "_id": item.UUID}})
		enriched = append(enriched, "\n")
		enriched = append(enriched, item)
		enriched = append(enriched, "\n")
	}

	body, err := json.Marshal(enriched)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	var re = regexp.MustCompile(`(}),"\\n",?`)
	body = []byte(re.ReplaceAllString(strings.TrimSuffix(strings.TrimPrefix(string(body), "["), "]"), "$1\n"))

	resData, err := e.ElasticSearchProvider.Bulk(body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (e *Enricher) HandleMapping(index string) error {
	_, err := e.ElasticSearchProvider.CreateIndex(index, DockerhubRichMapping)
	return err
}

func (e *Enricher) GetPreviouslyFetchedData(repositories []*Repository) (result *TopHitsStruct, err error) {

	urls := make([]string, 0)

	for _, repo := range repositories {
		url := fmt.Sprintf("%s/%s/%s", APIUrl, repo.Owner, repo.Repository)

		urls = append(urls, url)
	}

	hits := &TopHitsStruct{}

	query := map[string]interface{}{
		"size": 10000,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"origin": urls,
						},
					},
				},
			},
		},
		"collapse": map[string]string{
			"field": "origin",
		},
		"sort": []map[string]interface{}{
			{
				"metadata__updated_on": map[string]string{
					"order": "desc",
				},
			},
		},
	}

	err = e.ElasticSearchProvider.Get("sds-*-dockerhub-raw", query, hits)
	if err != nil {
		return nil, err
	}

	return hits, nil
}

func (e *Enricher) GetLastEnrichDate() (result float64, err error) {
	hits := &TopHitsStruct{}

	query := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			"last_date": map[string]interface{}{
				"max": map[string]interface{}{
						"field": "metadata__enriched_on",
				},
			},
		},
	}

	err = e.ElasticSearchProvider.Get("sds-*-dockerhub", query, hits)
	if err != nil {
		return 0, err
	}

	return hits.Aggregations.LastDate.Value, nil
}
