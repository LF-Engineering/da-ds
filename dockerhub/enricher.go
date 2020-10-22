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

	// todo:
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

	// todo: remove this
	fmt.Printf("enriched data: %s\n", body)

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
