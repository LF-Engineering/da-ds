package elastic

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io"
	"os"
)

// Provider ...
type Provider struct {
	client *elasticsearch.Client
}

// NewProvider ...
func NewProvider() (*Provider, error) {
	config := elasticsearch.Config{
		Addresses: []string{os.Getenv("ELASTIC_URL")},
		Username:  os.Getenv("ELASTIC_USERNAME"),
		Password:  os.Getenv("ELASTIC_PASSWORD"),
	}

	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &Provider{ client}, err
}

func CreateIndex(index string) (res *esapi.Response, err error) {
	var client *elasticsearch.Client
	client, err = GetClient()
	if err != nil {
		return
	}

	// Create Index request
	res, err = esapi.IndicesCreateRequest{
		Index: index,
		Body: strings.NewReader(
			`{
				"mappings": {
					"dynamic_templates": [
					  {
						"notanalyzed": {
						  "match": "*",
						  "match_mapping_type": "string",
						  "mapping": {
							"type": "keyword"
						  }
						}
					  },
					  {
						"formatdate": {
						  "match": "*",
						  "match_mapping_type": "date",
						  "mapping": {
							"format": "strict_date_optional_time||epoch_millis",
							"type": "date"
						  }
						}
					  }
					],
					"properties": { 
					  "grimoire_creation_date": {
						"type": "date"
					  }
						  }
				}
			  }`),
	}.Do(context.Background(), client)

	return res, err
}

func (e *Provider) Create(index string, document interface{}) (res *esapi.Response, err error) {
	return e.client.Create(
		query,
		client.Bulk.WithIndex(index),
	)
}