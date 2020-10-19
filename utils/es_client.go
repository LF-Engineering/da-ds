package utils

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

// ESClientProvider ...
type ESClientProvider struct {
	client *elasticsearch.Client
}

// ESParams ...
type ESParams struct {
	URL      string
	Username string
	Password string
}

// NewESClientProvider ...
func NewESClientProvider(params *ESParams) (*ESClientProvider, error) {
	config := elasticsearch.Config{
		Addresses: []string{params.URL},
		Username:  params.Username,
		Password:  params.Password,
	}

	client, err := elasticsearch.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &ESClientProvider{client}, err
}

/*func CreateIndex(index string) (res *esapi.Response, err error) {
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
*/

func (p *ESClientProvider) Add(index string, documentID string, body []byte) ([]byte, error) {
	buf := bytes.NewReader(body)

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: documentID,
		Body:       buf,
	}

	res, err := req.Do(context.Background(), p.client)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	var resBuf bytes.Buffer
	if _, err := resBuf.ReadFrom(res.Body); err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return resBuf.Bytes(), nil
}
