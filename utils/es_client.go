package utils

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
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

// CreateIndex ...
func (p *ESClientProvider) CreateIndex(index string, body []byte) ([]byte, error) {
	buf := bytes.NewReader(body)

	// Create Index request
	res, err := esapi.IndicesCreateRequest{
		Index: index,
		Body:  buf,
	}.Do(context.Background(), p.client)
	if err != nil {
		return nil, err
	}

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	return resBytes, nil
}

func (p *ESClientProvider) DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error) {
	res, err := esapi.IndicesDeleteRequest{
		Index:             []string{index},
		IgnoreUnavailable: &ignoreUnavailable,
	}.Do(context.Background(), p.client)
	if err != nil {
		return nil, err
	}

	body, err := toBytes(res)
	if err != nil {
		return nil, err
	}
	return body, err
}

// convert response to bytes
func toBytes(res *esapi.Response) ([]byte, error) {
	var resBuf bytes.Buffer
	if _, err := resBuf.ReadFrom(res.Body); err != nil {
		return nil, err
	}
	resBytes := resBuf.Bytes()
	return resBytes, nil
}

// Add ...
func (p *ESClientProvider) Add(index string, documentID string, body []byte) ([]byte, error) {
	buf := bytes.NewReader(body)

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: documentID,
		Body:       buf,
	}

	res, err := req.Do(context.Background(), p.client)
	if err != nil {
		return nil, err
	}

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return resBytes, nil
}

// Bulk ...
func (p *ESClientProvider) Bulk(body []byte) ([]byte, error) {
	buf := bytes.NewReader(body)

	req := esapi.BulkRequest{
		Body:       buf,
	}

	res, err := req.Do(context.Background(), p.client)
	if err != nil {
		return nil, err
	}

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return resBytes, nil
}
