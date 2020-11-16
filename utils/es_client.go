package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"regexp"
	"strings"
	"time"
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

type TopHitsStruct struct {
	Took         int          `json:"took"`
	Aggregations Aggregations `json:"aggregations"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Aggregations struct {
	Stat Stat `json:"stat"`
}

type Stat struct {
	Value         float64 `json:"value"`
	ValueAsString string  `json:"value_as_string"`
}

type BulkData struct {
	IndexName string
	ID        string
	Data      interface{}
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
	defer res.Body.Close()

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}

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
	defer res.Body.Close()

	body, err := toBytes(res)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 200 {
		return body, nil
	}

	if res.IsError() {

		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, err
		}

		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return nil, err
	}

	return body, nil
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
	defer res.Body.Close()

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 200 {
		return resBytes, nil
	}

	if res.IsError() {

		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, err
		}

		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return nil, err
	}

	return resBytes, nil
}

// Bulk ...
func (p *ESClientProvider) Bulk(body []byte) ([]byte, error) {
	buf := bytes.NewReader(body)

	req := esapi.BulkRequest{
		Body: buf,
	}

	res, err := req.Do(context.Background(), p.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	resBytes, err := toBytes(res)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == 200 {
		return resBytes, nil
	}

	if res.IsError() {

		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, err
		}

		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return nil, err
	}

	return resBytes, nil
}

func (p *ESClientProvider) BulkInsert(data []*BulkData) ([]byte, error) {
	lines := make([]interface{}, 0)

	for _, item := range data {
		indexName := item.IndexName
		index := map[string]interface{}{
			"index": map[string]string{
				"_index": indexName,
				"_id":    item.ID,
			},
		}
		lines = append(lines, index)
		lines = append(lines, "\n")
		lines = append(lines, item.Data)
		lines = append(lines, "\n")
	}

	body, err := json.Marshal(lines)
	if err != nil {
		return nil, errors.New("unable to convert body to json")
	}

	var re = regexp.MustCompile(`(}),"\\n",?`)
	body = []byte(re.ReplaceAllString(strings.TrimSuffix(strings.TrimPrefix(string(body), "["), "]"), "$1\n"))

	resData, err := p.Bulk(body)
	if err != nil {
		return nil, err
	}

	return resData, nil
}

func (p *ESClientProvider) Get(index string, query map[string]interface{}, result interface{}) (err error) {
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(query)
	if err != nil {
		return err
	}

	fmt.Println(&buf)

	res, err := p.client.Search(
		p.client.Search.WithIndex(index),
		p.client.Search.WithBody(&buf),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		// index exists so return true

		fmt.Println(res.Body)
		if err = json.NewDecoder(res.Body).Decode(result); err != nil {
			return err
		}

		return nil
	}

	if res.IsError() {
		if res.StatusCode == 404 {
			// index doesn't exist
			return errors.New("index doesn't exist")
		}

		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}

		err = fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
		return err
	}

	return nil
}

func (p *ESClientProvider) GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error) {

	hits := &TopHitsStruct{}

	q := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{},
		},
		"aggs": map[string]interface{}{
			"stat": map[string]interface{}{
				aggType: map[string]interface{}{
					"field": field,
				},
			},
		},
	}

	if mustConditions != nil {
		q["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = mustConditions
	}

	if mustNotConditions != nil {
		q["query"].(map[string]interface{})["bool"].(map[string]interface{})["must_not"] = mustNotConditions
	}
	err = p.Get(index, q, hits)
	if err != nil {
		return time.Now().UTC(), err
	}
	date, err := time.Parse(time.RFC3339, hits.Aggregations.Stat.ValueAsString)
	if err != nil {
		return time.Now().UTC(), err
	}

	return date, nil
}
