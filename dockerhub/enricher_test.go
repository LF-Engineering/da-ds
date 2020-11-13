package dockerhub

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"testing"
	"time"
)

func prepareEnrichObject() (*Enricher, error) {
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      "http://localhost:9200",
		Username: "elastic",
		Password: "changeme",
	})
	if err != nil {
		fmt.Println("err22222 ", err.Error())
	}
	srv := NewEnricher("0.0.1", esClientProvider)
	return srv, err
}

func TestGetPreviouslyFetchedData(t *testing.T) {
	srv, err := prepareEnrichObject()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	repos := []*Repository{
		{"cncf", "envoy"},
		{"hyperledger", "explorer"},
	}

    lastDate := "2019-10-20T18:07:47.729125Z"
    d, err := time.Parse(time.RFC3339, lastDate)

    for _, repo := range repos {
		raws, err := srv.GetPreviouslyFetchedDataItem(*repo, nil, &d, false)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		t.Logf("results: %v", raws.Hits.Hits)
	}
}
