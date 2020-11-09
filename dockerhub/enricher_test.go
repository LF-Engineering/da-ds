package dockerhub

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"testing"
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
		{"hyperledger", "besu"},
		{"hyperledger", "explorer"},
	}

	raws, err := srv.GetPreviouslyFetchedData(repos)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	t.Logf("results: %v", raws.Hits.Hits)
}
