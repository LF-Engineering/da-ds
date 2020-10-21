package dockerhub

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"testing"
	"time"
)

func TestFetchItems(t *testing.T) {
	srv, err := prepareObject()
	if err != nil {
		t.Errorf("err: %v", err.Error())
		return
	}
	owner := "hyperledger"
	repo := "caliper"

	raw, err := srv.FetchItem(owner, repo)
	if err != nil {
		t.Errorf("err: %v", err.Error())
		return
	}

	t.Logf("%v", raw)
}

func prepareObject() (*Fetcher, error) {
	httpClientProvider := utils.NewHttpClientProvider(5 * time.Second)
	params := &Params{
		Username:       "",
		Password:       "",
		BackendVersion: "0.0.1",
	}
	esClientProvider, err := utils.NewESClientProvider(&utils.ESParams{
		URL:      "http://localhost:9200",
		Username: "elastic",
		Password: "changeme",
	})
	if err != nil {
		fmt.Println("err22222 ", err.Error())
	}
	srv := NewFetcher(params, httpClientProvider, esClientProvider)
	return srv, err
}

func TestBulkInsert(t *testing.T) {
	srv, err := prepareObject()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	rawData := make([]*RepositoryRaw, 0)

	raw, err := srv.FetchItem("hyperledger", "besu")
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	rawData = append(rawData, raw)

	raw, err = srv.FetchItem("hyperledger", "explorer")
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	rawData = append(rawData, raw)

	t.Logf("response: %v", rawData)

	for _, item := range rawData {
		err := srv.HandleMapping(fmt.Sprintf("sds-%s-%s-dockerhub-raw", item.Data.Namespace, item.Data.Name))
		if err != nil {
			t.Errorf("err: %v", err)
		}
	}

	insert, err := srv.BulkInsert(rawData)
	if err != nil {
		t.Errorf("err: %v", err.Error())
		return
	}

	t.Logf("response: %s", insert)

}
