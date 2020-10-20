package dockerhub

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"testing"
	"time"
)

func TestFetchItems(t *testing.T) {
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
	owner := "hyperledger"
	repo := "caliper"
	srv := NewFetcher(params, httpClientProvider, esClientProvider)

	if err := srv.FetchItem(owner, repo); err != nil {
		fmt.Println("err1111 ", err.Error())
	}

}
