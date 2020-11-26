package dockerhub

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/da-ds/utils/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestFetchItem(t *testing.T) {
	// Arrange
	owner := "hyperledger"
	repo := "caliper"

	params := &Params{
		Username:       "",
		Password:       "",
		BackendVersion: "0.0.1",
	}
	httpClientProviderMock := &mocks.HttpClientProvider{}

	fakeResult := make(map[string]interface{})
	fakeResult["user"] = "hyperledger"
	fakeResult["name"] = "caliper"
	fakeResult["namespace"] = "hyperledger"
	fakeResult["repository_type"] = "image"
	fakeResult["status"] = 1
	fakeResult["description"] = "Caliper image for benchmarking blockchain platforms"
	fakeResult["is_private"] = false
	fakeResult["is_automated"] = false
	fakeResult["can_edit"] = false
	fakeResult["star_count"] = 1
	fakeResult["pull_count"] = 3272
	fakeResult["last_updated"] = "2020-10-19T13:15:09.478235Z"
	fakeResult["is_migrated"] = false
	fakeResult["has_starred"] = false
	fakeResult["full_description"] = "Documentation: https://hyperledger.github.io/caliper\\n\\nGitHub repository: https://github.com/hyperledger/caliper"
	fakeResult["affiliation"] = nil
	fakeResult["permissions"] = map[string]interface{}{"read": true, "write": false, "admin": false}

	b, err := json.Marshal(fakeResult)
	if err != nil {
		t.Fatal(err)
	}

	httpClientProviderMock.On("Request",
		fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s", owner, repo),
		"GET", mock.Anything, mock.Anything).Return(200, b, nil)

	esClientProviderMock := &mocks.ESClientProvider{}

	srv := NewFetcher(params, httpClientProviderMock, esClientProviderMock)

	// Act
	_, err = srv.FetchItem(owner, repo)

	// Assert
	assert.NoError(t, err)

}

func TestFetchItem2(t *testing.T) {
	// Arrange
	owner := "hyperledger"
	repo := "caliper"

	params := &Params{
		Username:       "",
		Password:       "",
		BackendVersion: "0.0.1",
	}
	httpClientProviderMock := &mocks.HttpClientProvider{}

	b := `{
    "user": "grimoirelab",
    "name": "perceval",
    "namespace": "grimoirelab",
    "repository_type": "image",
    "status": 1,
    "description": "Perceval Docker image to work in standalone mode",
    "is_private": false,
    "is_automated": true,
    "can_edit": false,
    "star_count": 1,
    "pull_count": 398,
    "last_updated": "2017-05-10T08:12:52.217787Z",
    "build_on_cloud": null,
    "has_starred": false,
    "full_description": "# Perceval [![Build Status](https://travis-ci.org/grimoirelab/perceval.svg?branch=master)](https://travis-ci.org/grimoirelab/perceval) [![Coverage Status](https://img.shields.io/coveralls/grimoirelab/perceval.svg)](https://coveralls.io/r/grimoirelab/perceval?branch=master)\n\nSend Sir Perceval on a quest to retrieve and gather data from software\nrepositories.\n\n## Usage\n\n\nusage:",
"affiliation": null,
"permissions": {
"read": true,
"write": false,
"admin": false
}
}`

	httpClientProviderMock.On("Request",
		fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s", owner, repo),
		"GET", mock.Anything, mock.Anything).Return(200, []byte(b), nil)

	esClientProviderMock := &mocks.ESClientProvider{}

	srv := NewFetcher(params, httpClientProviderMock, esClientProviderMock)

	// Act
	raw, err := srv.FetchItem(owner, repo)
	if err != nil {
		t.Errorf("cannot get data")
		return
	}

	testTime := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	raw.Data.FetchedOn = utils.ConvertTimeToFloat(testTime)
	uid, err := uuid.Generate(fmt.Sprintf("%v", raw.Data.FetchedOn))
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	raw.UUID = uid
	// Assert
	assert.Equal(t, "0fa16dc4edab9130a14914a8d797f634d13b4ff4", raw.UUID)
	assert.Equal(t, "1483228800", raw.Data.FetchedOn)

}

func prepareObject() (*Fetcher, ESClientProvider, error) {
	httpClientProvider := utils.NewHTTPClientProvider(5 * time.Second)

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
	return srv, esClientProvider, err
}

func TestBulkInsert(t *testing.T) {
	srv, esClientProvider, err := prepareObject()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	rawData := make([]*utils.BulkData, 0)
	repos := []*Repository{
		{"hyperledger", "besu", "sds-hyperledger-besu-dockerhub"},
		{"hyperledger", "explorer", "sds-hyperledger-explorer-dockerhub"},
	}

	for _, repo := range repos {
		raw, err := srv.FetchItem(repo.Owner, repo.Repository)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		rawData = append(rawData, &utils.BulkData{IndexName: repo.ESIndex, ID: raw.UUID, Data: raw})

		err = srv.HandleMapping(fmt.Sprintf("%s-raw", repo.ESIndex))
		if err != nil {
			t.Errorf("err: %v", err)
		}
	}

	t.Logf("response: %v", rawData)

	insert, err := esClientProvider.BulkInsert(rawData)
	if err != nil {
		t.Errorf("err: %v", err.Error())
		return
	}

	t.Logf("response: %s", insert)

}
