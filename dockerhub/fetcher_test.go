package dockerhub

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestFetchItemBasic(t *testing.T) {
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
	_, err = srv.FetchItem(owner, repo, time.Now())

	// Assert
	assert.NoError(t, err)

}

func TestFetchItemFromAPI(t *testing.T) {
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
	raw, err := srv.FetchItem(owner, repo, time.Now())
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

func TestFetchItem(t *testing.T) {
	// Arrange
	httpClientProviderMock := &mocks.HttpClientProvider{}
	esClientProviderMock := &mocks.ESClientProvider{}

	type httpClientData struct {
		owner      string
		repository string
	}
	type test struct {
		name           string
		httpClientData httpClientData
		expected       string
	}

	tests := []test{}

	testOnap := test{
		"OnapTest",
		httpClientData{
			"onap",
			"sdnc-ueb-listener-image",
		},
		`{
          "classified_fields_filtered" : null,
          "origin" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "metadata__timestamp" : "2020-07-31T15:51:45.585596Z",
          "backend_name" : "Dockerhub",
          "perceval_version" : "0.17.0",
          "backend_version" : "0.6.0",
          "uuid" : "974255f715035c22521d3324cff47968eb31a7d9",
          "timestamp" : 1.596210705585596E9,
          "metadata__updated_on" : "2020-07-31T15:51:45.585596Z",
          "data" : {
            "is_private" : false,
            "status" : 1,
            "fetched_on" : 1.596210705585596E9,
            "name" : "sdnc-ueb-listener-image",
            "can_edit" : false,
            "description" : "",
            "permissions" : {
              "write" : false,
              "read" : true,
              "admin" : false
            },
            "has_starred" : false,
            "namespace" : "onap",
            "last_updated" : "2020-06-25T11:31:33.021314Z",
            "affiliation" : null,
            "is_migrated" : false,
            "repository_type" : "image",
            "user" : "onap",
            "is_automated" : false,
            "pull_count" : 684,
            "full_description" : "",
            "star_count" : 0
          },
          "category" : "dockerhub-data",
          "updated_on" : 1.596210705585596E9,
          "tag" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "search_fields" : {
            "item_id" : "1596210705.585596",
            "namespace" : "onap",
            "name" : "sdnc-ueb-listener-image"
          }
        }`,
	}

	tests = append(tests, testOnap)

	for _, tst := range tests {
		t.Run(tst.name, func(tt *testing.T) {
			expectedRaw, err := toRepositoryRaw(tst.expected)
			if err != nil {
				t.Error(err)
			}

			data, err := json.Marshal(expectedRaw.Data)
			if err != nil {
				t.Error(err)
			}

			httpClientProviderMock.On("Request",
				fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s", tst.httpClientData.owner, tst.httpClientData.repository),
				"GET", mock.Anything, mock.Anything).Return(200, data, nil)

			params := &Params{
				Username:       "",
				Password:       "",
				BackendVersion: expectedRaw.BackendVersion,
			}

			srv := NewFetcher(params, httpClientProviderMock, esClientProviderMock)

			// Act
			raw, err := srv.FetchItem(tst.httpClientData.owner, tst.httpClientData.repository, expectedRaw.MetadataUpdatedOn)
			if err != nil {
				tt.Error(err)
			}
			// Assert
			assert.Equal(tt, expectedRaw.MetadataUpdatedOn.String(), raw.MetadataUpdatedOn.String())
			assert.Equal(tt, expectedRaw.Data.FetchedOn, raw.Data.FetchedOn)
			assert.Equal(tt, expectedRaw, *raw)
		})
	}
}

func toRepositoryRaw(b string) (RepositoryRaw, error) {
	expectedRaw := RepositoryRaw{}
	err := json.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
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
		raw, err := srv.FetchItem(repo.Owner, repo.Repository, time.Now())
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
