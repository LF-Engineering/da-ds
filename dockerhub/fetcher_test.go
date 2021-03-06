package dockerhub

import (
	"fmt"
	"testing"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"

	"github.com/LF-Engineering/da-ds/dockerhub/mocks"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	httpClientProviderMock := &mocks.HTTPClientProvider{}

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

	b, err := jsoniter.Marshal(fakeResult)
	if err != nil {
		t.Fatal(err)
	}

	httpClientProviderMock.On("Request",
		fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s", owner, repo),
		"GET", mock.Anything, mock.Anything, mock.Anything).Return(200, b, nil)

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
	httpClientProviderMock := &mocks.HTTPClientProvider{}

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
		"GET", mock.Anything, mock.Anything, mock.Anything).Return(200, []byte(b), nil)

	esClientProviderMock := &mocks.ESClientProvider{}

	srv := NewFetcher(params, httpClientProviderMock, esClientProviderMock)
	testTime := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)

	// Act
	raw, err := srv.FetchItem(owner, repo, testTime)
	if err != nil {
		t.Errorf("cannot get data")
		return
	}

	uid, err := uuid.Generate(raw.Origin, fmt.Sprintf("%f", raw.Data.FetchedOn))
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	raw.UUID = uid
	// Assert
	assert.Equal(t, "152c1e2f550c723b71dcdb88b297874f92377ef7", raw.UUID)
	assert.Equal(t, 1.4832288e09, raw.Data.FetchedOn)

}

func TestFetchItem(t *testing.T) {
	// Arrange
	httpClientProviderMock := &mocks.HTTPClientProvider{}
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
          "uuid" : "38308aeedecf37da98e097af718bbe736dbd4f25",
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

	testFabric := test{
		"FabricTest",
		httpClientData{
			"hyperledger",
			"fabric-zookeeper",
		},
		`{
          "data" : {
            "user" : "hyperledger",
            "name" : "fabric-zookeeper",
            "full_description" : "",
            "affiliation" : null,
            "repository_type" : "image",
            "is_migrated" : false,
            "can_edit" : false,
            "fetched_on" : 1.606743763620021E9,
            "pull_count" : 2386255,
            "collaborator_count" : 0,
            "last_updated" : "2020-11-17T01:43:05.870352Z",
            "namespace" : "hyperledger",
            "has_starred" : false,
            "star_count" : 15,
            "is_private" : false,
            "permissions" : {
              "read" : true,
              "admin" : false,
              "write" : false
            },
            "status" : 1,
            "description" : "Fabric Zookeeper docker image for Hyperledger Project",
            "is_automated" : false
          },
          "metadata__timestamp" : "2020-11-30T13:42:43.620021Z",
          "backend_version" : "0.6.0",
          "timestamp" : 1.606743763620021E9,
          "category" : "dockerhub-data",
          "classified_fields_filtered" : null,
          "updated_on" : 1.606743763620021E9,
          "metadata__updated_on" : "2020-11-30T13:42:43.620021Z",
          "tag" : "https://hub.docker.com/hyperledger/fabric-zookeeper",
          "uuid" : "150993eea55e4810371c6aa69043bbd9ae8e3cea",
          "origin" : "https://hub.docker.com/hyperledger/fabric-zookeeper",
          "search_fields" : {
            "name" : "fabric-zookeeper",
            "namespace" : "hyperledger",
            "item_id" : "1606743763.620021"
          },
          "backend_name" : "Dockerhub",
          "perceval_version" : "0.17.1"
        }`,
	}

	testSawtooth := test{
		"SawtoothTest",
		httpClientData{
			"hyperledger",
			"sawtooth-xo-tp-rust",
		},
		`{
          "data" : {
            "last_updated" : "2020-11-30T10:21:10.298838Z",
            "fetched_on" : 1.606745324657876E9,
            "collaborator_count" : 0,
            "status" : 1,
            "full_description" : "",
            "star_count" : 0,
            "is_automated" : false,
            "permissions" : {
              "write" : false,
              "read" : true,
              "admin" : false
            },
            "name" : "sawtooth-xo-tp-rust",
            "affiliation" : null,
            "namespace" : "hyperledger",
            "can_edit" : false,
            "is_private" : false,
            "description" : "",
            "pull_count" : 551,
            "has_starred" : false,
            "user" : "hyperledger",
            "repository_type" : "image",
            "is_migrated" : false
          },
          "timestamp" : 1.606745324657876E9,
          "tag" : "https://hub.docker.com/hyperledger/sawtooth-xo-tp-rust",
          "backend_name" : "Dockerhub",
          "perceval_version" : "0.17.1",
          "search_fields" : {
            "name" : "sawtooth-xo-tp-rust",
            "item_id" : "1606745324.657876",
            "namespace" : "hyperledger"
          },
          "category" : "dockerhub-data",
          "classified_fields_filtered" : null,
          "metadata__timestamp" : "2020-11-30T14:08:44.657876Z",
          "updated_on" : 1.606745324657876E9,
          "metadata__updated_on" : "2020-11-30T14:08:44.657876Z",
          "uuid" : "6f95ad322b23aec633eceb8d3239fa03d5480082",
          "origin" : "https://hub.docker.com/hyperledger/sawtooth-xo-tp-rust",
          "backend_version" : "0.6.0"
        }`,
	}

	testYocto := test{
		"YoctoTest",
		httpClientData{
			"crops",
			"yocto-eol",
		},
		`{
          "timestamp" : 1.60677117514455E9,
          "metadata__updated_on" : "2020-11-30T21:19:35.144550Z",
          "classified_fields_filtered" : null,
          "tag" : "https://hub.docker.com/crops/yocto-eol",
          "metadata__timestamp" : "2020-11-30T21:19:35.144550Z",
          "updated_on" : 1.60677117514455E9,
          "perceval_version" : "0.17.1",
          "backend_name" : "Dockerhub",
          "data" : {
            "last_updated" : "2020-07-09T03:35:44.222467Z",
            "description" : "These images contain distros that have reached EOL. They are no longer updated, tested or supported.",
            "full_description" : null,
            "star_count" : 0,
            "is_automated" : false,
            "pull_count" : 55,
            "can_edit" : false,
            "permissions" : {
              "read" : true,
              "write" : false,
              "admin" : false
            },
            "fetched_on" : 1.60677117514455E9,
            "status" : 1,
            "is_migrated" : false,
            "has_starred" : false,
            "repository_type" : "image",
            "name" : "yocto-eol",
            "collaborator_count" : 0,
            "is_private" : false,
            "user" : "crops",
            "affiliation" : null,
            "namespace" : "crops"
          },
          "category" : "dockerhub-data",
          "backend_version" : "0.6.0",
          "origin" : "https://hub.docker.com/crops/yocto-eol",
          "search_fields" : {
            "name" : "yocto-eol",
            "item_id" : "1606771175.144550",
            "namespace" : "crops"
          },
          "uuid" : "5abdb73ad0b6aa11f13193cad3ee3d40e648cbe8"
        }`,
	}

	testPrometheus := test{
		"PrometheusTest",
		httpClientData{
			"prom",
			"pushprox",
		},
		`{
         "backend_name" : "Dockerhub",
         "updated_on" : 1.599530819256249E9,
         "perceval_version" : "0.17.0",
         "data" : {
           "is_automated" : false,
           "permissions" : {
             "write" : false,
             "admin" : false,
             "read" : true
           },
           "affiliation" : null,
           "full_description" : null,
           "is_private" : false,
           "description" : "",
           "name" : "pushprox",
           "fetched_on" : 1.599530819256249E9,
           "user" : "prom",
           "can_edit" : false,
           "is_migrated" : false,
           "star_count" : 1,
           "namespace" : "prom",
           "has_starred" : false,
           "status" : 1,
           "last_updated" : "2020-06-23T07:56:26.882328Z",
           "repository_type" : "image",
           "pull_count" : 9076
         },
         "backend_version" : "0.6.0",
         "classified_fields_filtered" : null,
         "uuid" : "a59d9433c1c30e83b43ab8d5391bbfcd7f2f5edf",
         "metadata__timestamp" : "2020-09-08T02:06:59.256249Z",
         "metadata__updated_on" : "2020-09-08T02:06:59.256249Z",
         "tag" : "https://hub.docker.com/prom/pushprox",
         "category" : "dockerhub-data",
         "origin" : "https://hub.docker.com/prom/pushprox",
         "timestamp" : 1.599530819256249E9,
         "search_fields" : {
           "name" : "pushprox",
           "namespace" : "prom",
           "item_id" : "1599530819.256249"
         }
     }`,
	}

	testEdgex := test{
		"EdgexTest",
		httpClientData{
			"edgexfoundry",
			"docker-core-command-go",
		},
		`{
          "origin" : "https://hub.docker.com/edgexfoundry/docker-core-command-go",
          "category" : "dockerhub-data",
          "classified_fields_filtered" : null,
          "metadata__updated_on" : "2020-11-30T15:23:26.036942Z",
          "metadata__timestamp" : "2020-11-30T15:23:26.036942Z",
          "perceval_version" : "0.17.1",
          "uuid" : "7b61fe17d9b5741d5aba65ae77fa14195bacdde1",
          "search_fields" : {
            "item_id" : "1606749806.036942",
            "namespace" : "edgexfoundry",
            "name" : "docker-core-command-go"
          },
          "backend_name" : "Dockerhub",
          "tag" : "https://hub.docker.com/edgexfoundry/docker-core-command-go",
          "updated_on" : 1.606749806036942E9,
          "backend_version" : "0.6.0",
          "timestamp" : 1.606749806036942E9,
          "data" : {
            "fetched_on" : 1.606749806036942E9,
            "is_automated" : false,
            "affiliation" : null,
            "namespace" : "edgexfoundry",
            "status" : 1,
            "is_migrated" : false,
            "collaborator_count" : 0,
            "has_starred" : false,
            "is_private" : false,
            "star_count" : 2,
            "description" : "",
            "permissions" : {
              "read" : true,
              "write" : false,
              "admin" : false
            },
            "name" : "docker-core-command-go",
            "repository_type" : "image",
            "pull_count" : 709408,
            "full_description" : null,
            "user" : "edgexfoundry",
            "can_edit" : false,
            "last_updated" : "2020-11-18T17:41:09.216750Z"
          }
        }`,
	}

	tests = append(tests, testOnap)
	tests = append(tests, testFabric)
	tests = append(tests, testSawtooth)
	tests = append(tests, testYocto)
	tests = append(tests, testPrometheus)
	tests = append(tests, testEdgex)

	for _, tst := range tests {
		t.Run(tst.name, func(tt *testing.T) {
			expectedRaw, err := toRepositoryRaw(tst.expected)
			if err != nil {
				t.Error(err)
			}

			data, err := jsoniter.Marshal(expectedRaw.Data)
			if err != nil {
				t.Error(err)
			}

			httpClientProviderMock.On("Request",
				fmt.Sprintf("https://hub.docker.com/v2/repositories/%s/%s", tst.httpClientData.owner, tst.httpClientData.repository),
				"GET", mock.Anything, mock.Anything, mock.Anything).Return(200, data, nil)

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
			assert.Equal(tt, expectedRaw.UUID, raw.UUID)
			assert.Equal(tt, expectedRaw, *raw)
		})
	}
}

func toRepositoryRaw(b string) (RepositoryRaw, error) {
	expectedRaw := RepositoryRaw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}

func prepareObject() (*Fetcher, ESClientProvider, error) {
	httpClientProvider := http.NewClientProvider(5 * time.Second)

	params := &Params{
		Username:       "",
		Password:       "",
		BackendVersion: "0.0.1",
	}
	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
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
