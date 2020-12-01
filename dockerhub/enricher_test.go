package dockerhub

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
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

/*func TestGetPreviouslyFetchedData(t *testing.T) {
	srv, err := prepareEnrichObject()
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	repos := []*Repository{
		{"cncf", "envoy", "", "sds-cncf-envoy-dockerhub"},
		{"hyperledger", "explorer", "", "sds-hyperledger-explorer-dockerhub"},
	}

	lastDate := "2019-10-20T18:07:47.729125Z"
	d, err := time.Parse(time.RFC3339, lastDate)

	for _, repo := range repos {
		raws, err := srv.GetPreviouslyFetchedDataItem(repo, nil, &d, false)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}

		t.Logf("results: %v", raws.Hits.Hits)
	}
}
*/
func TestEnrichItem(t *testing.T) {
	// Arrange
	esClientProviderMock := &mocks.ESClientProvider{}

	type test struct {
		name      string
		fetchData string
		expected  string
	}

	tests := []test{}

	testOnap := test{
		"OnapTest",
		`{
		  "tag" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "uuid" : "9bbfef1ae44f3056092e98334a6f8fbe3f64be98",
          "perceval_version" : "0.17.1",
          "category" : "dockerhub-data",
          "updated_on" : 1.60662036069426E9,
          "metadata__timestamp" : "2020-11-29T03:26:00.694312Z",
          "backend_name" : "DockerHub",
          "search_fields" : {
            "item_id" : "1606620360.69426",
            "name" : "sdnc-ueb-listener-image",
            "namespace" : "onap"
          },
          "backend_version" : "0.6.0",
          "origin" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "metadata__updated_on" : "2020-11-29T03:26:00.694260Z",
          "data" : {
            "repository_type" : "image",
            "collaborator_count" : 0,
            "has_starred" : false,
            "full_description" : "",
            "namespace" : "onap",
            "status" : 1,
            "pull_count" : 733,
            "is_automated" : false,
            "can_edit" : false,
            "name" : "sdnc-ueb-listener-image",
            "description" : "",
            "fetched_on" : 1.60662036069426E9,
            "last_updated" : "2020-11-19T11:02:07.082437Z",
            "is_private" : false,
            "user" : "onap",
            "is_migrated" : false,
            "star_count" : 0,
            "affiliation" : null,
            "permissions" : {
              "write" : false,
              "admin" : false,
              "read" : true
            }
          },
          "classified_fields_filtered" : null,
          "timestamp" : 1.606620360694312E9
}
`,
		`{
          "origin" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "user" : "onap",
          "tag" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "offset" : null,
          "is_docker_image" : 0,
          "star_count" : 0,
          "is_private" : false,
          "repository_type" : "image",
          "affiliation" : null,
          "is_dockerhub_dockerhub" : 1,
          "metadata__updated_on" : "2020-11-29T03:26:00.694260Z",
          "metadata__timestamp" : "2020-11-29T03:26:00.694312Z",
          "metadata__filter_raw" : null,
          "full_description_analyzed" : "",
          "metadata__version" : "0.80.0",
          "description" : "",
          "status" : 1,
          "build_on_cloud" : null,
          "pull_count" : 733,
          "metadata__enriched_on" : "2020-11-30T16:24:16.436699Z",
          "metadata__backend_name" : "DockerhubEnrich",
          "is_automated" : false,
          "repository_labels" : null,
          "is_event" : 1,
          "uuid" : "9bbfef1ae44f3056092e98334a6f8fbe3f64be98",
          "id" : "sdnc-ueb-listener-image-onap",
          "creation_date" : "2020-11-29T03:26:00.694260Z",
          "last_updated" : "2020-11-19T11:02:07.082437Z",
          "description_analyzed" : ""
        }`,
	}

	testFabric := test{
		"FabricTest",
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
          "uuid" : "6cd1f5d3faed10cbc9de853c372f06c036460a5e",
          "origin" : "https://hub.docker.com/hyperledger/fabric-zookeeper",
          "search_fields" : {
            "name" : "fabric-zookeeper",
            "namespace" : "hyperledger",
            "item_id" : "1606743763.620021"
          },
          "backend_name" : "Dockerhub",
          "perceval_version" : "0.17.1"
        }
`,
		`{
          "is_private" : false,
          "metadata__backend_name" : "DockerhubEnrich",
          "origin" : "https://hub.docker.com/hyperledger/fabric-zookeeper",
          "is_automated" : false,
          "description" : "Fabric Zookeeper docker image for Hyperledger Project",
          "project" : "fabric",
          "metadata__timestamp" : "2020-11-30T13:42:43.620021Z",
          "uuid" : "6cd1f5d3faed10cbc9de853c372f06c036460a5e",
          "repository_type" : "image",
          "build_on_cloud" : null,
          "description_analyzed" : "Fabric Zookeeper docker image for Hyperledger Project",
          "full_description_analyzed" : "",
          "affiliation" : null,
          "creation_date" : "2020-11-30T13:42:43.620021Z",
          "metadata__enriched_on" : "2020-11-30T13:42:45.016479Z",
          "id" : "fabric-zookeeper-hyperledger",
          "tag" : "https://hub.docker.com/hyperledger/fabric-zookeeper",
          "pull_count" : 2386255,
          "repository_labels" : null,
          "last_updated" : "2020-11-17T01:43:05.870352Z",
          "offset" : null,
          "metadata__filter_raw" : null,
          "is_event" : 1,
          "project_ts" : 0,
          "is_docker_image" : 0,
          "is_dockerhub_dockerhub" : 1,
          "metadata__version" : "0.80.0",
          "metadata__updated_on" : "2020-11-30T13:42:43.620021Z",
          "user" : "hyperledger",
          "star_count" : 15,
          "status" : 1
        }`,
	}

	testSawtooth := test{
		"SawtoothTest",
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
          "uuid" : "37e85829947b85fb216a668810dea6b20aa6c365",
          "origin" : "https://hub.docker.com/hyperledger/sawtooth-xo-tp-rust",
          "backend_version" : "0.6.0"
        }`,
		`{
          "is_private" : false,
          "metadata__backend_name" : "DockerhubEnrich",
          "is_automated" : false,
          "origin" : "https://hub.docker.com/hyperledger/sawtooth-xo-tp-rust",
          "description" : "",
          "project" : "sawtooth",
          "metadata__timestamp" : "2020-11-30T14:08:44.657876Z",
          "uuid" : "37e85829947b85fb216a668810dea6b20aa6c365",
          "build_on_cloud" : null,
          "repository_type" : "image",
          "description_analyzed" : "",
          "affiliation" : null,
          "creation_date" : "2020-11-30T14:08:44.657876Z",
          "full_description_analyzed" : "",
          "metadata__enriched_on" : "2020-11-30T14:08:46.066578Z",
          "tag" : "https://hub.docker.com/hyperledger/sawtooth-xo-tp-rust",
          "id" : "sawtooth-xo-tp-rust-hyperledger",
          "pull_count" : 551,
          "repository_labels" : null,
          "last_updated" : "2020-11-30T10:21:10.298838Z",
          "offset" : null,
          "metadata__filter_raw" : null,
          "is_event" : 1,
          "project_ts" : 0,
          "is_docker_image" : 0,
          "is_dockerhub_dockerhub" : 1,
          "metadata__version" : "0.80.0",
          "metadata__updated_on" : "2020-11-30T14:08:44.657876Z",
          "user" : "hyperledger",
          "star_count" : 0,
          "status" : 1
        }`,
	}

	tests = append(tests, testOnap)
	tests = append(tests, testFabric)
	tests = append(tests, testSawtooth)

	for _, tst := range tests {
		t.Run(tst.name, func(tt *testing.T) {
			expectedRaw, err := toRepositoryRaw(tst.fetchData)
			if err != nil {
				t.Error(err)
			}

			expectedEnrich, err := toRepositoryEnrich(tst.expected)
			if err != nil {
				t.Error(err)
			}

			params := &Params{
				BackendVersion: expectedEnrich.BackendVersion,
			}

			srv := NewEnricher(params.BackendVersion, esClientProviderMock)

			// Act
			enrich, err := srv.EnrichItem(expectedRaw, expectedEnrich.Project, expectedEnrich.MetadataEnrichedOn)
			if err != nil {
				tt.Error(err)
			}
			// Assert
			assert.Equal(tt, expectedEnrich.MetadataUpdatedOn.String(), enrich.MetadataUpdatedOn.String())
			assert.Equal(tt, expectedEnrich.LastUpdated, enrich.LastUpdated)
			assert.Equal(tt, expectedEnrich.CreationDate.String(), enrich.CreationDate.String())
			assert.Equal(tt, expectedEnrich.MetadataEnrichedOn.String(), enrich.MetadataEnrichedOn.String())
			assert.Equal(tt, expectedEnrich, *enrich)
		})
	}
}

func toRepositoryEnrich(b string) (RepositoryEnrich, error) {
	expectedEnrich := RepositoryEnrich{}
	err := json.Unmarshal([]byte(b), &expectedEnrich)
	return expectedEnrich, err
}
