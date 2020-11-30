package dockerhub

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
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
		{"cncf", "envoy", "sds-cncf-envoy-dockerhub"},
		{"hyperledger", "explorer", "sds-hyperledger-explorer-dockerhub"},
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
          "last_updated" : "2020-11-19T11:02:07.082437Z",
          "repository_labels" : null,
          "is_event" : 0,
          "metadata__updated_on" : "2020-11-29T03:26:00.694260Z",
          "full_description_analyzed" : "",
          "tag" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "is_dockerhub_dockerhub" : 1,
          "creation_date" : "2020-11-29T03:26:00.694260Z",
          "build_on_cloud" : null,
          "metadata__backend_name" : "DockerhubEnrich",
          "origin" : "https://hub.docker.com/onap/sdnc-ueb-listener-image",
          "offset" : null,
          "user" : "onap",
          "star_count" : 0,
          "metadata__enriched_on" : "2020-11-29T03:26:01.614016Z",
          "repository_type" : "image",
          "description" : "",
          "is_automated" : false,
          "uuid" : "9bbfef1ae44f3056092e98334a6f8fbe3f64be98",
          "affiliation" : null,
          "pull_count" : 733,
          "is_private" : false,
          "metadata__timestamp" : "2020-11-29T03:26:00.694312Z",
          "id" : "sdnc-ueb-listener-image-onap",
          "metadata__filter_raw" : null,
          "status" : 1,
          "description_analyzed" : "",
          "is_docker_image" : 1,
          "metadata__version" : "0.80.0"
        }`,
	}

	tests = append(tests, testOnap)

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
			enrich, err := srv.EnrichItem(expectedRaw, expectedEnrich.MetadataEnrichedOn)
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
