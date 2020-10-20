package dockerhub

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/dockerhub/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestFetchItem(t *testing.T) {
	// Arrange
	params := &Params{
		Username:       "",
		Password:       "",
		BackendVersion: "0.0.1",
	}
	owner := "hyperledger"
	repo := "caliper"

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

	index := fmt.Sprintf("sds-%s-%s-dockerhub-raw", owner, repo)
	dockerhubRawMapping := []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}}`)

	esClientProviderMock.On("CreateIndex", index, dockerhubRawMapping).Return(nil, nil)
	esClientProviderMock.On("Add", index, mock.Anything, mock.Anything).Return([]byte("Index created"), nil)

	srv := NewFetcher(params, httpClientProviderMock, esClientProviderMock)

	// Act
	err = srv.FetchItem(owner, repo)

	// Assert
	assert.NoError(t, err)
}
