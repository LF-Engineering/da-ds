package bugzillarest

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/LF-Engineering/da-ds/bugzillarest/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	// Arrange
	httpClientMock := &mocks.HTTPClientProvider{}
	esClientMock := &mocks.ESClientProvider{}
	query := map[string]interface{}{"query": map[string]interface{}{"term": map[string]interface{}{"id": map[string]string{"value": "enrich"}}}}
	val := &TopHits{Hits: Hits{Hits: []NestedHits(nil)}}
	esClientMock.On("Get", "-last-action-date-cache", query, val).Run(func(args mock.Arguments) {

	}).Return(nil)

	fetchQuery := map[string]interface{}{"query": map[string]interface{}{"term": map[string]interface{}{"id": map[string]string{"value": "fetch"}}}}
	esClientMock.On("Get", "-last-action-date-cache", fetchQuery, val).Run(func(args mock.Arguments) {

	}).Return(nil)

	params := &MgrParams{
		EndPoint:               "",
		ShConnStr:              "",
		FetcherBackendVersion:  "",
		EnricherBackendVersion: "",
		Fetch:                  true,
		Enrich:                 true,
		ESUrl:                  "",
		EsUser:                 "",
		EsPassword:             "",
		FromDate:               nil,
		Project:                "",
		FetchSize:              1000,
		EnrichSize:             1000,
		Retries:                uint(3),
		Delay:                  time.Second * 2,
		GapURL:                 "",
		ESClientProvider:       esClientMock,
	}

	fetcher := NewFetcher(&FetcherParams{Endpoint: params.EndPoint, BackendVersion: params.FetcherBackendVersion}, httpClientMock, esClientMock)
	params.Fetcher = fetcher

	affiliationsClientMock := &mocks.AffiliationClient{}

	enricher := NewEnricher(&EnricherParams{BackendVersion: params.EnricherBackendVersion, Project: params.Project}, affiliationsClientMock)

	params.Enricher = enricher

	auth0ClientMock := &mocks.Auth0ClientProvider{}
	params.Auth0ClientProvider = auth0ClientMock
	mgr, err := NewManager(params)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	// Act
	err = mgr.Sync()

	// Assert
	assert.NoError(t, err)

}
