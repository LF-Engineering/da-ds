package bugzilla

import (
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/bugzilla/mocks"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
)

func TestFetchItem(t *testing.T) {
	// Arrange

	params := &Params{
		BackendVersion: "0.0.1",
		Endpoint:       "https://bugzilla.yoctoproject.org",
	}

	esClientProviderMock := &mocks.ESClientProvider{}

	httpClientProvider := utils.NewHttpClientProvider(50 * time.Second)
	srv := NewFetcher(params, httpClientProvider, esClientProviderMock)

	// Act
	_, err := srv.FetchItem(time.Now(), 3)

	// Assert
	assert.NoError(t, err)

}
