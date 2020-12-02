package bugzilla

import (
	"fmt"
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

	httpClientProvider := utils.NewHTTPClientProvider(50 * time.Second)
	srv := NewFetcher(params, httpClientProvider, esClientProviderMock)

	// Act
	now := time.Now()
	bugs, err := srv.FetchItem(now, 3, now)

	fmt.Println(len(bugs))
	// Assert
	assert.NoError(t, err)

}
