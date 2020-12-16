package bugzillarest

import (
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type HTTPClientProviderTest interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}



func TestBugzillaRestFetchItem(t *testing.T){
	httpClientProvider := utils.NewHTTPClientProvider(60*time.Second)
	srv := NewFetcher(httpClientProvider)
	err := srv.FetchItem()

	assert.NoError(t, err)
}
