package bugzillarest

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type HTTPClientProviderTest interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}


func TestBugzillaRestFetchItem(t *testing.T){

	url := "https://bugs.dpdk.org/rest/bug"

	from, err := time.Parse("2006-01-02 15:04:05", "2020-12-10 03:00:00")
	if err != nil {
		fmt.Println(err)
	}
	d := from.Format("2006-01-02T15:04:05")
	httpClientProvider := utils.NewHTTPClientProvider(60*time.Second)
	srv := NewFetcher(httpClientProvider)
	data, err := srv.FetchAll(url , d , "2", "2", from)

	fmt.Println("data")
	fmt.Println(len(data))

	assert.NoError(t, err)
}
