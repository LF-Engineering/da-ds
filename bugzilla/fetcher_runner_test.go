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
	from, er := time.Parse("2006-01-02 15:04:05", "2020-12-01 16:54:21")
	if er!=nil{
	}
	now := time.Now()
	limit := 25
	result := 25
	data := make([]*BugRaw, 0)
	for result == limit {
		bugs, err := srv.FetchItem(from, limit, now)
		if err != nil {
			fmt.Println(err)
		}

		from, er = time.Parse("2006-01-02 15:04:05", bugs[len(bugs)-1].ChangedAt)
		result = len(bugs)

		if result < 2 {
			bugs = nil
		}else {
			bugs = bugs[1:result]
			data = append(data, bugs...)
		}
	}

	fmt.Println("mmmm")
	fmt.Println(len(data))

	// Assert
	assert.NoError(t, nil)


}
