package bugzillarest

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	// Arrange
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
	}
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
