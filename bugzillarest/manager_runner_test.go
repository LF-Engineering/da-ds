package bugzillarest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	//x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	//now := time.Now().AddDate(0,0,-1)
	//from, _ := time.Parse("2006-01-02 15:04:05", "2020-12-09 18:00:00")
	var param Param

	param.EndPoint = "https://bugs.dpdk.org/"
	param.ShConnStr = "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test"
	param.FetcherBackendVersion = "0.1.0"
	param.EnricherBackendVersion = "0.1.0"
	param.Fetch = true
	param.Enrich = true
	param.ESUrl = "http://elastic:changeme@127.0.0.1:9200"
	param.EsIndex = "sds-data-plane-development-kit-dpdk-bugzillarest"
	param.Project = "dpdk"
	param.FetchSize = 5
	param.EnrichSize = 5
	param.Retries = 3
	param.Delay = 2 * time.Second
	param.GapURL = "http://127.0.0.1:8200"

	m, err := NewManager(param)
	if err != nil {
		t.Error(err)
	}
	err = m.Sync()
	// Assert
	assert.NoError(t, err)
}
