package bugzilla

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

	param.EndPoint = "https://bugzilla.yoctoproject.org"
	param.ShConnStr = "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test"
	param.FetcherBackendVersion = "0.1.0"
	param.EnricherBackendVersion = "0.1.0"
	param.Fetch = true
	param.Enrich = true
	param.ESUrl = "http://elastic:changeme@127.0.0.1:9200"
	param.EsIndex = "sds-yocto-bugzilla-for-merge"
	param.Project = "yocto"
	param.FetchSize = 20
	param.EnrichSize = 20
	param.Retries = 3
	param.Delay = 2 * time.Second
	param.GapURL = "http://127.0.0.1:8200/failure"

	param.ESCacheURL = "https://elastic:vrMLx5VZiIh0mJzxP1fDHkDo@8e6c295de3a04fc382e8e3390475f670.us-west-2.aws.found.io:9243"
	param.ESCacheUsername = "elastic"
	param.ESCachePassword = "vrMLx5VZiIh0mJzxP1fDHkDo"

	param.AuthGrantType = "client-credentials"

	param.AuthClientID = "hPRfTdGVfUEhm92v4AewVdZSuoY5Pr2v"
	param.AuthClientSecret = "m--atB4qrXS9LNg0u6AqcLKK5mtQyX6JReBeaPUpNrpyHSyUDnInLfe-agxYauPQ"
	param.AuthAudience = "https://api-gw.dev.platform.linuxfoundation.org/"

	param.AuthURL = "https://linuxfoundation-dev.auth0.com/"
	param.Environment = "test"

	m, err := NewManager(param)
	err = m.Sync()
	// Assert
	assert.NoError(t, err)
}
