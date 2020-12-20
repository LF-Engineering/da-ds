package bugzillarest

import (
"testing"
"time"
"github.com/stretchr/testify/assert"
)
func TestManager(t *testing.T) {
	x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	//now := time.Now().AddDate(0,0,-1)
	from, _ := time.Parse("2006-01-02 15:04:05", "2020-12-09 18:00:00")

	m, err := NewManager("https://bugs.dpdk.org/",
		x,
		"0.0.1",
		"0.0.1",
		true,
		false,
		"http://localhost:9200",
		"elastic",
		"changeme",
		"sds-test-data-plane-development-kit",
		&from,
		"tdd",
		2,
		2)
	if err != nil {
		t.Error(err)
	}
	err = m.Sync()
	// Assert
	assert.NoError(t, err)
}
