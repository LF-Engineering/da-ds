package bugzilla

import (
"testing"
"time"
"github.com/stretchr/testify/assert"
)
func TestManager(t *testing.T) {
	x := "lfinsights_test:urpialruvCadcyakhokcect2@tcp(lfinsights-db.clctyzfo4svp.us-west-2.rds.amazonaws.com)/lfinsights_test?charset=utf8"
	//now := time.Now().AddDate(0,0,-1)
	from, _ := time.Parse("2006-01-02 15:04:05", "2020-12-21 09:00:00")

	m, err := NewManager("https://bugzilla.yoctoproject.org",
		x,
		"0.0.1",
		"0.0.1",
		true,
		false,
		"http://localhost:9200",
		"elastic",
		"changeme",
		"sds-test-yocto-bugzilla",
		&from,
		"yocto",
		5,
		5,
		3,
		2 * time.Second,
		"http://localhost:9200")
	err = m.Sync()
	// Assert
	assert.NoError(t, err)
}
