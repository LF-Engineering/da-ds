package bugzilla

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestManager(t *testing.T) {

 m := NewManager("https://bugzilla.yoctoproject.org",
	 "0.0.1","0.0.1",
 	false, false,
	 "http://localhost:9200", "elastic","changeme", "sds-test-yocto-bugzilla" )
 err := m.Sync()
	// Assert
	assert.NoError(t, err)

}

// 	url := fmt.Sprintf("%s/buglist.cgi?chfieldfrom=%s&ctype=csv&limit=%v&order=changeddate", f.Endpoint, "2020-01-01%12:00:00", limit)