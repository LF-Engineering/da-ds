package bugzilla

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestManager(t *testing.T) {

 m := NewManager("https://bugzilla.yoctoproject.org",
	 "0.0.1","0.0.1",
 	false, true,
	 "http://localhost:9200", "elastic","changeme",
	 "sds-test-yocto-bugzilla", nil , 50 * time.Second, "yocto" )
 err := m.Sync()
	// Assert
	assert.NoError(t, err)

}

