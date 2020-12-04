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

