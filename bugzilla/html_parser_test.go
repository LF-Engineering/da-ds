package bugzilla

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetActivityLen(t *testing.T) {
	// Arrange
	body, err := ioutil.ReadFile("./mocks/activity.html")

	// Act
	count, _, err := GetActivityLen("#bugzilla-body tr", body)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 6, count)
}
