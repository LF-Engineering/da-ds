package utils

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLen(t *testing.T) {
	// Arrange
	body, err := ioutil.ReadFile("./test_files/activity.html")

	// Act
	count, err := GetLen("#bugzilla tr", body)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 5, count)
}
