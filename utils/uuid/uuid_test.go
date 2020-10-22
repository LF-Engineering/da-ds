package uuid

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerate(t *testing.T) {
	// Arrange
	args := []string{" abc ", "123"}

	// Act
	id, err := Generate(args...)

	// Assert
	assert.Equal(t, "18ecd81c8bb792b5c23142c89aa60d0fb2442863", id)
	assert.NoError(t, err)
}
