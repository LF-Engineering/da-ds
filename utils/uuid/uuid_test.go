package uuid

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerate(t *testing.T) {
	type testData struct {
		args []string
		result string
	}
	// Arrange
	tests := []testData{
		{[]string{" abc ", "123"},"18ecd81c8bb792b5c23142c89aa60d0fb2442863"},
		{[]string{"scm", "Mishal\\udcc5 Pytasz"}, "789a5559fc22f398b7e18d97601c027811773121"},
		{[]string{"1483228800.0"}, "e4c0899ba951ed06781c30eab386e4e2a9cc9f60"},
	}

	for _, test := range tests {
		// Act
		id, err := Generate(test.args...)

		// Assert
		assert.Equal(t, test.result, id)
		assert.NoError(t, err)
	}
}
