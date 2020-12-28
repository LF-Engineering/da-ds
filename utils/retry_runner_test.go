package utils

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)
var count = 0
func sum(a int, b int) error {
	x := a + b
	fmt.Println(x)
	count++
	if count > 3 {
		return nil
	}
	return errors.New("rrrrr")
}


func TestRetry(t *testing.T) {

	err := BackOfDelay(sum, 5, 3 * time.Second)

	assert.NoError(t, err)
}
