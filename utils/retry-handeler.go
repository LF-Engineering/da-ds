package utils

import (
	"time"

	"github.com/avast/retry-go"
)

// DelayOfCreateIndex ...
func DelayOfCreateIndex(ex func(str string, b []byte) ([]byte, error), uin uint, du time.Duration, index string, data []byte) error {

	retry.DefaultAttempts = uin
	retry.DefaultDelay = du

	err := retry.Do(func() error {
		_, err := ex(index, data)
		return err
	}, retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
		return retry.BackOffDelay(n, err, config)
	}))

	return err
}
