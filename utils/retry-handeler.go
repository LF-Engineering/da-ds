package utils

import (
	"github.com/avast/retry-go"
	"time"
)

// BackOfDelay take a func and try to execute it number of times
func BackOfDelay(ex func([]BulkData) ([]byte,error), uin uint, du time.Duration, data []BulkData ) error {

	retry.DefaultAttempts = uin
	retry.DefaultDelay = du


	err := retry.Do(func() error {
		_, err := ex(data)
		return err
	},retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
		return retry.BackOffDelay(n, err, config)
	}))

	return err
}

// DelayOfCreateIndex ...
func DelayOfCreateIndex(ex func(string2 string, b []byte) ([]byte,error), uin uint, du time.Duration, index string, data []byte ) error {

	retry.DefaultAttempts = uin
	retry.DefaultDelay = du


	err := retry.Do(func() error {
		_, err := ex(index,data)
		return err
	},retry.DelayType(func(n uint, err error, config *retry.Config) time.Duration {
		return retry.BackOffDelay(n, err, config)
	}))

	return err
}
