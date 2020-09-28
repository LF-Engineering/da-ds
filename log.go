package dads

import (
	"fmt"
	"time"
)

// Printf is a wrapper around Printf(...) that supports logging.
func Printf(format string, args ...interface{}) (n int, err error) {
	// Actual logging to stdout & DB
	now := time.Now()
	msg := fmt.Sprintf("%s: "+format, append([]interface{}{ToYMDHMSDate(now)}, args...)...)
	n, err = fmt.Printf("%s", msg)
	return
}
