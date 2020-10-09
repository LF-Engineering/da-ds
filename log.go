package dads

import (
	"fmt"
	"time"
)

// Printf is a wrapper around Printf(...) that supports logging and removes redacted data.
func Printf(format string, args ...interface{}) (n int, err error) {
	// Actual logging to stdout & DB
	now := time.Now()
	msg := FilterRedacted(fmt.Sprintf("%s: "+format, append([]interface{}{ToYMDHMSDate(now)}, args...)...))
	n, err = fmt.Printf("%s", msg)
	return
}

// PrintfNoRedacted is a wrapper around Printf(...) that supports logging and don't removes redacted data
func PrintfNoRedacted(format string, args ...interface{}) (n int, err error) {
	// Actual logging to stdout & DB
	now := time.Now()
	msg := fmt.Sprintf("%s: "+format, append([]interface{}{ToYMDHMSDate(now)}, args...)...)
	n, err = fmt.Printf("%s", msg)
	return
}
