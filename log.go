package dads

import (
	"fmt"
	"log"
	"time"
)

// Printf is a wrapper around Printf(...) that supports logging and removes redacted data.
func Printf(format string, args ...interface{}) {
	// Actual logging to stdout & DB
	now := time.Now()
	msg := FilterRedacted(fmt.Sprintf("%s: "+format, append([]interface{}{ToYMDHMSDate(now)}, args...)...))
	_, err := fmt.Printf("%s", msg)
	if err != nil {
		log.Printf("Err: %s", err.Error())
	}
}

// PrintfNoRedacted is a wrapper around Printf(...) that supports logging and don't removes redacted data
func PrintfNoRedacted(format string, args ...interface{}) {
	// Actual logging to stdout & DB
	now := time.Now()
	msg := fmt.Sprintf("%s: "+format, append([]interface{}{ToYMDHMSDate(now)}, args...)...)
	_, err := fmt.Printf("%s", msg)
	if err != nil {
		log.Printf("Err: %s", err.Error())
	}
}
