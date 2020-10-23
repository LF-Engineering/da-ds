package dads

import (
	"regexp"
	"strings"
	"sync"
)

var (
	// GRedactedStrings - need to be global, to redact them from error logs
	GRedactedStrings map[string]struct{}
	// GRedactedMtx - guard access to this map while in MT
	GRedactedMtx *sync.RWMutex
	redactedOnce sync.Once
	// AnonymizeURLPattern - used to remove sensitive data from the url - 3rd can be a GitHub password
	AnonymizeURLPattern = regexp.MustCompile(`(^.*)(://)(.*@)(.*$)`)
)

// AddRedacted - adds redacted string
func AddRedacted(newRedacted string, useMutex bool) {
	// Initialize map & mutex once
	redactedOnce.Do(func() {
		GRedactedStrings = make(map[string]struct{})
		GRedactedMtx = &sync.RWMutex{}
	})
	if useMutex {
		GRedactedMtx.Lock()
		defer func() {
			GRedactedMtx.Unlock()
		}()
	}
	if len(newRedacted) > 3 {
		GRedactedStrings[newRedacted] = struct{}{}
	}
}

// FilterRedacted - filter out all known redacted starings
func FilterRedacted(str string) string {
	if GRedactedStrings == nil {
		return str
	}
	GRedactedMtx.RLock()
	defer func() {
		GRedactedMtx.RUnlock()
	}()
	for redacted := range GRedactedStrings {
		str = strings.Replace(str, redacted, Redacted, -1)
	}
	return str
}

// GetRedacted - get redacted
func GetRedacted() (str string) {
	if GRedactedStrings == nil {
		return "[]"
	}
	GRedactedMtx.RLock()
	defer func() {
		GRedactedMtx.RUnlock()
	}()
	str = "["
	for redacted := range GRedactedStrings {
		str += redacted + " "
	}
	str += "]"
	return
}

// AnonymizeURL - remove sensitive data from the URL
func AnonymizeURL(url string) string {
	return AnonymizeURLPattern.ReplaceAllString(url, `$1$2$4`)
}
