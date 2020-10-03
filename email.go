package dads

import (
	"net"
	"regexp"
	"strings"
	"sync"
)

var (
	// EmailRegex - regexp to match email address
	EmailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
	// emailsCache validation cache
	emailsCache = map[string]bool{}
	// emailsCacheMtx - emails validation cache mutex
	emailsCacheMtx *sync.RWMutex
)

// IsValidEmail - is email correct: len, regexp, MX domain
// uses internal cache
func IsValidEmail(email string) (valid bool) {
	l := len(email)
	if l < 3 && l > 254 {
		return
	}
	if MT {
		emailsCacheMtx.RLock()
	}
	valid, ok := emailsCache[email]
	if MT {
		emailsCacheMtx.RUnlock()
	}
	if ok {
		return
	}
	defer func() {
		if MT {
			emailsCacheMtx.Lock()
		}
		emailsCache[email] = valid
		if MT {
			emailsCacheMtx.Unlock()
		}
	}()
	if !EmailRegex.MatchString(email) {
		return
	}
	parts := strings.Split(email, "@")
	mx, err := net.LookupMX(parts[1])
	if err != nil || len(mx) == 0 {
		return
	}
	valid = true
	return
}
