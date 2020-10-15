package dads

import (
	"net"
	"net/mail"
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
	// OpenAddrRE - '<...' -> '<' (... = whitespace)
	OpenAddrRE = regexp.MustCompile(`<\s+`)
	// CloseAddrRE - '...>' -> '>' (... = whitespace)
	CloseAddrRE = regexp.MustCompile(`\s+>`)
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

// ParseAddresses - parse address string into one or more name/email pairs
func ParseAddresses(ctx *Ctx, addrs string) (emails []*mail.Address, ok bool) {
	var e error
	patterns := []string{" at ", "_at_", " en "}
	addrs = strings.TrimSpace(addrs)
	addrs = SpacesRE.ReplaceAllString(addrs, " ")
	addrs = OpenAddrRE.ReplaceAllString(addrs, "<")
	addrs = CloseAddrRE.ReplaceAllString(addrs, ">")
	for _, pattern := range patterns {
		addrs = strings.Replace(addrs, pattern, "@", -1)
	}
	emails, e = mail.ParseAddressList(addrs)
	if e != nil {
		addrs2 := strings.Replace(addrs, `"`, "", -1)
		emails, e = mail.ParseAddressList(addrs2)
		if e != nil {
			emails = []*mail.Address{}
			ary := strings.Split(addrs2, ",")
			for _, f := range ary {
				f = strings.TrimSpace(f)
				email, e := mail.ParseAddress(f)
				if e == nil {
					emails = append(emails, email)
					if ctx.Debug > 1 {
						Printf("unable to parse '%s' but '%s' parsed to %v ('%s','%s')\n", addrs, f, email, email.Name, email.Address)
					}
				}
			}
			if len(emails) == 0 {
				if ctx.Debug > 0 {
					Printf("cannot get identities: cannot read email address(es) from %s\n", addrs)
				}
				return
			}
		}
	}
	for i, obj := range emails {
		// remove leading/trailing ' "
		// skip if starts with =?
		// should we allow empty name?
		obj.Name = strings.TrimSpace(strings.Trim(obj.Name, `"'`))
		obj.Address = strings.TrimSpace(strings.Trim(obj.Address, `"'`))
		if strings.HasPrefix(obj.Name, "=?") {
			if ctx.Debug > 0 {
				Printf("clearing buggy name '%s'\n", obj.Name)
			}
			obj.Name = ""
		}
		if obj.Name == "" || obj.Name == obj.Address {
			ary := strings.Split(obj.Address, "@")
			obj.Name = ary[0]
			if ctx.Debug > 1 {
				Printf("set name '%s' based on address '%s'\n", obj.Name, obj.Address)
			}
		}
		emails[i].Name = obj.Name
		emails[i].Address = obj.Address
	}
	ok = true
	return
}
