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
	// EmailReplacer - replacer for some email buggy characters
	EmailReplacer = strings.NewReplacer(" at ", "@", " AT ", "@", " At ", "@", " dot ", ".", " DOT ", ".", " Dot ", ".", "<", "", ">", "")
	// emailsCache validation cache
	emailsCache = map[string]bool{}
	// emailsCacheMtx - emails validation cache mutex
	emailsCacheMtx *sync.RWMutex
	// OpenAddrRE - '<...' -> '<' (... = whitespace)
	OpenAddrRE = regexp.MustCompile(`<\s+`)
	// CloseAddrRE - '...>' -> '>' (... = whitespace)
	CloseAddrRE = regexp.MustCompile(`\s+>`)
	// WhiteSpace - one or more whitespace characters
	WhiteSpace = regexp.MustCompile(`\s+`)
)

// IsValidDomain - is MX domain valid?
// uses internal cache
func IsValidDomain(domain string) (valid bool) {
	l := len(domain)
	if l < 4 && l > 254 {
		return
	}
	if MT {
		emailsCacheMtx.RLock()
	}
	valid, ok := emailsCache[domain]
	if MT {
		emailsCacheMtx.RUnlock()
	}
	if ok {
		// fmt.Printf("domain cache hit: '%s' -> %v\n", domain, valid)
		return
	}
	defer func() {
		if MT {
			emailsCacheMtx.Lock()
		}
		emailsCache[domain] = valid
		if MT {
			emailsCacheMtx.Unlock()
		}
	}()
	mx, err := net.LookupMX(domain)
	if err != nil || len(mx) == 0 {
		return
	}
	valid = true
	return
}

// IsValidEmail - is email correct: len, regexp, MX domain
// uses internal cache
func IsValidEmail(email string, validateDomain bool) (valid bool) {
	l := len(email)
	if l < 6 && l > 254 {
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
	email = WhiteSpace.ReplaceAllString(email, " ")
	email = strings.TrimSpace(EmailReplacer.Replace(email))
	email = strings.Split(email, " ")[0]
	if !EmailRegex.MatchString(email) {
		return
	}
	if validateDomain {
		parts := strings.Split(email, "@")
		if len(parts) <= 1 || !IsValidDomain(parts[1]) {
			return
		}
	}
	valid = true
	return
}

// ParseAddresses - parse address string into one or more name/email pairs
func ParseAddresses(ctx *Ctx, addrs string, maxAddrs int) (emails []*mail.Address, ok bool) {
	defer func() {
		if len(emails) > maxAddrs {
			emails = emails[:maxAddrs]
		}
	}()
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
					if len(emails) >= maxAddrs {
						break
					}
					continue
				}
				a := strings.Split(f, "@")
				if len(a) == 3 {
					// name@domain <name@domain> -> ['name', 'domain <name', 'domain>']
					// name@domain name@domain -> ['name', 'domain name', 'domain']
					name := a[0]
					domain := strings.Replace(a[2], ">", "", -1)
					nf := name + " <" + name + "@" + domain + ">"
					email, e := mail.ParseAddress(nf)
					if e == nil {
						emails = append(emails, email)
						if ctx.Debug > 1 {
							Printf("unable to parse '%s' but '%s' -> '%s' parsed to %v ('%s','%s')\n", addrs, f, nf, email, email.Name, email.Address)
						}
						if len(emails) > maxAddrs {
							break
						}
					}
				}
			}
			if len(emails) == 0 {
				if ctx.Debug > 1 {
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
