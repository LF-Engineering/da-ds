package dads

import (
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// MaxPayloadPrintfLen - truncate messages longer than this
	MaxPayloadPrintfLen = 0x2000
)

var (
	memCacheMtx *sync.RWMutex
	memCache    = map[string]*MemCacheEntry{}
)

// MemCacheEntry - single cache entry
type MemCacheEntry struct {
	G string    `json:"g"` // cache tag
	B []byte    `json:"b"` // cache data
	T time.Time `json:"t"` // when cached
	E time.Time `json:"e"` // when expires
}

// MemCacheDeleteExpired - delete expired cache entries
func MemCacheDeleteExpired(ctx *Ctx) {
	t := time.Now()
	ks := []string{}
	for k, v := range memCache {
		if t.After(v.E) {
			ks = append(ks, k)
		}
	}
	if ctx.Debug > 1 {
		Printf("running MemCacheDeleteExpired - deleting %d entries\n", len(ks))
	}
	for _, k := range ks {
		delete(memCache, k)
	}
}

// MaybeMemCacheCleanup - chance of cleaning expired cache entries
func MaybeMemCacheCleanup(ctx *Ctx) {
	// chance for cache cleanup
	if rand.Intn(100) < CacheCleanupProb {
		go func() {
			if MT {
				memCacheMtx.Lock()
			}
			MemCacheDeleteExpired(ctx)
			if MT {
				memCacheMtx.Unlock()
			}
		}()
	}
}

// CacheSummary - display cache summary stats
func CacheSummary(ctx *Ctx) {
	if ctx.Debug == 0 {
		return
	}
	if ctx.Debug >= 1 {
		Printf("identity cache: %d entries\n", len(identityCache))
		Printf("enrollments cache: %d entries\n", len(rollsCache))
		Printf("identity uuids cache: %d entries\n", len(i2uCache))
		Printf("emails cache: %d entries\n", len(emailsCache))
		Printf("uuids type 1 cache: %d entries\n", len(uuidsNonEmptyCache))
		Printf("uuids type 2 cache: %d entries\n", len(uuidsAffsCache))
	}
	if ctx.Debug >= 2 {
		if len(identityCache) > 0 {
			Printf("identity cache:\n%s\n", PrintCache(identityCache))
		}
		if len(rollsCache) > 0 {
			Printf("enrollments cache:\n%s\n", PrintCache(rollsCache))
		}
		if len(i2uCache) > 0 {
			Printf("identity uuids cache:\n%s\n", PrintCache(i2uCache))
		}
		if len(emailsCache) > 0 {
			Printf("emails cache:\n%s\n", PrintCache(emailsCache))
		}
		if len(uuidsNonEmptyCache) > 0 {
			Printf("uuids type 1 cache:\n%s\n", PrintCache(uuidsNonEmptyCache))
		}
		if len(uuidsAffsCache) > 0 {
			Printf("uuids type 2 cache:\n%s\n", PrintCache(uuidsAffsCache))
		}
		PrintfNoRedacted("Redacted data: %s\n", GetRedacted())
	}
}

// BytesToStringTrunc - truncate bytes stream to no more than maxLen
func BytesToStringTrunc(data []byte, maxLen int) (str string) {
	if len(data) <= maxLen {
		return string(data)
	}
	half := maxLen >> 1
	str = string(data[:half]) + "(...)" + string(data[len(data)-half:])
	return
}

// InterfaceToStringTrunc - truncate interface representation
func InterfaceToStringTrunc(iface interface{}, maxLen int) (str string) {
	data := fmt.Sprintf("%+v", iface)
	if len(data) <= maxLen {
		return data
	}
	half := maxLen >> 1
	str = data[:half] + "(...)" + data[len(data)-half:]
	return
}

// PrintCache - pretty print cache entries
func PrintCache(iCache interface{}) (s string) {
	cache := reflect.ValueOf(iCache)
	if cache.Kind() != reflect.Map {
		Printf("Error: not a map %+v\n", iCache)
		return
	}
	t := false
	for i, k := range cache.MapKeys() {
		v := cache.MapIndex(k)
		if !t {
			s += fmt.Sprintf("type: map[%T]%T\n", k.Interface(), v.Interface())
			t = true
		}
		s += fmt.Sprintf("%d) %+v: %+v\n", i+1, k.Interface(), v.Interface())
	}
	if s != "" {
		s = s[:len(s)-1]
	}
	return
}

// KeysOnly - return a corresponding interface contining only keys
func KeysOnly(i interface{}) (o map[string]interface{}) {
	if i == nil {
		return
	}
	is, ok := i.(map[string]interface{})
	if !ok {
		return
	}
	o = make(map[string]interface{})
	for k, v := range is {
		o[k] = KeysOnly(v)
	}
	return
}

// DumpKeys - dump interface structure, but only keys, no values
func DumpKeys(i interface{}) string {
	return strings.Replace(fmt.Sprintf("%v", KeysOnly(i)), "map[]", "", -1)
}

// PartitionString - partition a string to [pre-sep, sep, post-sep]
func PartitionString(s string, sep string) [3]string {
	parts := strings.SplitN(s, sep, 2)
	if len(parts) == 1 {
		return [3]string{parts[0], "", ""}
	}
	return [3]string{parts[0], sep, parts[1]}
}

// Dig interface for array of keys
func Dig(iface interface{}, keys []string, fatal, silent bool) (v interface{}, ok bool) {
	miss := false
	defer func() {
		if !ok && fatal {
			Fatalf("cannot dig %+v in %s", keys, DumpKeys(iface))
		}
	}()
	item, o := iface.(map[string]interface{})
	if !o {
		if !silent {
			Printf("Interface cannot be parsed: %+v\n", iface)
		}
		return
	}
	last := len(keys) - 1
	for i, key := range keys {
		var o bool
		if i < last {
			item, o = item[key].(map[string]interface{})
		} else {
			v, o = item[key]
		}
		if !o {
			if !silent {
				Printf("dig %+v, current: %s, %d/%d failed\n", keys, key, i+1, last+1)
			}
			miss = true
			break
		}
	}
	ok = !miss
	return
}

// DeepSet - set deep property of non-type decoded interface
func DeepSet(m interface{}, ks []string, v interface{}, create bool) (err error) {
	c, ok := m.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot access %v as a string map", m)
		return
	}
	last := len(ks) - 1
	for i, k := range ks {
		if i < last {
			obj, ok := c[k]
			if !ok {
				if create {
					c[k] = make(map[string]interface{})
					obj = c[k]
				} else {
					err = fmt.Errorf("cannot access #%d key %s from %v, all keys %v", i+1, k, DumpKeys(c), ks)
					return
				}
			}
			c, ok = obj.(map[string]interface{})
			if !ok {
				err = fmt.Errorf("cannot access %v as a string map, #%d key %s, all keys %v", c, i+1, k, ks)
				return
			}
			continue
		}
		c[k] = v
	}
	return
}

// StringToBool - convert string value to boolean value
// returns false for anything that was parsed as false, zero, empty etc:
// f, F, false, False, fALSe, 0, "", 0.00
// else returns true
func StringToBool(v string) bool {
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}
	f, err := strconv.ParseFloat(v, 64)
	if err == nil {
		return f != 0.0
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		return i != 0
	}
	return true
}

// NoSSLVerify - turn off SSL validation
func NoSSLVerify() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
}

// EnsurePath - craete archive directory (and all necessary parents as well)
func EnsurePath(path string) (string, error) {
	ary := strings.Split(path, "/")
	nonEmpty := []string{}
	for i, dir := range ary {
		if i > 0 && dir == "" {
			continue
		}
		nonEmpty = append(nonEmpty, dir)
	}
	path = strings.Join(nonEmpty, "/")
	return path, os.MkdirAll(path, 0755)
}

// Base64EncodeCookies - encode cookies array (strings) to base64 stream of bytes
func Base64EncodeCookies(cookies []string) (enc []byte) {
	last := len(cookies) - 1
	for i, cookie := range cookies {
		b := []byte(base64.StdEncoding.EncodeToString([]byte(cookie)))
		enc = append(enc, b...)
		if i != last {
			enc = append(enc, []byte("#")...)
		}
	}
	// Printf("Base64EncodeCookies(%d,%+v) --> %s\n", len(cookies), cookies, string(enc))
	return
}

// Base64DecodeCookies - decode cookies stored as stream of bytes to array of strings
func Base64DecodeCookies(enc []byte) (cookies []string, err error) {
	ary := bytes.Split(enc, []byte("#"))
	for _, item := range ary {
		var s []byte
		s, err = base64.StdEncoding.DecodeString(string(item))
		if err != nil {
			return
		}
		if len(s) > 0 {
			cookies = append(cookies, string(s))
		}
	}
	// Printf("Base64DecodeCookies(%s) --> %d,%+v\n", string(enc), len(cookies), cookies)
	return
}

// CookieToString - convert cookie to string
func CookieToString(c *http.Cookie) (s string) {
	// Other properties (skipped because login works without them)
	/*
	   Path       string
	   Domain     string
	   Expires    time.Time
	   RawExpires string
	   MaxAge   int
	   Secure   bool
	   HttpOnly bool
	   Raw      string
	   Unparsed []stringo
	*/
	if c.Name == "" && c.Value == "" {
		return
	}
	s = c.Name + "===" + c.Value
	// Printf("cookie %+v ----> %s\n", c, s)
	return
}

// StringToCookie - convert string to cookie
func StringToCookie(s string) (c *http.Cookie) {
	ary := strings.Split(s, "===")
	if len(ary) < 2 {
		return
	}
	c = &http.Cookie{Name: ary[0], Value: ary[1]}
	// Printf("cookie string %s ----> %+v\n", s, c)
	return
}

// RequestNoRetry - wrapper to do any HTTP request
// jsonStatuses - set of status code ranges to be parsed as JSONs
// errorStatuses - specify status value ranges for which we should return error
// okStatuses - specify status value ranges for which we should return error (only taken into account if not empty)
func RequestNoRetry(
	ctx *Ctx,
	url, method string,
	headers map[string]string,
	payload []byte,
	cookies []string,
	jsonStatuses, errorStatuses, okStatuses map[[2]int]struct{},
) (result interface{}, status int, isJSON bool, outCookies []string, err error) {
	var (
		payloadBody *bytes.Reader
		req         *http.Request
	)
	if len(payload) > 0 {
		payloadBody = bytes.NewReader(payload)
		req, err = http.NewRequest(method, url, payloadBody)
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
		err = fmt.Errorf("new request error:%+v for method:%s url:%s payload:%s", err, method, url, sPayload)
		return
	}
	for _, cookieStr := range cookies {
		cookie := StringToCookie(cookieStr)
		req.AddCookie(cookie)
	}
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
		err = fmt.Errorf("do request error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, sPayload)
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
		err = fmt.Errorf("read request body error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, sPayload)
		return
	}
	_ = resp.Body.Close()
	for _, cookie := range resp.Cookies() {
		outCookies = append(outCookies, CookieToString(cookie))
	}
	status = resp.StatusCode
	hit := false
	for r := range jsonStatuses {
		if status >= r[0] && status <= r[1] {
			hit = true
			break
		}
	}
	if hit {
		err = jsoniter.Unmarshal(body, &result)
		if err != nil {
			sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
			sBody := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
			err = fmt.Errorf("unmarshall request error:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s", err, method, url, headers, status, sPayload, sBody)
			return
		}
		isJSON = true
	} else {
		result = body
	}
	hit = false
	for r := range errorStatuses {
		if status >= r[0] && status <= r[1] {
			hit = true
			break
		}
	}
	if hit {
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
		sBody := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
		err = fmt.Errorf("status error:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s result:%+v", err, method, url, headers, status, sPayload, sBody, result)
	}
	if len(okStatuses) > 0 {
		hit = false
		for r := range okStatuses {
			if status >= r[0] && status <= r[1] {
				hit = true
				break
			}
		}
		if !hit {
			sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
			sBody := BytesToStringTrunc(payload, MaxPayloadPrintfLen)
			err = fmt.Errorf("status not success:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s result:%+v", err, method, url, headers, status, sPayload, sBody, result)
		}
	}
	return
}

// Request - wrapper around RequestNoRetry supporting retries
func Request(
	ctx *Ctx,
	url, method string,
	headers map[string]string,
	payload []byte,
	cookies []string,
	jsonStatuses, errorStatuses, okStatuses map[[2]int]struct{},
	retryRequest bool,
	cacheFor *time.Duration,
	skipInDryRun bool,
) (result interface{}, status int, outCookies []string, err error) {
	if skipInDryRun && ctx.DryRun {
		if ctx.Debug > 0 {
			Printf("dry-run: %s.%s(#h=%d,pl=%d,cks=%d) skipped in dry-run mode\n", method, url, len(headers), len(payload), len(cookies))
		}
		return
	}
	var isJSON bool
	if cacheFor != nil && !ctx.NoCache {
		b := []byte(method + url + fmt.Sprintf("%+v", headers))
		b = append(b, payload...)
		b = append(b, []byte(strings.Join(cookies, "==="))...)
		hash := sha1.New()
		_, e := hash.Write(b)
		if e == nil {
			hsh := hex.EncodeToString(hash.Sum(nil))
			cached, ok := GetL2Cache(ctx, hsh)
			if ok {
				// cache entry is 'status:isJson:b64cookies:data
				ary := bytes.Split(cached, []byte(":"))
				if len(ary) > 3 {
					var e error
					status, e = strconv.Atoi(string(ary[0]))
					if e == nil {
						var iJSON int
						iJSON, e = strconv.Atoi(string(ary[1]))
						if e == nil {
							outCookies, e = Base64DecodeCookies(ary[2])
							if e == nil {
								resData := bytes.Join(ary[3:], []byte(":"))
								if iJSON == 0 {
									result = resData
									return
								}
								var r interface{}
								e = jsoniter.Unmarshal(resData, &r)
								if e == nil {
									result = r
									return
								}
							}
						}
					}
				}
			}
			cacheDuration := *cacheFor
			defer func() {
				if err != nil {
					return
				}
				// cache entry is 'status:isJson:b64cookies:data
				b64cookies := Base64EncodeCookies(outCookies)
				data := []byte(fmt.Sprintf("%d:", status))
				if isJSON {
					bts, e := jsoniter.Marshal(result)
					if e != nil {
						return
					}
					data = append(data, []byte("1:")...)
					data = append(data, b64cookies...)
					data = append(data, []byte(":")...)
					data = append(data, bts...)
					tag := FilterRedacted(fmt.Sprintf("%s.%s(#h=%d,pl=%d,cks=%d) -> sts=%d,js=1,resp=%d,cks=%d", method, url, len(headers), len(payload), len(cookies), status, len(bts), len(outCookies)))
					SetL2Cache(ctx, hsh, tag, data, cacheDuration)
					return
				}
				data = append(data, []byte("0:")...)
				data = append(data, b64cookies...)
				data = append(data, []byte(":")...)
				data = append(data, result.([]byte)...)
				tag := FilterRedacted(fmt.Sprintf("%s.%s(#h=%d,pl=%d,cks=%d) -> sts=%d,js=0,resp=%d,cks=%d", method, url, len(headers), len(payload), len(cookies), status, len(result.([]byte)), len(outCookies)))
				SetL2Cache(ctx, hsh, tag, data, cacheDuration)
				return
			}()
		}
	}
	if !retryRequest {
		result, status, isJSON, outCookies, err = RequestNoRetry(ctx, url, method, headers, payload, cookies, jsonStatuses, errorStatuses, okStatuses)
		return
	}
	retry := 0
	for {
		result, status, isJSON, outCookies, err = RequestNoRetry(ctx, url, method, headers, payload, cookies, jsonStatuses, errorStatuses, okStatuses)
		info := func() (inf string) {
			inf = fmt.Sprintf("%s.%s:%s=%d", method, url, BytesToStringTrunc(payload, MaxPayloadPrintfLen), status)
			if ctx.Debug > 1 {
				inf += fmt.Sprintf(" error: %+v", err)
			}
			return
		}
		if err != nil {
			retry++
			if retry > ctx.Retry {
				Printf("%s failed after %d retries\n", info(), retry)
				return
			}
			seconds := (retry + 1) * (retry + 1)
			Printf("will do #%d retry of %s after %d seconds\n", retry, info(), seconds)
			time.Sleep(time.Duration(seconds) * time.Second)
			Printf("retrying #%d retry of %s after %d seconds\n", retry, info(), seconds)
			continue
		}
		if retry > 0 {
			Printf("#%d retry of %s succeeded\n", retry, info())
		}
		return
	}
}
