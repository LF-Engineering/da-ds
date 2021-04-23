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
	"regexp"
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
		Printf("parse date cache: %d entries\n", len(parseDateCache))
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
		if len(parseDateCache) > 0 {
			Printf("parse date cache:\n%s\n", PrintCache(parseDateCache))
		}
		PrintfNoRedacted("Redacted data: %s\n", GetRedacted())
	}
}

// StringTrunc - truncate string to no more than maxLen
func StringTrunc(data string, maxLen int, addLenInfo bool) (str string) {
	lenInfo := ""
	if addLenInfo {
		lenInfo = "(" + strconv.Itoa(len(data)) + "): "
	}
	if len(data) <= maxLen {
		return lenInfo + data
	}
	half := maxLen >> 1
	str = lenInfo + data[:half] + "(...)" + data[len(data)-half:]
	return
}

// IndexAt - index of substring starting at a given position
func IndexAt(s, sep string, n int) int {
	idx := strings.Index(s[n:], sep)
	if idx > -1 {
		idx += n
	}
	return idx
}

// MatchGroups - return regular expression matching groups as a map
func MatchGroups(re *regexp.Regexp, arg string) (result map[string]string) {
	match := re.FindStringSubmatch(arg)
	result = make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && i <= len(match) {
			result[name] = match[i]
		}
	}
	return
}

// MatchGroupsArray - return regular expression matching groups as a map
func MatchGroupsArray(re *regexp.Regexp, arg string) (result map[string][]string) {
	match := re.FindAllStringSubmatch(arg, -1)
	//Printf("match(%d,%d): %+v\n", len(match), len(re.SubexpNames()), match)
	result = make(map[string][]string)
	names := re.SubexpNames()
	names = names[1:]
	for idx, m := range match {
		if idx == 0 {
			for i, name := range names {
				result[name] = []string{m[i+1]}
			}
			continue
		}
		for i, name := range names {
			ary, _ := result[name]
			result[name] = append(ary, m[i+1])
		}
	}
	return
}

// BytesToStringTrunc - truncate bytes stream to no more than maxLen
func BytesToStringTrunc(data []byte, maxLen int, addLenInfo bool) (str string) {
	lenInfo := ""
	if addLenInfo {
		lenInfo = "(" + strconv.Itoa(len(data)) + "): "
	}
	if len(data) <= maxLen {
		return lenInfo + string(data)
	}
	half := maxLen >> 1
	str = lenInfo + string(data[:half]) + "(...)" + string(data[len(data)-half:])
	return
}

// InterfaceToStringTrunc - truncate interface representation
func InterfaceToStringTrunc(iface interface{}, maxLen int, addLenInfo bool) (str string) {
	data := fmt.Sprintf("%+v", iface)
	lenInfo := ""
	if addLenInfo {
		lenInfo = "(" + strconv.Itoa(len(data)) + "): "
	}
	if len(data) <= maxLen {
		return lenInfo + data
	}
	half := maxLen >> 1
	str = "(" + strconv.Itoa(len(data)) + "): " + data[:half] + "(...)" + data[len(data)-half:]
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

// PreviewOnly - return a corresponding interface with preview values
func PreviewOnly(i interface{}, l int) (o interface{}) {
	if i == nil {
		return
	}
	is, ok := i.(map[string]interface{})
	if !ok {
		str := InterfaceToStringTrunc(i, l, false)
		str = strings.Replace(str, "\n", " ", -1)
		o = str
		return
	}
	iface := make(map[string]interface{})
	for k, v := range is {
		iface[k] = PreviewOnly(v, l)
	}
	o = iface
	return
}

// DumpPreview - dump interface structure, keys and truncated values preview
func DumpPreview(i interface{}, l int) string {
	return strings.Replace(fmt.Sprintf("%v", PreviewOnly(i, l)), "map[]", "", -1)
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
	if v == "no" || v == "n" {
		return false
	}
	return true
}

// NoSSLVerify - turn off SSL validation
func NoSSLVerify() {
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
}

// EnsurePath - craete archive directory (and all necessary parents as well)
// if noLastDir is set, then skip creating the last directory in the path
func EnsurePath(path string, noLastDir bool) (string, error) {
	ary := strings.Split(path, "/")
	nonEmpty := []string{}
	for i, dir := range ary {
		if i > 0 && dir == "" {
			continue
		}
		nonEmpty = append(nonEmpty, dir)
	}
	path = strings.Join(nonEmpty, "/")
	var createPath string
	if noLastDir {
		createPath = strings.Join(nonEmpty[:len(nonEmpty)-1], "/")
	} else {
		createPath = path
	}
	return path, os.MkdirAll(createPath, 0755)
}

// Base64EncodeHeaders - encode headers to base64 stream of bytes
func Base64EncodeHeaders(headers map[string][]string) (enc []byte) {
	var err error
	enc, err = jsoniter.Marshal(headers)
	if err != nil {
		return
	}
	// Printf("Base64EncodeHeaders.1(%+v) --> %s\n", headers, string(enc))
	enc = []byte(base64.StdEncoding.EncodeToString(enc))
	// Printf("Base64EncodeHeaders.2(%+v) --> %s\n", headers, string(enc))
	return
}

// Base64DecodeHeaders - decode headers stored as stream of bytes to map of string arrays
func Base64DecodeHeaders(enc []byte) (headers map[string][]string, err error) {
	var bts []byte
	bts, err = base64.StdEncoding.DecodeString(string(enc))
	if err != nil {
		return
	}
	// Printf("Base64DecodeHeaders.1(%s) --> %+v\n", string(enc), string(bts))
	var result map[string]interface{}
	err = jsoniter.Unmarshal(bts, &result)
	// Printf("Base64DecodeHeaders.2(%s) --> %+v,%v\n", string(bts), result, err)
	headers = make(map[string][]string)
	for k, v := range result {
		ary, ok := v.([]interface{})
		if !ok {
			continue
		}
		sAry := []string{}
		for _, v := range ary {
			vs, ok := v.(string)
			if ok {
				sAry = append(sAry, vs)
			}
		}
		headers[k] = sAry
	}
	// Printf("Base64DecodeHeaders.3 --> %+v\n", headers)
	return
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
	jsonStatuses, errorStatuses, okStatuses, cacheStatuses map[[2]int]struct{},
) (result interface{}, status int, isJSON bool, outCookies []string, outHeaders map[string][]string, cache bool, err error) {
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
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
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
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
		err = fmt.Errorf("do request error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, sPayload)
		if strings.Contains(err.Error(), "socket: too many open files") {
			Printf("too many open socets detected, sleeping for 3 seconds\n")
			time.Sleep(time.Duration(3) * time.Second)
		}
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
		err = fmt.Errorf("read request body error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, sPayload)
		return
	}
	_ = resp.Body.Close()
	for _, cookie := range resp.Cookies() {
		outCookies = append(outCookies, CookieToString(cookie))
	}
	outHeaders = resp.Header
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
			sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
			sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
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
		sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
		sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
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
			sPayload := BytesToStringTrunc(payload, MaxPayloadPrintfLen, true)
			sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
			err = fmt.Errorf("status not success:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s result:%+v", err, method, url, headers, status, sPayload, sBody, result)
		}
	}
	if err == nil {
		for r := range cacheStatuses {
			if status >= r[0] && status <= r[1] {
				cache = true
				break
			}
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
	jsonStatuses, errorStatuses, okStatuses, cacheStatuses map[[2]int]struct{},
	retryRequest bool,
	cacheFor *time.Duration,
	skipInDryRun bool,
) (result interface{}, status int, outCookies []string, outHeaders map[string][]string, err error) {
	if skipInDryRun && ctx.DryRun {
		if ctx.Debug > 0 {
			Printf("dry-run: %s.%s(#h=%d,pl=%d,cks=%d) skipped in dry-run mode\n", method, url, len(headers), len(payload), len(cookies))
		}
		return
	}
	var (
		isJSON bool
		cache  bool
	)
	// fmt.Printf("url=%s method=%s headers=%+v payload=%+v cookies=%+v\n", url, method, headers, payload, cookies)
	if cacheFor != nil && !ctx.NoCache {
		// cacheKey is hash(method,url,headers,payload,cookies
		b := []byte(method + url + fmt.Sprintf("%+v", headers))
		b = append(b, payload...)
		b = append(b, []byte(strings.Join(cookies, "==="))...)
		hash := sha1.New()
		_, e := hash.Write(b)
		if e == nil {
			hsh := hex.EncodeToString(hash.Sum(nil))
			cached, ok := GetL2Cache(ctx, hsh)
			if ok {
				// cache entry is 'status:isJson:b64cookies:headers:data
				ary := bytes.Split(cached, []byte(":"))
				if len(ary) >= 5 {
					var e error
					status, e = strconv.Atoi(string(ary[0]))
					if e == nil {
						var iJSON int
						iJSON, e = strconv.Atoi(string(ary[1]))
						if e == nil {
							outCookies, e = Base64DecodeCookies(ary[2])
							if e == nil {
								outHeaders, e = Base64DecodeHeaders(ary[3])
								if e == nil {
									resData := bytes.Join(ary[4:], []byte(":"))
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
			}
			cacheDuration := *cacheFor
			defer func() {
				if err != nil || !cache {
					return
				}
				// cache entry is 'status:isJson:b64cookies:headers:data
				b64cookies := Base64EncodeCookies(outCookies)
				b64headers := Base64EncodeHeaders(outHeaders)
				data := []byte(fmt.Sprintf("%d:", status))
				if isJSON {
					bts, e := jsoniter.Marshal(result)
					if e != nil {
						return
					}
					data = append(data, []byte("1:")...)
					data = append(data, b64cookies...)
					data = append(data, []byte(":")...)
					data = append(data, b64headers...)
					data = append(data, []byte(":")...)
					data = append(data, bts...)
					tag := FilterRedacted(fmt.Sprintf("%s.%s(#h=%d,pl=%d,cks=%d) -> sts=%d,js=1,resp=%d,cks=%d,hdrs=%d", method, url, len(headers), len(payload), len(cookies), status, len(bts), len(outCookies), len(outHeaders)))
					SetL2Cache(ctx, hsh, tag, data, cacheDuration)
					return
				}
				data = append(data, []byte("0:")...)
				data = append(data, b64cookies...)
				data = append(data, []byte(":")...)
				data = append(data, b64headers...)
				data = append(data, []byte(":")...)
				data = append(data, result.([]byte)...)
				tag := FilterRedacted(fmt.Sprintf("%s.%s(#h=%d,pl=%d,cks=%d) -> sts=%d,js=0,resp=%d,cks=%d,hdrs=%d", method, url, len(headers), len(payload), len(cookies), status, len(result.([]byte)), len(outCookies), len(outHeaders)))
				SetL2Cache(ctx, hsh, tag, data, cacheDuration)
				return
			}()
		}
	}
	if !retryRequest {
		result, status, isJSON, outCookies, outHeaders, cache, err = RequestNoRetry(ctx, url, method, headers, payload, cookies, jsonStatuses, errorStatuses, okStatuses, cacheStatuses)
		return
	}
	retry := 0
	for {
		result, status, isJSON, outCookies, outHeaders, cache, err = RequestNoRetry(ctx, url, method, headers, payload, cookies, jsonStatuses, errorStatuses, okStatuses, cacheStatuses)
		info := func() (inf string) {
			inf = fmt.Sprintf("%s.%s:%s=%d", method, url, BytesToStringTrunc(payload, MaxPayloadPrintfLen, true), status)
			if ctx.Debug > 1 {
				inf += fmt.Sprintf(" error: %+v", err)
			} else if err != nil {
				inf += fmt.Sprintf(" error: %+v", StringTrunc(err.Error(), MaxPayloadPrintfLen, true))
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
