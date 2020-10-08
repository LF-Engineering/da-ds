package dads

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
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
	if ctx.Debug > 0 {
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
		Printf("identity cache:\n%s\n", PrintCache(identityCache))
		Printf("enrollments cache:\n%s\n", PrintCache(rollsCache))
		Printf("identity uuids cache:\n%s\n", PrintCache(i2uCache))
		Printf("emails cache:\n%s\n", PrintCache(emailsCache))
		Printf("uuids type 1 cache:\n%s\n", PrintCache(uuidsNonEmptyCache))
		Printf("uuids type 2 cache:\n%s\n", PrintCache(uuidsAffsCache))
	}
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

// RequestNoRetry - wrapper to do any HTTP request
// jsonStatuses - set of status code ranges to be parsed as JSONs
// errorStatuses - specify status value ranges for which we should return error
// okStatuses - specify status value ranges for which we should return error (only taken into account if not empty)
func RequestNoRetry(
	ctx *Ctx,
	url, method string,
	headers map[string]string,
	payload []byte,
	jsonStatuses, errorStatuses, okStatuses map[[2]int]struct{},
) (result interface{}, status int, isJSON bool, err error) {
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
		err = fmt.Errorf("new request error:%+v for method:%s url:%s payload:%s", err, method, url, string(payload))
		return
	}
	for header, value := range headers {
		req.Header.Set(header, value)
	}
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("do request error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, string(payload))
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("read request body error:%+v for method:%s url:%s headers:%v payload:%s", err, method, url, headers, string(payload))
		return
	}
	_ = resp.Body.Close()
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
			err = fmt.Errorf("unmarshall request error:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s", err, method, url, headers, status, string(payload), string(body))
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
		err = fmt.Errorf("status error:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s result:%+v", err, method, url, headers, status, string(payload), string(body), result)
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
			err = fmt.Errorf("status not success:%+v for method:%s url:%s headers:%v status:%d payload:%s body:%s result:%+v", err, method, url, headers, status, string(payload), string(body), result)
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
	jsonStatuses, errorStatuses, okStatuses map[[2]int]struct{},
	retryRequest bool,
	cacheFor *time.Duration,
	skipInDryRun bool,
) (result interface{}, status int, err error) {
	if skipInDryRun && ctx.DryRun {
		return
	}
	var isJSON bool
	if cacheFor != nil && !ctx.NoCache {
		b := []byte(method + url + fmt.Sprintf("%+v", headers))
		b = append(b, payload...)
		hash := sha1.New()
		_, e := hash.Write(b)
		if e == nil {
			hsh := hex.EncodeToString(hash.Sum(nil))
			cached, ok := GetL2Cache(ctx, hsh)
			if ok {
				ary := bytes.Split(cached, []byte(":"))
				if len(ary) > 2 {
					var e error
					status, e = strconv.Atoi(string(ary[0]))
					if e == nil {
						var iJSON int
						iJSON, e = strconv.Atoi(string(ary[1]))
						if e == nil {
							resData := bytes.Join(ary[2:], []byte(":"))
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
			cacheDuration := *cacheFor
			defer func() {
				if err != nil {
					return
				}
				data := []byte(fmt.Sprintf("%d:", status))
				if isJSON {
					bts, e := jsoniter.Marshal(result)
					if e != nil {
						return
					}
					data = append(data, []byte("1:")...)
					data = append(data, bts...)
					tag := fmt.Sprintf("%s.%s(#h=%d,pl=%d) -> sts=%d,js=1,resp=%d", method, url, len(headers), len(payload), status, len(bts))
					SetL2Cache(ctx, hsh, tag, data, cacheDuration)
					return
				}
				data = append(data, []byte("0:")...)
				data = append(data, result.([]byte)...)
				tag := fmt.Sprintf("%s.%s(#h=%d,pl=%d) -> sts=%d,js=0,resp=%d", method, url, len(headers), len(payload), status, len(result.([]byte)))
				SetL2Cache(ctx, hsh, tag, data, cacheDuration)
				return
			}()
		}
	}
	if !retryRequest {
		result, status, isJSON, err = RequestNoRetry(ctx, url, method, headers, payload, jsonStatuses, errorStatuses, okStatuses)
		return
	}
	retry := 0
	for {
		result, status, isJSON, err = RequestNoRetry(ctx, url, method, headers, payload, jsonStatuses, errorStatuses, okStatuses)
		info := func() (inf string) {
			inf = fmt.Sprintf("%s.%s:%s=%d", method, url, string(payload), status)
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
