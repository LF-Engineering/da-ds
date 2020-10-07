package dads

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var (
	esCacheMtx *sync.RWMutex
)

// ESCacheEntry - single cache entry
type ESCacheEntry struct {
	K string    `json:"k"` // cache key
	G string    `json:"g"` // cache tag
	B []byte    `json:"b"` // cache data
	T time.Time `json:"t"` // when cached
	E time.Time `json:"e"` // when expires
}

// ESCacheGet - get value from cache
func ESCacheGet(ctx *Ctx, key string) (entry *ESCacheEntry, ok bool) {
	data := `{"query":{"term":{"k.keyword":{"value": "` + JSONEscape(key) + `"}}}}`
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := fmt.Sprintf("%s/dads_cache/_search", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		Printf("New request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		return
	}
	type R struct {
		H struct {
			H []struct {
				S ESCacheEntry `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	var r R
	err = jsoniter.Unmarshal(body, &r)
	if err != nil {
		Printf("Unmarshal error: %+v\n", err)
		return
	}
	if len(r.H.H) == 0 {
		return
	}
	entry = &(r.H.H[0].S)
	ok = true
	return
}

// ESCacheSet - set cache value
func ESCacheSet(ctx *Ctx, key string, entry *ESCacheEntry) {
	entry.K = key
	payloadBytes, err := jsoniter.Marshal(entry)
	if err != nil {
		Printf("json %+v marshal error: %+v\n", entry, err)
		return
	}
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := fmt.Sprintf("%s/dads_cache/_doc?refresh=true", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		data := string(payloadBytes)
		Printf("New request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		data := string(payloadBytes)
		Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 201 {
		data := string(payloadBytes)
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			return
		}
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		return
	}
	return
}

// ESCacheDelete - delete cache key
func ESCacheDelete(ctx *Ctx, key string) {
	data := `{"query":{"term":{"k.keyword":{"value": "` + JSONEscape(key) + `"}}}}`
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := fmt.Sprintf("%s/dads_cache/_delete_by_query?conflicts=proceed&refresh=true", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		Printf("New request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			return
		}
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		return
	}
}

// ESCacheDeleteExpired - delete expired cache entries
func ESCacheDeleteExpired(ctx *Ctx) {
	data := `{"query":{"range":{"e":{"lte": "now"}}}}`
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := fmt.Sprintf("%s/dads_cache/_delete_by_query?conflicts=proceed&refresh=true", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		Printf("New request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			return
		}
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		return
	}
}

// GetESCache - get value from cache - thread safe and support expiration
func GetESCache(ctx *Ctx, k string) (b []byte, ok bool) {
	defer MaybeMemCacheCleanup()
	defer MaybeESCacheCleanup(ctx)
	if MT {
		esCacheMtx.RLock()
	}
	entry, ok := ESCacheGet(ctx, k)
	if MT {
		esCacheMtx.RUnlock()
	}
	if !ok {
		if ctx.Debug > 0 {
			Printf("GetESCache(%s): miss\n", k)
		}
		return
	}
	if time.Now().After(entry.E) {
		ok = false
		if MT {
			esCacheMtx.Lock()
		}
		ESCacheDelete(ctx, k)
		if MT {
			esCacheMtx.Unlock()
		}
		if ctx.Debug > 0 {
			Printf("GetESCache(%s,%s): expired\n", k, entry.G)
		}
		return
	}
	b = entry.B
	if ctx.Debug > 0 {
		Printf("GetESCache(%s,%s): hit\n", k, entry.G)
	}
	return
}

// SetESCache - set cache value, expiration date and handles multithreading etc
func SetESCache(ctx *Ctx, k, tg string, b []byte, expires time.Duration) {
	defer MaybeMemCacheCleanup()
	defer MaybeESCacheCleanup(ctx)
	t := time.Now()
	e := t.Add(expires)
	if MT {
		esCacheMtx.RLock()
	}
	_, ok := ESCacheGet(ctx, k)
	if MT {
		esCacheMtx.RUnlock()
	}
	if ok {
		if MT {
			esCacheMtx.Lock()
		}
		ESCacheDelete(ctx, k)
		ESCacheSet(ctx, k, &ESCacheEntry{B: b, T: t, E: e, G: tg})
		if MT {
			esCacheMtx.Unlock()
		}
		if ctx.Debug > 0 {
			Printf("SetESCache(%s,%s): replaced\n", k, tg)
		}
	} else {
		if MT {
			esCacheMtx.Lock()
		}
		ESCacheSet(ctx, k, &ESCacheEntry{B: b, T: t, E: e, G: tg})
		if MT {
			esCacheMtx.Unlock()
		}
		if ctx.Debug > 0 {
			Printf("SetESCache(%s,%s): added\n", k, tg)
		}
	}
}

// MaybeESCacheCleanup - 10% chance of cleaning expired cache entries
func MaybeESCacheCleanup(ctx *Ctx) {
	// 10% chance for cache cleanup
	t := time.Now()
	if t.Second()%10 == 0 {
		go func() {
			if MT {
				esCacheMtx.Lock()
			}
			ESCacheDeleteExpired(ctx)
			if MT {
				esCacheMtx.Unlock()
			}
			if ctx.Debug > 0 {
				Printf("ContributorsCache: deleted expired items\n")
			}
		}()
	}
}

// CreateESCache - creates dads_cache index needed for caching
func CreateESCache(ctx *Ctx) {
	// Create index, ignore if exists (see status 400 is not in error statuses)
	_, _, err := Request(ctx, ctx.ESURL+"/dads_cache", Put, nil, []byte{}, nil, map[[2]int]struct{}{{401, 599}: {}}, nil, false, nil)
	FatalOnError(err)
}

// SendToElastic - send items to ElasticSearch
func SendToElastic(ctx *Ctx, ds DS, raw bool, key string, items []interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk uploading %d items\n", ds.Name(), raw, key, len(items))
	}
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex + "/_bulk?refresh=" + BulkRefreshMode
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex + "/_bulk?refresh=" + BulkRefreshMode
	}
	// {"index":{"_id":"uuid"}}
	payloads := []byte{}
	newLine := []byte("\n")
	var (
		doc []byte
		hdr []byte
	)
	for _, item := range items {
		doc, err = jsoniter.Marshal(item)
		if err != nil {
			return
		}
		id, ok := item.(map[string]interface{})[key].(string)
		if !ok {
			err = fmt.Errorf("missing %s property in %+v", key, DumpKeys(item))
			return
		}
		hdr = []byte(`{"index":{"_id":"` + id + "\"}}\n")
		payloads = append(payloads, hdr...)
		payloads = append(payloads, doc...)
		payloads = append(payloads, newLine...)
	}
	_, _, err = Request(
		ctx,
		url,
		Post,
		map[string]string{"Content-Type": "application/x-ndjson"},
		payloads,
		nil,                                 // JSON statuses
		map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 400-599
		nil,                                 // OK statuses
		true,
		nil,
	)
	if err == nil {
		if ctx.Debug > 0 {
			Printf("%s(raw=%v,key=%s) ES bulk upload saved %d items\n", ds.Name(), raw, key, len(items))
		}
		return
	}
	Printf("%s(raw=%v,key=%s) ES bulk upload of %d items failed, falling back to one-by-one mode\n", ds.Name(), raw, key, len(items))
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk upload error: %+v\n", ds.Name(), raw, key, err)
	}
	err = nil
	// Fallback to one-by-one inserts
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex + "/_doc/"
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex + "/_doc/"
	}
	headers := map[string]string{"Content-Type": "application/json"}
	for _, item := range items {
		doc, _ = jsoniter.Marshal(item)
		id, _ := item.(map[string]interface{})[key].(string)
		_, _, err = Request(
			ctx,
			url+id,
			Put,
			headers,
			doc,
			nil,                                 // JSON statuses
			map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 400-599
			map[[2]int]struct{}{{200, 201}: {}}, // OK statuses: 200-201
			true,
			nil,
		)
	}
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk upload saved %d items (in non-bulk mode)\n", ds.Name(), raw, key, len(items))
	}
	return
}

// GetLastUpdate - get last update date from ElasticSearch
func GetLastUpdate(ctx *Ctx, ds DS, raw bool) (lastUpdate *time.Time) {
	// curl -s -XPOST -H 'Content-type: application/json' '${URL}/index/_search?size=0' -d '{"aggs":{"m":{"max":{"field":"date_field"}}}}' | jq -r '.aggregations.m.value_as_string'
	dateField := JSONEscape(ds.DateField(ctx))
	originField := JSONEscape(ds.OriginField(ctx))
	origin := JSONEscape(ds.Origin(ctx))
	var payloadBytes []byte
	if ds.ResumeNeedsOrigin(ctx) {
		payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + originField + `":"` + origin + `"}}}},"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
	} else {
		payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
	}
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex + "/_search?size=0"
	}
	if ctx.Debug > 0 {
		Printf("resume from date query raw=%v: %s\n", raw, string(payloadBytes))
	}
	method := Post
	resp, _, err := Request(
		ctx,
		url,
		method,
		map[string]string{"Content-Type": "application/json"}, // headers
		payloadBytes,                        // payload
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200, 404
		true,
		nil,
	)
	FatalOnError(err)
	type resultStruct struct {
		Aggs struct {
			M struct {
				Str string `json:"value_as_string"`
			} `json:"m"`
		} `json:"aggregations"`
	}
	var res resultStruct
	err = jsoniter.Unmarshal(resp.([]byte), &res)
	if err != nil {
		Printf("resume from date JSON decode error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	if res.Aggs.M.Str != "" {
		var tm time.Time
		tm, err = TimeParseAny(res.Aggs.M.Str)
		if err != nil {
			Printf("resume from date decode aggregations error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		lastUpdate = &tm
	}
	return
}

// GetLastOffset - get last offset from ElasticSearch
func GetLastOffset(ctx *Ctx, ds DS, raw bool) (offset float64) {
	offset = -1.0
	// curl -s -XPOST -H 'Content-type: application/json' '${URL}/index/_search?size=0' -d '{"aggs":{"m":{"max":{"field":"offset_field"}}}}' | jq -r '.aggregations.m.value'
	offsetField := JSONEscape(ds.OffsetField(ctx))
	originField := JSONEscape(ds.OffsetField(ctx))
	origin := JSONEscape(ds.Origin(ctx))
	var payloadBytes []byte
	if ds.ResumeNeedsOrigin(ctx) {
		payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + originField + `":"` + origin + `"}}}},"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
	} else {
		payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
	}
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex + "/_search?size=0"
	}
	if ctx.Debug > 0 {
		Printf("resume from offset query raw=%v: %s\n", raw, string(payloadBytes))
	}
	method := Post
	resp, _, err := Request(
		ctx,
		url,
		method,
		map[string]string{"Content-Type": "application/json"}, // headers
		payloadBytes,                        // payload
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200, 404
		true,
		nil,
	)
	FatalOnError(err)
	type resultStruct struct {
		Aggs struct {
			M struct {
				Int *float64 `json:"value,omitempty"`
			} `json:"m"`
		} `json:"aggregations"`
	}
	var res = resultStruct{}
	err = jsoniter.Unmarshal(resp.([]byte), &res)
	if err != nil {
		Printf("resume from offset JSON decode error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	if res.Aggs.M.Int != nil {
		offset = *res.Aggs.M.Int
	}
	return
}
