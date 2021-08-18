package dads

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	urlLib "net/url"

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
		sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, sBody)
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
		sEntry := Nil
		if entry != nil {
			sEntry = InterfaceToStringTrunc(*entry, MaxPayloadPrintfLen, true)
		}
		Printf("json %s marshal error: %+v\n", sEntry, err)
		return
	}
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := fmt.Sprintf("%s/dads_cache/_doc?refresh=true", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		sData := BytesToStringTrunc(payloadBytes, MaxPayloadPrintfLen, true)
		Printf("New request error: %+v for %s url: %s, data: %s\n", err, method, url, sData)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		sData := BytesToStringTrunc(payloadBytes, MaxPayloadPrintfLen, true)
		Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, sData)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 201 {
		sData := BytesToStringTrunc(payloadBytes, MaxPayloadPrintfLen, true)
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, sData)
			return
		}
		sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, sData, resp.StatusCode, sBody)
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
		sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, sBody)
		return
	}
}

// ESCacheDeleteExpired - delete expired cache entries
func ESCacheDeleteExpired(ctx *Ctx) {
	if ctx.Debug > 1 {
		Printf("running ESCacheDeleteExpired\n")
	}
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
		sBody := BytesToStringTrunc(body, MaxPayloadPrintfLen, true)
		Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, sBody)
		return
	}
}

// GetESCache - get value from cache - thread safe and support expiration
func GetESCache(ctx *Ctx, k string) (b []byte, tg string, expires time.Time, ok bool) {
	defer MaybeESCacheCleanup(ctx)
	if MT {
		esCacheMtx.RLock()
	}
	entry, ok := ESCacheGet(ctx, k)
	if MT {
		esCacheMtx.RUnlock()
	}
	if !ok {
		if ctx.Debug > 1 {
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
		if ctx.Debug > 1 {
			Printf("GetESCache(%s,%s): expired %v\n", k, entry.G, entry.E)
		}
		return
	}
	b = entry.B
	tg = entry.G
	expires = entry.E
	if ctx.Debug > 1 {
		Printf("GetESCache(%s,%s): hit (%v)\n", k, tg, expires)
	}
	return
}

// GetL2Cache - get value from cache - thread safe and support expiration
func GetL2Cache(ctx *Ctx, k string) (b []byte, ok bool) {
	defer MaybeMemCacheCleanup(ctx)
	if MT {
		memCacheMtx.RLock()
	}
	entry, ok := memCache[k]
	if MT {
		memCacheMtx.RUnlock()
	}
	if !ok {
		if ctx.Debug > 1 {
			Printf("GetL2Cache(%s): miss\n", k)
		}
		var (
			g string
			e time.Time
		)
		b, g, e, ok = GetESCache(ctx, k)
		if ok {
			t := time.Now()
			if MT {
				memCacheMtx.Lock()
			}
			memCache[k] = &MemCacheEntry{G: g, B: b, T: t, E: e}
			if MT {
				memCacheMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("GetL2Cache(%s,%s): L2 hit (%v)\n", k, g, e)
			}
		}
		return
	}
	if time.Now().After(entry.E) {
		ok = false
		if MT {
			memCacheMtx.Lock()
		}
		delete(memCache, k)
		if MT {
			memCacheMtx.Unlock()
		}
		if ctx.Debug > 1 {
			Printf("GetL2Cache(%s,%s): expired %v\n", k, entry.G, entry.E)
		}
		var (
			g string
			e time.Time
		)
		b, g, e, ok = GetESCache(ctx, k)
		if ok {
			t := time.Now()
			if MT {
				memCacheMtx.Lock()
			}
			memCache[k] = &MemCacheEntry{G: g, B: b, T: t, E: e}
			if MT {
				memCacheMtx.Unlock()
			}
			if ctx.Debug > 1 {
				Printf("GetL2Cache(%s,%s): L2 hit (%v)\n", k, g, e)
			}
		}
		return
	}
	b = entry.B
	if ctx.Debug > 1 {
		Printf("GetL2Cache(%s,%s): hit (%v)\n", k, entry.G, entry.E)
	}
	return
}

// SetESCache - set cache value, expiration date and handles multithreading etc
func SetESCache(ctx *Ctx, k, tg string, b []byte, expires time.Duration) {
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
		if ctx.Debug > 1 {
			Printf("SetESCache(%s,%s): replaced (%v)\n", k, tg, e)
		}
	} else {
		if MT {
			esCacheMtx.Lock()
		}
		ESCacheSet(ctx, k, &ESCacheEntry{B: b, T: t, E: e, G: tg})
		if MT {
			esCacheMtx.Unlock()
		}
		if ctx.Debug > 1 {
			Printf("SetESCache(%s,%s): added (%v)\n", k, tg, e)
		}
	}
}

// SetL2Cache - set cache value, expiration date and handles multithreading etc
func SetL2Cache(ctx *Ctx, k, tg string, b []byte, expires time.Duration) {
	defer MaybeMemCacheCleanup(ctx)
	SetESCache(ctx, k, tg, b, expires)
	t := time.Now()
	e := t.Add(expires)
	if MT {
		memCacheMtx.Lock()
	}
	_, ok := memCache[k]
	memCache[k] = &MemCacheEntry{G: tg, B: b, T: t, E: e}
	if MT {
		memCacheMtx.Unlock()
	}
	if ok {
		if ctx.Debug > 1 {
			Printf("SetL2Cache(%s,%s): replaced (%v)\n", k, tg, e)
		}
		return
	}
	if ctx.Debug > 1 {
		Printf("SetL2Cache(%s,%s): added (%v)\n", k, tg, e)
	}
}

// MaybeESCacheCleanup - chance of cleaning expired cache entries
func MaybeESCacheCleanup(ctx *Ctx) {
	// chance for cache cleanup
	if rand.Intn(100) < CacheCleanupProb {
		if MT {
			go func() {
				esCacheMtx.Lock()
				ESCacheDeleteExpired(ctx)
				esCacheMtx.Unlock()
				if ctx.Debug > 2 {
					Printf("ContributorsCache: deleted expired items\n")
				}
			}()
			return
		}
		ESCacheDeleteExpired(ctx)
	}
}

// CreateESCache - creates dads_cache index needed for caching
func CreateESCache(ctx *Ctx) {
	// Create index, ignore if exists (see status 400 is not in error statuses)
	_, _, _, _, err := Request(ctx, ctx.ESURL+"/dads_cache", Put, nil, []byte{}, []string{}, nil, map[[2]int]struct{}{{401, 599}: {}}, nil, nil, false, nil, false)
	FatalOnError(err)
}

// SendToElastic - send items to ElasticSearch
func SendToElastic(ctx *Ctx, ds DS, raw bool, key string, items []interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk uploading %d items\n", ds.Name(), raw, key, len(items))
	}
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex + "/_bulk?refresh=" + BulkRefreshMode + "&wait_for_active_shards=" + BulkWaitForActiveShardsMode
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex + "/_bulk?refresh=" + BulkRefreshMode + "&wait_for_active_shards=" + BulkWaitForActiveShardsMode
	}
	// {"index":{"_id":"uuid"}}
	payloads := []byte{}
	newLine := []byte("\n")
	var (
		doc    []byte
		hdr    []byte
		status int
	)
	for _, item := range items {
		doc, err = jsoniter.Marshal(item)
		if err != nil {
			return
		}
		iID, ok := item.(map[string]interface{})[key]
		if !ok {
			err = fmt.Errorf("missing %s property in %+v", key, DumpKeys(item))
			return
		}
		id, ok := iID.(string)
		if !ok {
			err = fmt.Errorf("%s property is %T not string %+v", key, iID, iID)
			return
		}
		hdr = []byte(`{"index":{"_id":"` + id + "\"}}\n")
		payloads = append(payloads, hdr...)
		payloads = append(payloads, doc...)
		payloads = append(payloads, newLine...)
	}
	var result interface{}
	result, status, _, _, err = Request(
		ctx,
		url,
		Post,
		map[string]string{"Content-Type": "application/x-ndjson"},
		payloads,
		[]string{},
		map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
		map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 400-599
		nil,                                 // OK statuses
		nil,                                 // Cache statuses
		true,                                // retry
		nil,                                 // cache duration
		true,                                // skip in dry-run mode
	)
	resp, ok := result.(map[string]interface{})
	if ok {
		ers, ok := resp["errors"].(bool)
		if ok && ers {
			msg := InterfaceToStringTrunc(result, 1000, true)
			Printf("bulk upload failed: %s\n", msg)
			err = fmt.Errorf("%s", msg)
		}
	}
	if err == nil {
		if ctx.Debug > 0 {
			Printf("%s(raw=%v,key=%s) ES bulk upload saved %d items\n", ds.Name(), raw, key, len(items))
		}
		return
	}
	var sResp string
	bResp, ok := result.([]byte)
	if ok {
		sResp = BytesToStringTrunc(bResp, MaxPayloadPrintfLen, true)
	}
	Printf("%s(raw=%v,key=%s) ES bulk upload of %d items failed, falling back to one-by-one mode, response: %s\n", ds.Name(), raw, key, len(items), sResp)
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk upload error: %+v\n", ds.Name(), raw, key, err)
	}
	err = nil
	// Fallback to one-by-one inserts
	var indexName string
	if raw {
		indexName = ctx.RawIndex
	} else {
		indexName = ctx.RichIndex
	}
	// On ES server errors (not client errors) and when GAP URL is configured
	if status >= 500 && ctx.GapURL != "" {
		docs := [][]byte{}
		ids := []string{}
		indices := []string{}
		for _, item := range items {
			doc, err = jsoniter.Marshal(item)
			if err != nil {
				return
			}
			iID, ok := item.(map[string]interface{})[key]
			if !ok {
				err = fmt.Errorf("missing %s property in %+v", key, DumpKeys(item))
				return
			}
			id, ok := iID.(string)
			if !ok {
				err = fmt.Errorf("%s property is %T not string %+v", key, iID, iID)
				return
			}
			indices = append(indices, indexName)
			ids = append(ids, id)
			docs = append(docs, doc)
		}
		err = SendMultipleDocumentsToGAP(ctx, indices, ids, docs, true)
		if err == nil {
			Printf("Sent bulk request to GAP handler in response to http code %d\n", status)
			return
		}
		Printf("Sending multiple documents to GAP handler in response to %d HTTP code failed, fall back to one-by-one: %+v\n", status, err)
	}
	url = ctx.ESURL + "/" + indexName + "/_doc/"
	headers := map[string]string{"Content-Type": "application/json"}
	retry := true
	var itemStatus int
	for _, item := range items {
		doc, _ = jsoniter.Marshal(item)
		id, _ := item.(map[string]interface{})[key].(string)
		id = urlLib.PathEscape(id)
		_, itemStatus, _, _, err = Request(
			ctx,
			url+id,
			Put,
			headers,
			doc,
			[]string{},
			nil,                                 // JSON statuses
			map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 400-599
			map[[2]int]struct{}{{200, 201}: {}}, // OK statuses: 200-201
			nil,                                 // Cache statuses
			retry,                               // retry
			nil,                                 // cache duration
			true,                                // skip in dry-run mode
		)
		if err != nil {
			Printf("SendToElastic: error: %+v for %+v\n", err, item)
			switch ctx.AllowFail {
			case 0:
				// send to gap handler if configured only for 5xx HTTP error codes - they mean server side issue
				if itemStatus >= 500 && ctx.GapURL != "" {
					err = SendSingleDocumentToGAP(ctx, indexName, id, doc, true)
					if err != nil {
						return
					}
				} else {
					// continue normally, so next items will attempt retry too
					err = nil
				}
			case 1:
				// return error
				return
			case 2:
				// ignore (do not continue)
				err = nil
				return
			case 3:
				// continue, but do not attempt to retry on next items
				retry = false
				err = nil
			default:
				// continue normally, so next items will attempt retry too
				err = nil
			}
		}
	}
	if ctx.Debug > 0 {
		Printf("%s(raw=%v,key=%s) ES bulk upload saved %d items (in non-bulk mode)\n", ds.Name(), raw, key, len(items))
	}
	return
}

func prettyPrint(data interface{}) string {
	j := jsoniter.Config{SortMapKeys: true, EscapeHTML: true}.Froze()
	pretty, err := j.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Sprintf("%T: %+v", data, data)
	}
	return string(pretty)
}

func printObj(data interface{}) string {
	j := jsoniter.Config{SortMapKeys: true, EscapeHTML: true}.Froze()
	pretty, err := j.Marshal(data)
	if err != nil {
		return fmt.Sprintf("%+v", data)
	}
	return string(pretty)
}

// GetLastUpdate - get last update date from ElasticSearch
func GetLastUpdate(ctx *Ctx, ds DS, raw bool) (lastUpdate *time.Time) {
	// curl -s -XPOST -H 'Content-type: application/json' '${URL}/index/_search?size=0' -d '{"aggs":{"m":{"max":{"field":"date_field"}}}}' | jq -r '.aggregations.m.value_as_string'
	dateField := JSONEscape(ds.DateField(ctx))
	originField := JSONEscape(ds.OriginField(ctx))
	origin := JSONEscape(ds.Origin(ctx))
	var payloadBytes []byte
	if ds.ResumeNeedsCategory(ctx, raw) {
		category := ds.ItemCategory(ctx)
		categoryField := "is_" + ds.Name() + "_" + category
		if ds.ResumeNeedsOrigin(ctx, raw) {
			payloadBytes = []byte(`{"query":{"bool":{"filter":[{"term":{"` + originField + `":"` + origin + `"}},{"term":{"` + categoryField + `":1}}]}},"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
		} else {
			payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + categoryField + `":1}}}},"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
		}
	} else {
		if ds.ResumeNeedsOrigin(ctx, raw) {
			payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + originField + `":"` + origin + `"}}}},"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
		} else {
			payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + dateField + `"}}}}`)
		}
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
	resp, _, _, _, err := Request(
		ctx,
		url,
		method,
		map[string]string{"Content-Type": "application/json"}, // headers
		payloadBytes,                        // payload
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		nil,                                 // Cache statuses
		true,                                // retry
		nil,                                 // cache for
		false,                               // skip in dry-run mode
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
	originField := JSONEscape(ds.OriginField(ctx))
	origin := JSONEscape(ds.Origin(ctx))
	var payloadBytes []byte
	if ds.ResumeNeedsCategory(ctx, raw) {
		category := ds.ItemCategory(ctx)
		categoryField := "is_" + ds.Name() + "_" + category
		if ds.ResumeNeedsOrigin(ctx, raw) {
			payloadBytes = []byte(`{"query":{"bool":{"filter":[{"term":{"` + originField + `":"` + origin + `"}},{"term":{"` + categoryField + `":1}}]}},"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
		} else {
			payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + categoryField + `":1}}}},"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
		}
	} else {
		if ds.ResumeNeedsOrigin(ctx, raw) {
			payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"` + originField + `":"` + origin + `"}}}},"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
		} else {
			payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + offsetField + `"}}}}`)
		}
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
	resp, _, _, _, err := Request(
		ctx,
		url,
		method,
		map[string]string{"Content-Type": "application/json"}, // headers
		payloadBytes,                        // payload
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200, 404
		nil,                                 // Cache statuses
		true,                                // retry
		nil,                                 // cache for
		false,                               // skip in dry-run mode
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
