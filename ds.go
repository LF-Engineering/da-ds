package dads

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var (
	// MappingNotAnalyzeString - make all string keywords by default (not analyze them)
	MappingNotAnalyzeString = []byte(`{"dynamic_templates":[{"notanalyzed":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"type":"date","format":"strict_date_optional_time||epoch_millis"}}}]}`)
	// BulkRefreshMode - bulk upload refresh mode, can be: false, true, wait_for
	BulkRefreshMode = "true"
)

// DS - interface for all data source types
type DS interface {
	ParseArgs(*Ctx) error
	Name() string
	Info() string
	Validate() error
	FetchRaw(*Ctx) error
	FetchItems(*Ctx) error
	Enrich(*Ctx) error
	DateField(*Ctx) string
	OffsetField(*Ctx) string
	Categories() map[string]struct{}
	CustomFetchRaw() bool
	CustomEnrich() bool
	SupportDateFrom() bool
	SupportOffsetFrom() bool
	ResumeNeedsOrigin() bool
	Origin() string
	ItemID(interface{}) string
	ItemUpdatedOn(interface{}) time.Time
	ItemCategory(interface{}) string
	SearchFields() map[string][]string
	ElasticRawMapping() []byte
	ElasticRichMapping() []byte
}

// GetUUID - generate UUID of string args
func GetUUID(ctx *Ctx, args ...string) (h string) {
	if ctx.Debug > 1 {
		defer func() {
			Printf("GetUUID(%v) --> %s\n", args, h)
		}()
	}
	stripF := func(str string) string {
		isOk := func(r rune) bool {
			return r < 32 || r >= 127
		}
		t := transform.Chain(norm.NFKD, transform.RemoveFunc(isOk))
		str, _, _ = transform.String(t, str)
		return str
	}
	arg := ""
	for _, a := range args {
		if a == "" {
			Fatalf("GetUUID(%v) - empty argument(s) not allowed", args)
		}
		if arg != "" {
			arg += ":"
		}
		arg += stripF(a)
	}
	hash := sha1.New()
	_, err := hash.Write([]byte(arg))
	FatalOnError(err)
	h = hex.EncodeToString(hash.Sum(nil))
	return
}

// Request - wrapper to do any HTTP request
// jsonStatuses - set of status code ranges to be parsed as JSONs
// errorStatuses - specify status value ranges for which we should return error
// okStatuses - specify status value ranges for which we should return error (only taken into account if not empty)
func Request(
	ctx *Ctx,
	url, method string,
	headers map[string]string,
	payload []byte,
	jsonStatuses, errorStatuses, okStatuses map[[2]int]struct{},
) (result interface{}, status int, err error) {
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

// SendToElastic - send items to ElasticSearch
func SendToElastic(ctx *Ctx, ds DS, raw bool, key string, items []interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("%s: saving %d items\n", ds.Name(), len(items))
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
		uuid, ok := item.(map[string]interface{})[key].(string)
		if !ok {
			err = fmt.Errorf("missing %s property in %+v", key, item)
			return
		}
		hdr = []byte(`{"index":{"_id":"` + uuid + "\"}}\n")
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
	)
	if err == nil {
		if ctx.Debug > 0 {
			Printf("%s: saved %d items\n", ds.Name(), len(items))
		}
		return
	}
	Printf("%s: bulk upload of %d items failed, falling back to one-by-one mode\n", ds.Name(), len(items))
	if ctx.Debug > 1 {
		Printf("Error: %+v\n", err)
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
		uuid, _ := item.(map[string]interface{})[key].(string)
		_, _, err = Request(
			ctx,
			url+uuid,
			Put,
			headers,
			doc,
			nil,                                 // JSON statuses
			map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 400-599
			map[[2]int]struct{}{{200, 201}: {}}, // OK statuses: 200-201
		)
	}
	if ctx.Debug > 0 {
		Printf("%s: saved %d items (in non-bulk mode)\n", ds.Name(), len(items))
	}
	return
}

// GetLastUpdate - get last update date from ElasticSearch
func GetLastUpdate(ctx *Ctx, ds DS) (lastUpdate *time.Time) {
	// curl -s -XPOST -H 'Content-type: application/json' '${URL}/index/_search?size=0' -d '{"aggs":{"m":{"max":{"field":"date_field"}}}}' | jq -r '.aggregations.m.value_as_string'
	dateField := ds.DateField(ctx)
	var payloadBytes []byte
	if ds.ResumeNeedsOrigin() {
		payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"origin":"` + JSONEscape(ds.Origin()) + `"}}}},"aggs":{"m":{"max":{"field":"` + JSONEscape(dateField) + `"}}}}`)
	} else {
		payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + JSONEscape(dateField) + `"}}}}`)
	}
	url := ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
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
		Printf("JSON decode error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	if res.Aggs.M.Str != "" {
		var tm time.Time
		tm, err = TimeParseAny(res.Aggs.M.Str)
		if err != nil {
			Printf("Decode aggregations error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		lastUpdate = &tm
	}
	return
}

// GetLastOffset - get last offset from ElasticSearch
func GetLastOffset(ctx *Ctx, ds DS) (offset float64) {
	offset = -1.0
	// curl -s -XPOST -H 'Content-type: application/json' '${URL}/index/_search?size=0' -d '{"aggs":{"m":{"max":{"field":"offset_field"}}}}' | jq -r '.aggregations.m.value'
	offsetField := ds.OffsetField(ctx)
	var payloadBytes []byte
	if ds.ResumeNeedsOrigin() {
		payloadBytes = []byte(`{"query":{"bool":{"filter":{"term":{"origin":"` + JSONEscape(ds.Origin()) + `"}}}},"aggs":{"m":{"max":{"field":"` + JSONEscape(offsetField) + `"}}}}`)
	} else {
		payloadBytes = []byte(`{"aggs":{"m":{"max":{"field":"` + JSONEscape(offsetField) + `"}}}}`)
	}
	url := ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
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
		Printf("JSON decode error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	if res.Aggs.M.Int != nil {
		offset = *res.Aggs.M.Int
	}
	return
}

// HandleMapping - create/update mapping for raw or rich index
func HandleMapping(ctx *Ctx, ds DS, raw bool) (err error) {
	// Create index, ignore if exists (see status 400 is not in error statuses)
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex
	}
	_, _, err = Request(
		ctx,
		url,
		Put,
		nil,                                 // headers
		[]byte{},                            // payload
		nil,                                 // JSON statuses
		map[[2]int]struct{}{{401, 599}: {}}, // error statuses: 401-599
		nil,                                 // OK statuses
	)
	FatalOnError(err)
	// DS specific raw index mapping
	var mapping []byte
	if raw {
		mapping = ds.ElasticRawMapping()
	} else {
		mapping = ds.ElasticRichMapping()
	}
	url += "/_mapping"
	_, _, err = Request(
		ctx,
		url,
		Put,
		map[string]string{"Content-Type": "application/json"},
		mapping,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}},
	)
	FatalOnError(err)
	// Global not analyze string mapping
	_, _, err = Request(
		ctx,
		url,
		Put,
		map[string]string{"Content-Type": "application/json"},
		MappingNotAnalyzeString,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}},
	)
	FatalOnError(err)
	return
}

// FetchRaw - implement fetch raw data (generic)
func FetchRaw(ctx *Ctx, ds DS) (err error) {
	if ds.CustomFetchRaw() {
		return ds.FetchRaw(ctx)
	}
	err = HandleMapping(ctx, ds, true)
	if err != nil {
		Fatalf(ds.Name()+": HandleMapping error: %+v\n", err)
	}
	if ctx.DateFrom != nil && ctx.OffsetFrom >= 0.0 {
		Fatalf(ds.Name() + ": you cannot use both date from and offset from\n")
	}
	if ctx.DateTo != nil && ctx.OffsetTo >= 0.0 {
		Fatalf(ds.Name() + ": you cannot use both date to and offset to\n")
	}
	var (
		lastUpdate *time.Time
		offset     *float64
	)
	if ds.SupportDateFrom() {
		lastUpdate = ctx.DateFrom
		if lastUpdate == nil {
			lastUpdate = GetLastUpdate(ctx, ds)
		}
		if lastUpdate != nil {
			Printf("%s: starting from date: %v\n", ds.Name(), *lastUpdate)
			ctx.DateFrom = lastUpdate
		}
	}
	if ds.SupportOffsetFrom() {
		if ctx.OffsetFrom >= 0.0 {
			offset = &ctx.OffsetFrom
		}
		if offset == nil {
			lastOffset := GetLastOffset(ctx, ds)
			if lastOffset >= 0.0 {
				offset = &lastOffset
			}
		}
		if offset != nil {
			Printf("%s: starting from offset: %v\n", ds.Name(), *offset)
			ctx.OffsetFrom = *offset
		}
	}
	if lastUpdate != nil && offset != nil {
		Fatalf(ds.Name() + ": you cannot use both date from and offset from\n")
	}
	if ctx.Category != "" {
		_, ok := ds.Categories()[ctx.Category]
		if !ok {
			Fatalf(ds.Name() + ": category " + ctx.Category + " not supported")
		}
	}
	err = ds.FetchItems(ctx)
	return
}

// Enrich - implement fetch raw data (generic)
func Enrich(ctx *Ctx, ds DS) (err error) {
	if ds.CustomEnrich() {
		return ds.Enrich(ctx)
	}
	err = HandleMapping(ctx, ds, false)
	if err != nil {
		Fatalf(ds.Name()+": HandleMapping error: %+v\n", err)
	}
	return
}
