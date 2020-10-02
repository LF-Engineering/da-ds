package dads

import (
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
)

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
			err = fmt.Errorf("missing %s property in %+v", key, DumpKeys(item))
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
		true,
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
			true,
		)
	}
	if ctx.Debug > 0 {
		Printf("%s: saved %d items (in non-bulk mode)\n", ds.Name(), len(items))
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
		Printf("raw %v resume from date query: %s\n", raw, string(payloadBytes))
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
		Printf("raw %v resume from offset query: %s\n", raw, string(payloadBytes))
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
