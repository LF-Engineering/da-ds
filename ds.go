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
}

// GetUUID - generate UUID of string args
func GetUUID(ctx *Ctx, args ...string) (h string) {
	if ctx.Debug > 1 {
		defer func() {
			fmt.Printf("GetUUID(%v) --> %s\n", args, h)
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
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		Printf("New request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		Printf("Do request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == 404 {
		return
	}
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		Printf("Method:%s url:%s status:%d query:%s\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		return
	}
	type resultStruct struct {
		Aggs struct {
			M struct {
				Str string `json:"value_as_string"`
			} `json:"m"`
		} `json:"aggregations"`
	}
	res := resultStruct{}
	err = jsoniter.NewDecoder(resp.Body).Decode(&res)
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
	payloadBody := bytes.NewReader(payloadBytes)
	method := Post
	url := ctx.ESURL + "/" + ctx.RawIndex + "/_search?size=0"
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		Printf("New request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		Printf("Do request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == 404 {
		return
	}
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		Printf("Method:%s url:%s status:%d query:%s\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
		return
	}
	type resultStruct struct {
		Aggs struct {
			M struct {
				Int *float64 `json:"value,omitempty"`
			} `json:"m"`
		} `json:"aggregations"`
	}
	res := resultStruct{}
	err = jsoniter.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		Printf("JSON decode error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
		return
	}
	if res.Aggs.M.Int != nil {
		offset = *res.Aggs.M.Int
	}
	return
}

// FetchRaw - implement fetch raw data (generic)
func FetchRaw(ctx *Ctx, ds DS) (err error) {
	if ds.CustomFetchRaw() {
		return ds.FetchRaw(ctx)
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
			Printf("%s: staring from date: %v\n", ds.Name(), *lastUpdate)
			ctx.DateFrom = lastUpdate
		}
	}
	if ds.SupportOffsetFrom() {
		if ctx.OffsetFrom >= 0.0 {
			offset = &ctx.OffsetFrom
		}
		if offset == nil {
			lastOffset := GetLastOffset(ctx, ds)
			offset = &lastOffset
		}
		if offset != nil {
			Printf("%s: staring from offset: %v\n", ds.Name(), *offset)
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
	return
}
