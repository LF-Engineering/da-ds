package dads

import (
	"fmt"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// ConfluenceBackendVersion - backend version
	ConfluenceBackendVersion = "0.0.1"
)

var (
	// ConfluenceRawMapping - Confluence raw index mapping
	ConfluenceRawMapping = []byte(`{"dynamic":true,"properties":{"data":{"properties":{"metadata__updated_on":{"type":"date"},"extensions":{"dynamic":false,"properties":{}},"ancestors":{"properties":{"extensions":{"dynamic":false,"properties":{}}}},"body":{"dynamic":false,"properties":{}}}}}}`)
	// ConfluenceRichMapping - Confluence rich index mapping
	ConfluenceRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"title_analyzed":{"type":"text","index":true}}}`)
	// ConfluenceCategories - categories defined for Confluence
	ConfluenceCategories = map[string]struct{}{HistoricalContent: {}}
	// ConfluenceDefaultMaxContents - max contents to fetch at a time
	ConfluenceDefaultMaxContents = 200
)

// DSConfluence - DS implementation for confluence - does nothing at all, just presents a skeleton code
type DSConfluence struct {
	DS          string
	URL         string // From DA_CONFLUENCE_URL - Group name like GROUP-topic
	NoSSLVerify bool   // From DA_CONFLUENCE_NO_SSL_VERIFY
	MultiOrigin bool   // From DA_CONFLUENCE_MULTI_ORIGIN - allow multiple groups in a single index
	MaxContents int    // From DA_CONFLUENCE_MAX_CONTENTS, defaults to ConfluenceDefaultMaxContents (200)
}

// ParseArgs - parse confluence specific environment variables
func (j *DSConfluence) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Confluence
	prefix := "DA_CONFLUENCE_"
	j.URL = os.Getenv(prefix + "URL")
	j.NoSSLVerify = StringToBool(os.Getenv(prefix + "NO_SSL_VERIFY"))
	j.MultiOrigin = StringToBool(os.Getenv(prefix + "MULTI_ORIGIN"))
	if j.NoSSLVerify {
		NoSSLVerify()
	}
	if ctx.Env("MAX_CONTENTS") != "" {
		maxContents, err := strconv.Atoi(ctx.Env("MAX_CONTENTS"))
		FatalOnError(err)
		if maxContents > 0 {
			j.MaxContents = maxContents
		}
	} else {
		j.MaxContents = ConfluenceDefaultMaxContents
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSConfluence) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	return
}

// Name - return data source name
func (j *DSConfluence) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSConfluence) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSConfluence) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for datasource
func (j *DSConfluence) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSConfluence) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for datasource
func (j *DSConfluence) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// GetConfluenceContents - get confluence historical contents
func (j *DSConfluence) GetConfluenceContents(ctx *Ctx, fromDate, next string) (contents []map[string]interface{}, newNext string, err error) {
	if next == "" {
		return
	}
	method := Get
	cacheDur := time.Duration(6) * time.Hour
	// Init state
	var url string
	if next == "i" {
		url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d&expand=ancestors", j.MaxContents)
	} else {
		url = j.URL + next
	}
	res, status, _, err := Request(
		ctx,
		url,
		method,
		nil,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		false,                               // retry
		&cacheDur,                           // cache duration
		false,                               // skip in dry-run mode
	)
	// Printf("res=%v\n", res.(map[string]interface{}))
	Printf("status=%d, err=%v\n", status, err)
	if err != nil {
		return
	}
	result, ok := res.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot parse JSON from:\n%s\n", string(res.([]byte)))
		return
	}
	iLinks, ok := result["_links"]
	if ok {
		links, ok := iLinks.(map[string]interface{})
		if ok {
			iNext, ok := links["next"]
			if ok {
				newNext, _ = iNext.(string)
			}
		}
	}
	iResults, ok := result["results"]
	if ok {
		results, ok := iResults.([]interface{})
		if ok {
			for _, iResult := range results {
				content, ok := iResult.(map[string]interface{})
				if ok {
					contents = append(contents, content)
				}
			}
		}
	}
	return
}

// FetchItems - implement enrich data for confluence datasource
func (j *DSConfluence) FetchItems(ctx *Ctx) (err error) {
	var sDateFrom string
	if ctx.DateFrom != nil {
		sDateFrom = ToYMDHMDate(*ctx.DateFrom)
	} else {
		sDateFrom = "1970-01-01 00:00"
	}
	next := "i"
	var (
		ch             chan error
		allContents    []interface{}
		allContentsMtx *sync.Mutex
		escha          []chan error
		eschaMtx       *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allContentsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processContent := func(c chan error, content map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		esItem := j.AddMetadata(ctx, content)
		if ctx.Project != "" {
			content["project"] = ctx.Project
		}
		esItem["data"] = content
		// FIXME
		Printf("esItem: %+v\n", esItem)
		os.Exit(1)
		if allContentsMtx != nil {
			allContentsMtx.Lock()
		}
		allContents = append(allContents, esItem)
		nContents := len(allContents)
		if nContents >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allContents)
				if ee != nil {
					Printf("error %v sending %d historical contents to ElasticSearch\n", ee, len(allContents))
				}
				allContents = []interface{}{}
				if allContentsMtx != nil {
					allContentsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToElastic(wch)
				}()
			} else {
				e = sendToElastic(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allContentsMtx != nil {
				allContentsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, next)
			if err != nil {
				return
			}
			for _, cont := range contents {
				go func(content map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processContent(ch, content)
					if e != nil {
						Printf("process error: %v\n", e)
						return
					}
					if esch != nil {
						if eschaMtx != nil {
							eschaMtx.Lock()
						}
						escha = append(escha, esch)
						if eschaMtx != nil {
							eschaMtx.Unlock()
						}
					}
				}(cont)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			}
			if next == "" {
				break
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for {
			var contents []map[string]interface{}
			contents, next, err = j.GetConfluenceContents(ctx, sDateFrom, next)
			if err != nil {
				return
			}
			for _, content := range contents {
				_, err = processContent(nil, content)
				if err != nil {
					return
				}
			}
			if next == "" {
				break
			}
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nContents := len(allContents)
	if ctx.Debug > 0 {
		Printf("%d remaining contents to send to ES\n", nContents)
	}
	if nContents > 0 {
		err = SendToElastic(ctx, j, true, UUID, allContents)
		if err != nil {
			Printf("Error %v sending %d contents to ES\n", err, len(allContents))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSConfluence) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSConfluence) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSConfluence) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSConfluence) RichIDField(*Ctx) string {
	// IMPL:
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSConfluence) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSConfluence) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSConfluence) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSConfluence) Categories() map[string]struct{} {
	return ConfluenceCategories
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSConfluence) ResumeNeedsOrigin(ctx *Ctx) bool {
	return j.MultiOrigin
}

// Origin - return current origin
func (j *DSConfluence) Origin(ctx *Ctx) string {
	return j.URL
}

// ItemID - return unique identifier for an item
func (j *DSConfluence) ItemID(item interface{}) string {
	// IMPL: toString(item["id"]) + '#v' + toString(item["version"]["number"])
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// AddMetadata - add metadata to the item
func (j *DSConfluence) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	// IMPL:
	mItem = make(map[string]interface{})
	origin := j.URL
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = ConfluenceBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(item)
	// FIXME: special confluence search fields (non standard)
	//mItem["search_fields"] = j.GenSearchFields(ctx, issue, uuid)
	//mItem["search_fields"] = make(map[string]interface{})
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSConfluence) ItemUpdatedOn(item interface{}) time.Time {
	// IMPL: toDatetime(item['version']['when'])
	return time.Now()
}

// ItemCategory - return unique identifier for an item
func (j *DSConfluence) ItemCategory(item interface{}) string {
	// IMPL:
	return HistoricalContent
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSConfluence) ElasticRawMapping() []byte {
	return ConfluenceRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSConfluence) ElasticRichMapping() []byte {
	return ConfluenceRichMapping
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSConfluence) GetItemIdentities(ctx *Ctx, doc interface{}) (map[[3]string]struct{}, error) {
	// IMPL:
	return map[[3]string]struct{}{}, nil
}

// ConfluenceEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func ConfluenceEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	// IMPL:
	if ctx.Debug > 0 {
		Printf("confluence enrich items %d/%d func\n", len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	dbConfigured := ctx.AffsDBConfigured()
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		src, ok := item.(map[string]interface{})["_source"]
		if !ok {
			e = fmt.Errorf("Missing _source in item %+v", DumpKeys(item))
			return
		}
		doc, ok := src.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v\n", doc)
			return
		}
		if 1 == 0 {
			Printf("%v\n", dbConfigured)
		}
		// Actual item enrichment
		/*
			    var rich map[string]interface{}
					if thrN > 1 {
						mtx.Lock()
					}
					*docs = append(*docs, rich)
					if thrN > 1 {
						mtx.Unlock()
					}
		*/
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
	return
}

// EnrichItems - perform the enrichment
func (j *DSConfluence) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, ConfluenceEnrichItemsFunc, nil, true)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSConfluence) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	// IMPL:
	rich = item
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSConfluence) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSConfluence) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSConfluence) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	// IMPL:
	return []string{Author}, true
}
