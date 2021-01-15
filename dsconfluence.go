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
	ConfluenceBackendVersion = "0.1.0"
)

var (
	// ConfluenceRawMapping - Confluence raw index mapping
	ConfluenceRawMapping = []byte(`{"dynamic":true,"properties":{"data":{"properties":{"metadata__updated_on":{"type":"date"},"extensions":{"dynamic":false,"properties":{}},"ancestors":{"properties":{"extensions":{"dynamic":false,"properties":{}}}},"body":{"dynamic":false,"properties":{}}}}}}`)
	// ConfluenceRichMapping - Confluence rich index mapping
	ConfluenceRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"title_analyzed":{"type":"text","index":true}}}`)
	// ConfluenceCategories - categories defined for Confluence
	ConfluenceCategories = map[string]struct{}{HistoricalContent: {}}
	// ConfluenceDefaultMaxContents - max contents to fetch at a time
	ConfluenceDefaultMaxContents = 1000
	// ConfluenceDefaultSearchField - default search field
	ConfluenceDefaultSearchField = "item_id"
	// ConfluenceContentRoles - roles to fetch affiliation data for historical content
	ConfluenceContentRoles = []string{"by"}
	// ConfluenceRichAuthorField - rich index author field
	ConfluenceRichAuthorField = "by"
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
	if j.URL == "" {
		err = fmt.Errorf("URL must be set")
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

// GetHistoricalContents - get historical contents from teh current content
func (j *DSConfluence) GetHistoricalContents(ctx *Ctx, content map[string]interface{}, dateFrom time.Time) (contents []map[string]interface{}, err error) {
	iContentURL, _ := Dig(content, []string{"_links", "webui"}, true, false)
	ancestors, ok := Dig(content, []string{"ancestors"}, false, true)
	if !ok {
		ancestors = []interface{}{}
	}
	contentURL, _ := iContentURL.(string)
	contentURL = j.URL + contentURL
	content["content_url"] = contentURL
	content["ancestors"] = ancestors
	iVersionNumber, _ := Dig(content, []string{"version", "number"}, true, false)
	lastVersion := int(iVersionNumber.(float64))
	////
	if lastVersion == 1 {
		contents = append(contents, content)
		return
	}
	iID, ok := content["id"]
	if !ok {
		err = fmt.Errorf("missing id property in content: %+v", content)
		return
	}
	id, ok := iID.(string)
	if !ok {
		err = fmt.Errorf("id property is not a string: %+v", content)
		return
	}
	method := Get
	cacheDur := time.Duration(24) * time.Hour
	version := 1
	var (
		res    interface{}
		status int
	)
	for {
		////url := j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("body.storage,history,version")
		url := j.URL + "/rest/api/content/" + id + "?version=" + strconv.Itoa(version) + "&status=historical&expand=" + neturl.QueryEscape("history,version")
		if ctx.Debug > 1 {
			Printf("historical content url: %s\n", url)
		}
		res, status, _, _, err = Request(
			ctx,
			url,
			method,
			nil,
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {500, 500}: {}, {404, 404}: {}}, // OK statuses: 200
			map[[2]int]struct{}{{200, 200}: {}},                                 // Cache statuses: 200
			false,                                                               // retry
			&cacheDur,                                                           // cache duration
			false,                                                               // skip in dry-run mode
		)
		if status == 404 || status == 500 {
			if ctx.Debug > 1 {
				Printf("%s: v%d status %d: %s\n", id, version, status, url)
			}
			break
		}
		if err != nil {
			return
		}
		result, ok := res.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
			return
		}
		iLatest, _ := Dig(result, []string{"history", "latest"}, true, false)
		latest, ok := iLatest.(bool)
		if !ok {
			err = fmt.Errorf("cannot read latest property: %+v", result)
			return
		}
		iWhen, ok := Dig(result, []string{"version", "when"}, false, true)
		if !ok {
			if ctx.Debug > 0 {
				Printf("missing 'when' attribute for content %s version %d, skipping\n", id, version)
			}
			if latest {
				break
			}
			version++
			continue
		}
		var when time.Time
		when, err = TimeParseInterfaceString(iWhen)
		if err != nil {
			return
		}
		if !when.Before(dateFrom) {
			result["content_url"] = contentURL
			result["ancestors"] = ancestors
			contents = append(contents, result)
		}
		if ctx.Debug > 2 {
			Printf("%s: v%d %+v,%v (%s)\n", id, version, when, latest, url)
		}
		if latest {
			break
		}
		version++
		////
		if version == lastVersion {
			break
		}
	}
	contents = append(contents, content)
	if ctx.Debug > 1 {
		Printf("final %s %d (%d historical contents)\n", id, version, len(contents))
	}
	return
}

// GetConfluenceContents - get confluence historical contents
func (j *DSConfluence) GetConfluenceContents(ctx *Ctx, fromDate, next string) (contents []map[string]interface{}, newNext string, err error) {
	/*
		Printf("GetConfluenceContents: in\n")
		defer func() {
			Printf("GetConfluenceContents: out %d\n", len(contents))
		}()
	*/
	if next == "" {
		return
	}
	method := Get
	cacheDur := time.Duration(24) * time.Hour
	var url string
	// Init state
	if next == "i" {
		////url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d&expand=ancestors", j.MaxContents)
		url = j.URL + "/rest/api/content/search?cql=" + neturl.QueryEscape("lastModified>='"+fromDate+"' order by lastModified") + fmt.Sprintf("&limit=%d", j.MaxContents) + "&expand=" + neturl.QueryEscape("ancestors,version")
	} else {
		url = j.URL + next
	}
	if ctx.Debug > 1 {
		Printf("content url: %s\n", url)
	}
	res, status, _, _, err := Request(
		ctx,
		url,
		method,
		nil,
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses: 200
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheDur,                           // cache duration
		false,                               // skip in dry-run mode
	)
	// Printf("res=%v\n", res.(map[string]interface{}))
	// Printf("status=%d, err=%v\n", status, err)
	if err != nil {
		return
	}
	result, ok := res.(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot parse JSON from (status: %d):\n%s", status, string(res.([]byte)))
		return
	}
	iNext, ok := Dig(result, []string{"_links", "next"}, false, true)
	if ok {
		newNext, _ = iNext.(string)
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
	var (
		sDateFrom string
		dateFrom  time.Time
	)
	if ctx.DateFrom != nil {
		dateFrom = *ctx.DateFrom
		sDateFrom = ToYMDHMDate(dateFrom)
	} else {
		dateFrom = DefaultDateFrom
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
		// Printf("processContent: in\n")
		var contents []map[string]interface{}
		contents, e = j.GetHistoricalContents(ctx, content, dateFrom)
		if e != nil {
			return
		}
		var esItems []interface{}
		for _, content := range contents {
			esItem := j.AddMetadata(ctx, content)
			if ctx.Project != "" {
				content["project"] = ctx.Project
			}
			esItem["data"] = content
			esItems = append(esItems, esItem)
		}
		// Printf("processContent: out %d\n", len(contents))
		if allContentsMtx != nil {
			allContentsMtx.Lock()
		}
		allContents = append(allContents, esItems...)
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
	// Because in confluence one raw item generates no more than 1 rich item
	return UUID
}

// RichAuthorField - return rich author field name
func (j *DSConfluence) RichAuthorField(*Ctx) string {
	return ConfluenceRichAuthorField
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
	id, _ := Dig(item, []string{"id"}, true, false)
	versionNumber, _ := Dig(item, []string{"version", "number"}, true, false)
	return id.(string) + "#v" + fmt.Sprintf("%.0f", versionNumber.(float64))
}

// AddMetadata - add metadata to the item
func (j *DSConfluence) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
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
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	id, _ := Dig(item, []string{"id"}, true, false)
	versionNumber, _ := Dig(item, []string{"version", "number"}, true, false)
	var ancestorIDs []interface{}
	iAncestors, ok := Dig(item, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorID, ok := ancestor["id"]
				if ok {
					ancestorIDs = append(ancestorIDs, ancestorID)
				}
			}
		}
	}
	FatalOnError(DeepSet(mItem, []string{"search_fields", ConfluenceDefaultSearchField}, itemID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "content_id"}, id, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "ancestor_ids"}, ancestorIDs, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "version_number"}, versionNumber, false))
	// Printf("%+v\n", mItem["search_fields"])
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSConfluence) ItemUpdatedOn(item interface{}) time.Time {
	iWhen, _ := Dig(item, []string{"version", "when"}, false, true)
	when, err := TimeParseInterfaceString(iWhen)
	FatalOnError(err)
	return when
}

// ItemCategory - return unique identifier for an item
func (j *DSConfluence) ItemCategory(item interface{}) string {
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
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSConfluence) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("%+v -> %+v\n", DumpPreview(doc, 100), identities)
		}()
	}
	iUser, ok := Dig(doc, []string{"data", "version", "by"}, true, false)
	user, _ := iUser.(map[string]interface{})
	username := Nil
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	name := Nil
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	email := Nil
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	if name == Nil && username == Nil && email == Nil {
		return
	}
	identities = map[[3]string]struct{}{{name, username, email}: {}}
	return
}

// ConfluenceEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func ConfluenceEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
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
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		var rich map[string]interface{}
		rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
		if e != nil {
			return
		}
		e = EnrichItem(ctx, ds, rich)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		if thrN > 1 {
			mtx.Unlock()
		}
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
	rich = make(map[string]interface{})
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	page, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	for _, field := range []string{"type", "id", "status", "title", "content_url"} {
		rich[field], _ = page[field]
	}
	title := ""
	iTitle, ok := page["title"]
	if ok {
		title, _ = iTitle.(string)
	}
	rich["title_analyzed"] = title
	if len(title) > KeywordMaxlength {
		title = title[:KeywordMaxlength]
	}
	rich["title"] = title
	version, ok := page["version"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing version field in item %+v", DumpKeys(page))
		return
	}
	userName, ok := Dig(version, []string{"by", "username"}, false, true)
	if ok {
		rich["author_name"] = userName
	} else {
		rich["author_name"], _ = Dig(version, []string{"by", "displayName"}, true, false)
	}
	rich["message"], _ = Dig(version, []string{"message"}, false, true)
	iVersion, _ := version["number"]
	rich["version"] = iVersion
	rich["date"], _ = version["when"]
	////base, _ := Dig(page, []string{"_links", "base"}, true, false)
	webUI, _ := Dig(page, []string{"_links", "webui"}, true, false)
	////rich["url"] = base.(string) + webUI.(string)
	rich["url"] = j.URL + webUI.(string)
	iSpace, ok := Dig(page, []string{"_expandable", "space"}, false, true)
	if ok {
		space, _ := iSpace.(string)
		space = strings.Replace(space, "/rest/api/space/", "", -1)
		rich["space"] = space
	}
	var (
		ancestorTitles []interface{}
		ancestorLinks  []interface{}
	)
	iAncestors, ok := Dig(page, []string{"ancestors"}, false, true)
	if ok {
		ancestors, ok := iAncestors.([]interface{})
		if ok {
			for _, iAncestor := range ancestors {
				ancestor, ok := iAncestor.(map[string]interface{})
				if !ok {
					continue
				}
				ancestorTitle, ok := ancestor["title"]
				if ok {
					ancestorTitles = append(ancestorTitles, ancestorTitle)
				} else {
					ancestorTitles = append(ancestorTitles, "NO_TITLE")
				}
				ancestorLink, _ := Dig(ancestor, []string{"_links", "webui"}, true, false)
				ancestorLinks = append(ancestorLinks, ancestorLink)
			}
		}
	}
	rich["ancestors_titles"] = ancestorTitles
	rich["ancestors_links"] = ancestorLinks
	iType, _ := Dig(page, []string{"type"}, true, false)
	if iType.(string) == "page" && int(iVersion.(float64)) == 1 {
		rich["type"] = "new_page"
	}
	rich["is_blogpost"] = 0
	tp, _ := rich["type"].(string)
	rich["is_"+tp] = 1
	// can also be rich["date"]
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	if affs {
		authorKey := "by"
		var affsItems map[string]interface{}
		affsItems, err = j.AffsItems(ctx, item, ConfluenceContentRoles, updatedOn)
		if err != nil {
			return
		}
		for prop, value := range affsItems {
			rich[prop] = value
		}
		for _, suff := range AffsFields {
			rich[Author+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + MultiOrgNames
		_, ok := Dig(rich, []string{orgsKey}, false, true)
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
	}
	for prop, value := range CommonFields(j, updatedOn, Confluence) {
		rich[prop] = value
	}
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSConfluence) AffsItems(ctx *Ctx, page map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	affsItems = make(map[string]interface{})
	var dt time.Time
	dt, err = TimeParseInterfaceString(date)
	if err != nil {
		return
	}
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, page, role)
		if len(identity) == 0 {
			continue
		}
		affsIdentity, empty, e := IdentityAffsData(ctx, j, identity, nil, dt, role)
		if e != nil {
			Printf("AffsItems/IdentityAffsData: error: %v for %v,%v,%v\n", e, identity, dt, role)
		}
		if empty {
			Printf("no identity affiliation data for identity %+v\n", identity)
			continue
		}
		for prop, value := range affsIdentity {
			affsItems[prop] = value
		}
		for _, suff := range RequiredAffsFields {
			k := role + suff
			_, ok := affsIdentity[k]
			if !ok {
				affsIdentity[k] = Unknown
			}
		}
	}
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSConfluence) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	iUser, ok := Dig(item, []string{"data", "version", "by"}, true, false)
	user, _ := iUser.(map[string]interface{})
	username := Nil
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	} else {
		iPublicName, ok := user["publicName"]
		if ok {
			username, _ = iPublicName.(string)
		}
	}
	name := Nil
	iDisplayName, ok := user["displayName"]
	if ok {
		name, _ = iDisplayName.(string)
	}
	email := Nil
	iEmail, ok := user["email"]
	if ok {
		email, _ = iEmail.(string)
	}
	if name == Nil && username == Nil && email == Nil {
		return
	}
	identity = map[string]interface{}{"name": name, "username": username, "email": email}
	return
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSConfluence) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	return []string{"by"}, true
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSConfluence) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}
