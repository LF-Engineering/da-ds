package dads

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// JiraAPIRoot - main API path
	JiraAPIRoot = "/rest/api/2"
	// JiraAPISearch - search API subpath
	JiraAPISearch = "/search"
	// JiraAPIField - field API subpath
	JiraAPIField = "/field"
	// JiraAPIIssue - issue API subpath
	JiraAPIIssue = "/issue"
	// JiraAPIComment - comments API subpath
	JiraAPIComment = "/comment"
	// JiraBackendVersion - backend version
	JiraBackendVersion = "0.0.1"
	// JiraDefaultSearchField - default search field
	JiraDefaultSearchField = "item_id"
	// JiraDropCustomFields - drop custom fields from raw index
	JiraDropCustomFields = false
	// JiraFilterByProjectInComments - filter by project when searching for comments
	JiraFilterByProjectInComments = false
	// JiraMapCustomFields - run custom fields mapping
	JiraMapCustomFields = true
)

var (
	// JiraSearchFields - extra search fields
	JiraSearchFields = map[string][]string{
		"project_id":   {"fields", "project", "id"},
		"project_key":  {"fields", "project", "key"},
		"project_name": {"fields", "project", "name"},
		"issue_key":    {"key"},
	}
	// JiraRawMapping - Jira index mapping
	JiraRawMapping = []byte(`{"dynamic":true,"properties":{"data":{"properties":{"renderedFields":{"dynamic":false,"properties":{}},"operations":{"dynamic":false,"properties":{}},"fields":{"dynamic":true,"properties":{"description":{"type":"text","index":true},"environment":{"type":"text","index":true}}},"changelog":{"properties":{"histories":{"dynamic":false,"properties":{}}}},"comments_data":{"properties":{"body":{"type":"text","index":true}}}}}}}`)
	// JiraRichMapping - Jira index mapping
	JiraRichMapping = []byte(`{"properties":{"main_description_analyzed":{"type":"text","index":true},"releases":{"type":"keyword"},"body":{"type":"text","index":true}}}`)
)

// DSJira - DS implementation for Jira
type DSJira struct {
	DS          string
	URL         string // From DA_JIRA_URL - Jira URL
	NoSSLVerify bool   // From DA_JIRA_NO_SSL_VERIFY
	Token       string // From DA_JIRA_TOKEN
	PageSize    int    // From DA_JIRA_PAGE_SIZE
}

// JiraField - informatin about fields present in issues
type JiraField struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Custom bool   `json:"custom"`
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Jira
	// Jira specific env variables
	j.URL = os.Getenv("DA_JIRA_URL")
	j.NoSSLVerify = os.Getenv("DA_JIRA_NO_SSL_VERIFY") != ""
	j.Token = os.Getenv("DA_JIRA_TOKEN")
	if os.Getenv("DA_JIRA_PAGE_SIZE") == "" {
		j.PageSize = 400
	} else {
		pageSize, err := strconv.Atoi(os.Getenv("DA_JIRA_PAGE_SIZE"))
		FatalOnError(err)
		if pageSize > 0 {
			j.PageSize = pageSize
		}
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSJira) Validate() (err error) {
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	if j.URL == "" {
		err = fmt.Errorf("Jira URL must be set")
	}
	return
}

// Name - return data source name
func (j *DSJira) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSJira) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSJira) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for Jira
func (j *DSJira) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSJira) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for Jira
func (j *DSJira) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// GetFields - implement get fields for jira datasource
func (j *DSJira) GetFields(ctx *Ctx) (customFields map[string]JiraField, err error) {
	url := j.URL + JiraAPIRoot + JiraAPIField
	method := Get
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Authorization": "Basic " + j.Token}
	}
	var resp interface{}
	resp, _, err = Request(ctx, url, method, headers, nil, nil, nil, map[[2]int]struct{}{{200, 200}: {}})
	if err != nil {
		return
	}
	var fields []JiraField
	err = jsoniter.Unmarshal(resp.([]byte), &fields)
	if err != nil {
		return
	}
	customFields = make(map[string]JiraField)
	for _, field := range fields {
		if !field.Custom {
			continue
		}
		customFields[field.ID] = field
	}
	return
}

// GenSearchFields - generate extra search fields
func (j *DSJira) GenSearchFields(ctx *Ctx, issue interface{}, uuid string) (fields map[string]interface{}) {
	searchFields := j.SearchFields()
	fields = make(map[string]interface{})
	fields[JiraDefaultSearchField] = uuid
	for field, keyAry := range searchFields {
		item, _ := issue.(map[string]interface{})
		var value interface{}
		last := len(keyAry) - 1
		miss := false
		for i, key := range keyAry {
			var ok bool
			if i < last {
				item, ok = item[key].(map[string]interface{})
			} else {
				value, ok = item[key]
			}
			if !ok {
				Printf("%s: %+v, current: %s, %d/%d failed\n", field, keyAry, key, i+1, last+1)
				miss = true
				break
			}
		}
		if !miss {
			if ctx.Debug > 1 {
				Printf("Found %s: %+v --> %+v\n", field, keyAry, value)
			}
			fields[field] = value
		}
	}
	if ctx.Debug > 1 {
		Printf("Returing %+v\n", fields)
	}
	return
}

// ProcessIssue - process a single issue
func (j *DSJira) ProcessIssue(ctx *Ctx, allIssues *[]interface{}, allIssuesMtx *sync.Mutex, issue interface{}, customFields map[string]JiraField, from time.Time, to *time.Time, thrN int) (wch chan error, err error) {
	var mtx *sync.RWMutex
	if thrN > 1 {
		mtx = &sync.RWMutex{}
	}
	issueID := j.ItemID(issue)
	var headers map[string]string
	if j.Token != "" {
		headers = map[string]string{"Content-Type": "application/json", "Authorization": "Basic " + j.Token}
	} else {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	processIssue := func(c chan error) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		url := j.URL + JiraAPIRoot + JiraAPIIssue + "/" + issueID + JiraAPIComment
		startAt := int64(0)
		maxResults := int64(j.PageSize)
		epochMS := from.UnixNano() / 1e6
		// Seems like original Jira was using project filter there which is not needed IMHO.
		var jql string
		if JiraFilterByProjectInComments {
			if to != nil {
				epochToMS := (*to).UnixNano() / 1e6
				if ctx.Project != "" {
					jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d AND updated < %d order by updated asc"`, ctx.Project, epochMS, epochToMS)
				} else {
					jql = fmt.Sprintf(`"jql":"updated > %d AND updated < %d order by updated asc"`, epochMS, epochToMS)
				}
			} else {
				if ctx.Project != "" {
					jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
				} else {
					jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
				}
			}
		} else {
			if to != nil {
				epochToMS := (*to).UnixNano() / 1e6
				jql = fmt.Sprintf(`"jql":"updated > %d AND updated < %d order by updated asc"`, epochMS, epochToMS)
			} else {
				jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
			}
		}
		method := Get
		for {
			payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s}`, startAt, maxResults, jql))
			var res interface{}
			res, _, e = Request(
				ctx,
				url,
				method,
				headers,
				payloadBytes,
				map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
				nil,                                 // Error statuses
				map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200, 404
			)
			if e != nil {
				return
			}
			comments, ok := res.(map[string]interface{})["comments"].([]interface{})
			if !ok {
				e = fmt.Errorf("unable to unmarshal comments from %+v", res)
				return
			}
			if ctx.Debug > 1 {
				nComments := len(comments)
				if nComments > 0 {
					Printf("Processing %d comments\n", len(comments))
				}
			}
			if thrN > 1 {
				mtx.Lock()
			}
			issueComments, ok := issue.(map[string]interface{})["comments_data"].([]interface{})
			if !ok {
				issue.(map[string]interface{})["comments_data"] = []interface{}{}
			}
			issueComments, _ = issue.(map[string]interface{})["comments_data"].([]interface{})
			if !ok {
				issueComments = comments
			} else {
				issueComments = append(issueComments, comments...)
			}
			issue.(map[string]interface{})["comments_data"] = issueComments
			if thrN > 1 {
				mtx.Unlock()
			}
			totalF, ok := res.(map[string]interface{})["total"].(float64)
			if !ok {
				e = fmt.Errorf("unable to unmarshal total from %+v", res)
				return
			}
			maxResultsF, ok := res.(map[string]interface{})["maxResults"].(float64)
			if !ok {
				e = fmt.Errorf("unable to maxResults total from %+v", res)
				return
			}
			total := int64(totalF)
			maxResults = int64(maxResultsF)
			inc := int64(totalF)
			if maxResultsF < totalF {
				inc = int64(maxResultsF)
			}
			startAt += inc
			if startAt >= total {
				startAt = total
				break
			}
			if ctx.Debug > 0 {
				Printf("Processing next comments page from %d/%d\n", startAt, total)
			}
		}
		if ctx.Debug > 1 {
			Printf("Processed %d comments\n", startAt)
		}
		return
	}
	var ch chan error
	if thrN > 1 {
		ch = make(chan error)
		go func() {
			_ = processIssue(ch)
		}()
	} else {
		err = processIssue(nil)
		if err != nil {
			return
		}
	}
	if thrN > 1 {
		mtx.RLock()
	}
	issueFields, ok := issue.(map[string]interface{})["fields"].(map[string]interface{})
	if thrN > 1 {
		mtx.RUnlock()
	}
	if !ok {
		err = fmt.Errorf("unable to unmarshal fields from issue %+v", issue)
		return
	}
	if JiraMapCustomFields {
		type mapping struct {
			ID    string
			Name  string
			Value interface{}
		}
		m := make(map[string]mapping)
		for k, v := range issueFields {
			customField, ok := customFields[k]
			if !ok {
				continue
			}
			m[k] = mapping{ID: customField.ID, Name: customField.Name, Value: v}
		}
		// Printf("%+v\n", m)
		for k, v := range m {
			if ctx.Debug > 1 {
				prev := issueFields[k]
				Printf("%s: %+v -> %+v\n", k, prev, v)
			}
			issueFields[k] = v
		}
	}
	// Extra fields
	esItem := make(map[string]interface{})
	origin := j.Origin()
	uuid := UUIDNonEmpty(ctx, origin, issueID)
	timestamp := time.Now()
	esItem["backend_name"] = j.DS
	esItem["backend_version"] = JiraBackendVersion
	esItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	esItem["origin"] = origin
	esItem["uuid"] = uuid
	if thrN > 1 {
		mtx.Lock()
	}
	updatedOn := j.ItemUpdatedOn(issue)
	esItem["updated_on"] = updatedOn
	esItem["category"] = j.ItemCategory(issue)
	esItem["search_fields"] = j.GenSearchFields(ctx, issue, uuid)
	// Seems like it doesn't make sense, because we just added those custom fields
	if JiraDropCustomFields {
		for k := range issueFields {
			if strings.HasPrefix(strings.ToLower(k), "customfield_") {
				delete(issueFields, k)
			}
		}
	}
	esItem[DefaultDateField] = ToESDate(updatedOn)
	esItem[DefaultTimestampField] = ToESDate(timestamp)
	if ctx.Project != "" {
		issue.(map[string]interface{})["project"] = ctx.Project
	}
	esItem["data"] = issue
	if thrN > 1 {
		mtx.Unlock()
		err = <-ch
	}
	if allIssuesMtx != nil {
		allIssuesMtx.Lock()
	}
	*allIssues = append(*allIssues, esItem)
	nIssues := len(*allIssues)
	if nIssues >= ctx.ESBulkSize {
		sendToElastic := func(c chan error) (e error) {
			defer func() {
				if c != nil {
					c <- e
				}
			}()
			e = SendToElastic(ctx, j, true, "uuid", *allIssues)
			if e != nil {
				Printf("Error %v sending %d issues to ElasticSearch\n", e, len(*allIssues))
			}
			*allIssues = []interface{}{}
			if allIssuesMtx != nil {
				allIssuesMtx.Unlock()
			}
			return
		}
		if thrN > 1 {
			wch = make(chan error)
			go func() {
				_ = sendToElastic(wch)
			}()
		} else {
			err = sendToElastic(nil)
			if err != nil {
				return
			}
		}
	} else {
		if allIssuesMtx != nil {
			allIssuesMtx.Unlock()
		}
	}
	return
}

// FetchItems - implement fetch items for jira datasource
func (j *DSJira) FetchItems(ctx *Ctx) (err error) {
	thrN := GetThreadsNum(ctx)
	var customFields map[string]JiraField
	fieldsFetched := false
	var chF chan error
	getFields := func(c chan error) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
			if ctx.Debug > 0 {
				Printf("Got %d custom fields\n", len(customFields))
			}
		}()
		customFields, e = j.GetFields(ctx)
		return
	}
	if thrN > 1 {
		chF = make(chan error)
		go func() {
			_ = getFields(chF)
		}()
	} else {
		err = getFields(nil)
		if err != nil {
			Printf("GetFields error: %+v\n", err)
			return
		}
		fieldsFetched = true
	}
	// '{"jql":"updated > 1601281314000 order by updated asc","startAt":0,"maxResults":400,"expand":["renderedFields","transitions","operations","changelog"]}'
	var (
		from time.Time
		to   *time.Time
	)
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
	} else {
		from = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	to = ctx.DateTo
	url := j.URL + JiraAPIRoot + JiraAPISearch
	startAt := int64(0)
	maxResults := int64(j.PageSize)
	jql := ""
	epochMS := from.UnixNano() / 1e6
	if to != nil {
		epochToMS := (*to).UnixNano() / 1e6
		if ctx.Project != "" {
			jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d AND updated < %d order by updated asc"`, ctx.Project, epochMS, epochToMS)
		} else {
			jql = fmt.Sprintf(`"jql":"updated > %d AND updated < %d order by updated asc"`, epochMS, epochToMS)
		}
	} else {
		if ctx.Project != "" {
			jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
		} else {
			jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
		}
	}
	expand := `"expand":["renderedFields","transitions","operations","changelog"]`
	allIssues := []interface{}{}
	var allIssuesMtx *sync.Mutex
	var escha []chan error
	var eschaMtx *sync.Mutex
	var chE chan error
	if thrN > 1 {
		chE = make(chan error)
		allIssuesMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	method := Post
	var headers map[string]string
	if j.Token != "" {
		// Token should be BASE64("useremail:api_token"), see: https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis
		headers = map[string]string{"Content-Type": "application/json", "Authorization": "Basic " + j.Token}
	} else {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	for {
		payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s,%s}`, startAt, maxResults, jql, expand))
		var res interface{}
		res, _, err = Request(
			ctx,
			url,
			method,
			headers,
			payloadBytes,
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200, 404
		)
		if err != nil {
			return
		}
		if !fieldsFetched {
			err = <-chF
			if err != nil {
				Printf("GetFields error: %+v\n", err)
				return
			}
			fieldsFetched = true
		}
		processIssues := func(c chan error) (e error) {
			defer func() {
				if c != nil {
					c <- e
				}
			}()
			issues, ok := res.(map[string]interface{})["issues"].([]interface{})
			if !ok {
				e = fmt.Errorf("unable to unmarshal issues from %+v", res)
				return
			}
			if ctx.Debug > 0 {
				Printf("Processing %d issues\n", len(issues))
			}
			for _, issue := range issues {
				var esch chan error
				esch, e = j.ProcessIssue(ctx, &allIssues, allIssuesMtx, issue, customFields, from, to, thrN)
				if e != nil {
					Printf("Error %v processing issue: %+v\n", e, issue)
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
			}
			return
		}
		if thrN > 1 {
			go func() {
				_ = processIssues(chE)
			}()
			nThreads++
			if nThreads == thrN {
				err = <-chE
				if err != nil {
					return
				}
				nThreads--
			}
		} else {
			err = processIssues(nil)
			if err != nil {
				return
			}
		}
		totalF, ok := res.(map[string]interface{})["total"].(float64)
		if !ok {
			err = fmt.Errorf("unable to unmarshal total from %+v", res)
			return
		}
		maxResultsF, ok := res.(map[string]interface{})["maxResults"].(float64)
		if !ok {
			err = fmt.Errorf("unable to maxResults total from %+v", res)
			return
		}
		total := int64(totalF)
		maxResults = int64(maxResultsF)
		inc := int64(totalF)
		if maxResultsF < totalF {
			inc = int64(maxResultsF)
		}
		startAt += inc
		if startAt >= total {
			startAt = total
			break
		}
		if ctx.Debug > 0 {
			Printf("Processing next issues page from %d/%d\n", startAt, total)
		}
	}
	for thrN > 1 && nThreads > 0 {
		err = <-chE
		nThreads--
		if err != nil {
			return
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nIssues := len(allIssues)
	if ctx.Debug > 0 {
		Printf("Remain %d issues to send to ElasticSearch\n", nIssues)
	}
	if nIssues > 0 {
		err = SendToElastic(ctx, j, true, "uuid", allIssues)
		if err != nil {
			Printf("Error %v sending %d issues to ElasticSearch\n", err, len(allIssues))
		}
	}
	Printf("Processed %d issues\n", startAt)
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSJira) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSJira) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSJira) DateField(*Ctx) string {
	return DefaultDateField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSJira) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

//Categories - return a set of configured categories
func (j *DSJira) Categories() map[string]struct{} {
	return map[string]struct{}{"issue": {}}
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
// Jira usually stores only one instance per index, so we don't need to enable filtering by origin to resume
func (j *DSJira) ResumeNeedsOrigin() bool {
	return false
}

// Origin - return current origin
func (j *DSJira) Origin() string {
	return j.URL
}

// ItemID - return unique identifier for an item
func (j *DSJira) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})["id"].(string)
	if !ok {
		Fatalf("%s: ItemID() - cannot extract id from %+v", j.DS, item)
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSJira) ItemUpdatedOn(item interface{}) time.Time {
	fields, ok := item.(map[string]interface{})["fields"].(map[string]interface{})
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract fields from %+v", j.DS, item)
	}
	sUpdated, ok := fields["updated"].(string)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract updated from %+v", j.DS, fields)
	}
	updated, err := TimeParseES(sUpdated)
	FatalOnError(err)
	return updated
}

// ItemCategory - return unique identifier for an item
func (j *DSJira) ItemCategory(item interface{}) string {
	return Issue
}

// SearchFields - define (optional) search fields to be returned
func (j *DSJira) SearchFields() map[string][]string {
	return JiraSearchFields
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSJira) ElasticRawMapping() []byte {
	return JiraRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSJira) ElasticRichMapping() []byte {
	return JiraRichMapping
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSJira) GetItemIdentities(doc interface{}) (identities map[[3]string]struct{}, err error) {
	fields, ok := doc.(map[string]interface{})["data"].(map[string]interface{})["fields"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("cannot read data.fields from doc %+v", doc)
		return
	}
	init := false
	for _, field := range []string{"assignee", "reporter", "creator"} {
		f, ok := fields[field].(map[string]interface{})
		if !ok {
			// Printf("field %s not found\n", field)
			continue
		}
		any := false
		identity := [3]string{}
		for i, k := range []string{"displayName", "name", "emailAddress"} {
			v, ok := f[k].(string)
			// Printf("%d: (%s,%s) -> (%s, %v)\n", i, field, k, v, ok)
			if ok {
				identity[i] = v
				any = true
			} else {
				identity[i] = Nil
			}
		}
		if any {
			if !init {
				identities = make(map[[3]string]struct{})
				init = true
			}
			identities[identity] = struct{}{}
		}
	}
	return
}
