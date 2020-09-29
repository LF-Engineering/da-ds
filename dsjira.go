package dads

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
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
	var req *http.Request
	req, err = http.NewRequest(method, url, nil)
	if err != nil {
		Printf("New request error: %+v for %s url: %s\n", err, method, url)
		return
	}
	if j.Token != "" {
		req.Header.Set("Authorization", "Basic "+j.Token)
	}
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		Printf("Do request error: %+v for %s url: %s\n", err, method, url)
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		Printf("ReadAll request error: %+v for %s url: %s\n", err, method, url)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		Printf("Method:%s url:%s status:%d query:%s\n%s\n", method, url, resp.StatusCode, body)
		return
	}
	var fields []JiraField
	err = jsoniter.Unmarshal(body, &fields)
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

// ProcessIssue - process a single issue
func (j *DSJira) ProcessIssue(ctx *Ctx, issue interface{}, customFields map[string]JiraField, from time.Time, thrN int) (err error) {
	var mtx *sync.RWMutex
	if thrN > 1 {
		mtx = &sync.RWMutex{}
	}
	processIssue := func(c chan error) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		if thrN > 1 {
			mtx.RLock()
		}
		sID, ok := issue.(map[string]interface{})["id"].(string)
		if thrN > 1 {
			mtx.RUnlock()
		}
		if !ok {
			e = fmt.Errorf("unable to unmarshal id from issue %+v", issue)
			return
		}
		iID, e := strconv.Atoi(sID)
		if e != nil {
			e = fmt.Errorf("unable to unmarshal id from string %s", sID)
			return
		}
		if ctx.Debug > 1 {
			Printf("Issue ID: %d\n", iID)
		}
		url := j.URL + JiraAPIRoot + JiraAPIIssue + "/" + sID + JiraAPIComment
		startAt := int64(0)
		maxResults := int64(j.PageSize)
		epochMS := from.UnixNano() / 1e6
		/*
			    // I think we don't need project filter there, because the entire issue belongs to roject or not
			    // So I'm only using date filter
					jql := ""
					if ctx.Project != "" {
						jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
					} else {
						jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
					}
		*/
		jql := fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
		method := Get
		for {
			payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s}`, startAt, maxResults, jql))
			payloadBody := bytes.NewReader(payloadBytes)
			var req *http.Request
			req, err = http.NewRequest(method, url, payloadBody)
			if err != nil {
				Printf("New request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
				return
			}
			req.Header.Set("Content-Type", "application/json")
			if j.Token != "" {
				req.Header.Set("Authorization", "Basic "+j.Token)
			}
			var resp *http.Response
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				Printf("Do request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
				return
			}
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				Printf("ReadAll request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				Printf("Method:%s url:%s status:%d query:%s\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
				return
			}
			var res interface{}
			err = jsoniter.Unmarshal(body, &res)
			if err != nil {
				return
			}
			comments, ok := res.(map[string]interface{})["comments"].([]interface{})
			if !ok {
				e = fmt.Errorf("unable to unmarshal comments from %+v", res)
				return
			}
			if ctx.Debug > 0 {
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
			return err
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
	if thrN > 1 {
		err = <-ch
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
	var from time.Time
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
	} else {
		from = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	url := j.URL + JiraAPIRoot + JiraAPISearch
	startAt := int64(0)
	maxResults := int64(j.PageSize)
	jql := ""
	epochMS := from.UnixNano() / 1e6
	if ctx.Project != "" {
		jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
	} else {
		jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
	}
	expand := `"expand":["renderedFields","transitions","operations","changelog"]`
	var chE chan error
	if thrN > 1 {
		chE = make(chan error)
	}
	nThreads := 0
	method := Post
	for {
		payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s,%s}`, startAt, maxResults, jql, expand))
		payloadBody := bytes.NewReader(payloadBytes)
		var req *http.Request
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			Printf("New request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if j.Token != "" {
			// Token should be BASE64("useremail:api_token"), see: https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis
			req.Header.Set("Authorization", "Basic "+j.Token)
		}
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			Printf("Do request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			Printf("ReadAll request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			Printf("Method:%s url:%s status:%d query:%s\n%s\n", method, url, resp.StatusCode, string(payloadBytes), body)
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
		var res interface{}
		err = jsoniter.Unmarshal(body, &res)
		if err != nil {
			return
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
				er := j.ProcessIssue(ctx, issue, customFields, from, thrN)
				if er != nil {
					Printf("Error %v processing issue: %+v\n", er, issue)
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
