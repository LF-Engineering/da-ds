package dads

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// DSJira - DS implementation for Jira
type DSJira struct {
	DS          string
	APIRoot     string
	APISearch   string
	URL         string // From DA_JIRA_URL - Jira URL
	NoSSLVerify bool   // From DA_JIRA_NO_SSL_VERIFY
	User        string // From DA_JIRA_USER
	Pass        string // From DA_JIRA_PASS
}

// ParseArgs - parse jira specific environment variables
func (j *DSJira) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Jira

	// Jira specific env variables
	j.URL = os.Getenv("DA_JIRA_URL")
	j.NoSSLVerify = os.Getenv("DA_JIRA_NO_SSL_VERIFY") != ""
	j.User = os.Getenv("DA_JIRA_USER")
	j.Pass = os.Getenv("DA_JIRA_PASS")
	j.APIRoot = "/rest/api/2"
	j.APISearch = "/search"
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

// FetchItems - implement enrich data for jira datasource
func (j *DSJira) FetchItems(ctx *Ctx) (err error) {
	// '{"jql":"updated > 1601281314000 order by updated asc","startAt":0,"maxResults":100,"expand":["renderedFields","transitions","operations","changelog"]}'
	var from time.Time
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
	} else {
		from = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	url := j.URL + j.APIRoot + j.APISearch
	startAt := 0
	maxResults := 1000
	jql := ""
	epochMS := from.UnixNano() / 1e6
	if ctx.Project != "" {
		jql = fmt.Sprintf(`"jql":"project = %s AND updated > %d order by updated asc"`, ctx.Project, epochMS)
	} else {
		jql = fmt.Sprintf(`"jql":"updated > %d order by updated asc"`, epochMS)
	}
	expand := `"expand":["renderedFields","transitions","operations","changelog"]`
	for {
		payloadBytes := []byte(fmt.Sprintf(`{"startAt":%d,"maxResults":%d,%s,%s}`, startAt, maxResults, jql, expand))
		payloadBody := bytes.NewReader(payloadBytes)
		method := Get
		var req *http.Request
		req, err = http.NewRequest(method, url, payloadBody)
		fmt.Printf("%s/%+v\n", url, string(payloadBytes))
		if err != nil {
			Printf("New request error: %+v for %s url: %s, query: %s\n", err, method, url, string(payloadBytes))
			return
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
		type result struct {
			Total int `json:"total"`
			Max   int `json:"maxResults"`
		}
		var res result
		err = jsoniter.Unmarshal(body, &res)
		fmt.Printf("%+v\n", res)
		break
	}
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
