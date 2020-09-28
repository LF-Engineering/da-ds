package dads

import (
	"fmt"
	"os"
	"time"
)

// DSJira - DS implementation for Jira
type DSJira struct {
	DS          string
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

// FetchRaw - implement fetch raw data for Jira
func (j *DSJira) FetchRaw(ctx *Ctx) (lastData *time.Time, err error) {
	return
}

// Enrich - implement enrich data for Jira
func (j *DSJira) Enrich(ctx *Ctx, startFrom *time.Time) (err error) {
	return
}
