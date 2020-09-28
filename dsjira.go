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

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSJira) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for Jira
func (j *DSJira) FetchRaw(ctx *Ctx) (lastData *time.Time, err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSJira) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for Jira
func (j *DSJira) Enrich(ctx *Ctx, startFrom *time.Time) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
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
