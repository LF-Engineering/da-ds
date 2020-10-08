package dads

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// GroupsioBackendVersion - backend version
	GroupsioBackendVersion = "0.0.0"
	// GroupsioURLRoot - root url for group name origin
	GroupsioURLRoot = "https://groups.io/g/"
	// GroupsioDefaultSearchField - default search field
	// GroupsioDefaultSearchField = "item_id"
)

var (
	// GroupsioRawMapping - Groupsio raw index mapping
	GroupsioRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}`)
	// GroupsioRichMapping - Groupsio rich index mapping
	GroupsioRichMapping = []byte(`{"properties":{"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}`)
)

// DSGroupsio - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGroupsio struct {
	DS          string
	GroupName   string // From DA_GROUPSIO_URL - Jira URL
	NoSSLVerify bool   // From DA_GROUPSIO_NO_SSL_VERIFY
	Email       string // From DA_GROUPSIO_EMAIL
	Password    string // From DA_GROUPSIO_PASSWORD
	PageSize    int    // From DA_GROUPSIO_PAGE_SIZE
	MultiOrigin bool   // FROM DA_GROUPSIO_MULTI_ORIGIN
}

// ParseArgs - parse stub specific environment variables
func (j *DSGroupsio) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Groupsio
	prefix := "DA_GROUPSIO_"
	j.GroupName = os.Getenv(prefix + "GROUP_NAME")
	j.NoSSLVerify = os.Getenv(prefix+"NO_SSL_VERIFY") != ""
	j.Email = os.Getenv(prefix + "EMAIL")
	j.Password = os.Getenv(prefix + "PASSWORD")
	if os.Getenv(prefix+"PAGE_SIZE") == "" {
		j.PageSize = 500
	} else {
		pageSize, err := strconv.Atoi(os.Getenv(prefix + "PAGE_SIZE"))
		FatalOnError(err)
		if pageSize > 0 {
			j.PageSize = pageSize
		}
	}
	j.MultiOrigin = os.Getenv(prefix+"MULTI_ORIGIN") != ""
	if j.NoSSLVerify {
		NoSSLVerify()
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSGroupsio) Validate() (err error) {
	url := strings.TrimSpace(j.GroupName)
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}
	ary := strings.Split(url, "/")
	j.GroupName = ary[len(ary)-1]
	if j.GroupName == "" {
		err = fmt.Errorf("Group name must be set: [https://groups.io/g/]GROUP+channel")
	}
	return
}

// Name - return data source name
func (j *DSGroupsio) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSGroupsio) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSGroupsio) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (j *DSGroupsio) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSGroupsio) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (j *DSGroupsio) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSGroupsio) FetchItems(ctx *Ctx) (err error) {
	Printf("DSGroupsio.FetchItems\n")
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSGroupsio) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSGroupsio) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSGroupsio) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSGroupsio) RichIDField(*Ctx) string {
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSGroupsio) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSGroupsio) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSGroupsio) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSGroupsio) Categories() map[string]struct{} {
	return map[string]struct{}{}
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSGroupsio) ResumeNeedsOrigin(ctx *Ctx) bool {
	return j.MultiOrigin
}

// Origin - return current origin
func (j *DSGroupsio) Origin(ctx *Ctx) string {
	if ctx.Tag != "" {
		return ctx.Tag
	}
	return GroupsioURLRoot + j.GroupName
}

// ItemID - return unique identifier for an item
func (j *DSGroupsio) ItemID(item interface{}) string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGroupsio) ItemUpdatedOn(item interface{}) time.Time {
	return time.Now()
}

// ItemCategory - return unique identifier for an item
func (j *DSGroupsio) ItemCategory(item interface{}) string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// SearchFields - define (optional) search fields to be returned
func (j *DSGroupsio) SearchFields() map[string][]string {
	return map[string][]string{}
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSGroupsio) ElasticRawMapping() []byte {
	return GroupsioRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSGroupsio) ElasticRichMapping() []byte {
	return GroupsioRichMapping
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGroupsio) GetItemIdentities(ctx *Ctx, doc interface{}) (map[[3]string]struct{}, error) {
	return map[[3]string]struct{}{}, nil
}

// EnrichItems - perform the enrichment
func (j *DSGroupsio) EnrichItems(ctx *Ctx) (err error) {
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGroupsio) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool) (rich map[string]interface{}, err error) {
	rich = item
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGroupsio) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGroupsio) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for Jira backend
func (j *DSGroupsio) AllRoles(ctx *Ctx) []string {
	return []string{"author"}
}
