package dockerhub

import (
	"encoding/json"
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	jsoniter "github.com/json-iterator/go"
	"net/http"
	"time"
)

const (
	// DockerhubAPIURL - dockerhub API URL
	DockerhubAPIURL          = "https://hub.docker.com/v2"
	DockerhubAPILogin        = "users/login"
	DockerhubAPIRepositories = "repositories"

	// Dockerhub - common constant string
	Dockerhub string = "dockerhub"
)

var (
	// DockerhubSearchFields - extra search fields
	DockerhubSearchFields = map[string][]string{
		"name":      {"name"},
		"namespace": {"namespace"},
	}
	// DockerhubRawMapping - Jira raw index mapping
	DockerhubRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}`)
	// DockerhubRichMapping - Jira rich index mapping
	DockerhubRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"description":{"type":"text","index":true},"description_analyzed":{"type":"text","index":true},"full_description_analyzed":{"type":"text","index":true}}}`)
)

// Fetcher contains dockerhub datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	IncludeArchived       bool
	MultiOrigin           bool // can we store multiple endpoints in a single index?
	HttpClientProvider    HttpClientProvider
	ElasticSearchProvider ESClientProvider
	Username              string
	Password              string
	BackendVersion        string
}

// DockerHubParams ...
type DockerhubParams struct {
	Username       string
	Password       string
	BackendVersion string
}

// HttpClientProvider ...
type HttpClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte) (statusCode int, resBody []byte, err error)
}

// ESClientProvider ...
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
}

// NewFetcher initiates a new dockerhub fetcher
func NewFetcher(params *DockerhubParams, httpClientProvider HttpClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Dockerhub,
		HttpClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		Username:              params.Username,
		Password:              params.Password,
	}
}

// Validate dockerhub datasource configuration
func (f *Fetcher) Validate() error {

	return nil
}

// todo: to be reviewed
// Name - return data source name
func (f *Fetcher) Name() string {
	return f.DSName
}

// Info - return DS configuration in a human readable form
func (f *Fetcher) Info() string {
	return fmt.Sprintf("%+v", f)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (f *Fetcher) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (f *Fetcher) FetchRaw() (err error) {
	dads.Printf("%s should use generic FetchRaw()\n", f.DSName)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (f *Fetcher) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (f *Fetcher) Enrich() (err error) {
	dads.Printf("%s should use generic Enrich()\n", f.DSName)
	return
}

func (f *Fetcher) login(username string, password string) (string, error) {
	url := fmt.Sprintf("%s/%s", DockerhubAPIURL, DockerhubAPILogin)

	payload := make(map[string]interface{})
	payload["username"] = username
	payload["password"] = password

	p, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	dads.Printf("dockerhub login via: %s\n", url)

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "Post", nil, p)

	if statusCode == http.StatusOK {
		res := LoginResponse{}
		err = jsoniter.Unmarshal(resBody, &res)
		if err != nil {
			fmt.Printf("Cannot unmarshal result from %s\n", string(resBody))
			return "", err
		}
	}

	return "", errors.New("invalid login credentials")
}

// FetchItems ...
func (f *Fetcher) FetchItems(owner string, repository string) error {
	// login
	token := ""

	if f.Password != "" {
		t, err := f.login(f.Username, f.Password)
		if err != nil {
			return err
		}
		token = t
	}

	// todo: call repository api
	url := fmt.Sprintf("%s/%s/%s/%s", DockerhubAPIURL, DockerhubAPIRepositories, owner, repository)
	headers := map[string]string{}
	if token != "" {
		headers["Authorization"] = fmt.Sprintf("JWT %s", token)
	}

	statusCode, resBody, err := f.HttpClientProvider.Request(url, "GET", headers, nil)
	if err != nil || statusCode != http.StatusOK {
		return errors.New("invalid request")
	}

	// todo: is there any required process befor saving into ES?

	// todo: save result to elastic search (raw document)
	index := fmt.Sprintf("sds-%s-%s-dockerhub-raw", owner, repository)
	esRes, err := f.ElasticSearchProvider.Add(index, fmt.Sprintf("test-id-%v", time.Now().Unix()), resBody)
	if err != nil {
		return err
	}

	fmt.Println(string(esRes))
	return nil
}

/*// FetchItems - implement enrich data for stub datasource
func (j *DSDockerhub) FetchItems(ctx *Ctx) (err error) {
	// Login to groups.io
	method := Post
	url := DockerhubAPIURL + DockerhubAPILogin

	cacheLoginDur := time.Duration(24)*time.Hour + time.Duration(5)*time.Minute
	var res interface{}
	body := make(map[string]interface{})
	body["username"] = j.UserName
	body["password"] = j.Password

	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	fmt.Printf("dockerhub login via: %s\n", url)

	res, _, _, err = Request(
		ctx,
		url,
		method,
		map[string]string{"Content": "Content-Type: application/json"},
		b,
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		false,                               // retry
		&cacheLoginDur,                      // cache duration
		false,                               // skip in dry-run mode
	)
	if err != nil {
		return err
	}

	result := make(map[string]interface{})
	err = jsoniter.Unmarshal(res.([]byte), &result)
	if err != nil {
		fmt.Printf("Cannot unmarshal result from %s\n", string(res.([]byte)))
		return err
	}

	var messages [][]byte
	// Process messages (possibly in threads)
	var (
		ch         chan error
		allMsgs    []interface{}
		allMsgsMtx *sync.Mutex
		escha      []chan error
		eschaMtx   *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allMsgsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processMsg := func(c chan error, msg []byte) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// FIXME: Real data processing here
		item := map[string]interface{}{"id": time.Now().UnixNano(), "name": "xyz"}
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
		if allMsgsMtx != nil {
			allMsgsMtx.Lock()
		}
		allMsgs = append(allMsgs, item)
		nMsgs := len(allMsgs)
		if nMsgs >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allMsgs)
				if ee != nil {
					Printf("error %v sending %d messages to ElasticSearch\n", ee, len(allMsgs))
				}
				allMsgs = []interface{}{}
				if allMsgsMtx != nil {
					allMsgsMtx.Unlock()
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
			if allMsgsMtx != nil {
				allMsgsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for _, message := range messages {
			go func(msg []byte) {
				var (
					e    error
					esch chan error
				)
				esch, e = processMsg(ch, msg)
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
			}(message)
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
	} else {
		for _, message := range messages {
			_, err = processMsg(nil, message)
			if err != nil {
				return
			}
		}
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			return
		}
	}
	nMsgs := len(allMsgs)
	if ctx.Debug > 0 {
		Printf("%d remaining messages to send to ES\n", nMsgs)
	}
	if nMsgs > 0 {
		err = SendToElastic(ctx, j, true, UUID, allMsgs)
		if err != nil {
			Printf("Error %v sending %d messages to ES\n", err, len(allMsgs))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSDockerhub) SupportDateFrom() bool {
	// IMPL:
	return false
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSDockerhub) SupportOffsetFrom() bool {
	// IMPL:
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSDockerhub) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSDockerhub) RichIDField(*Ctx) string {
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSDockerhub) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSDockerhub) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSDockerhub) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSDockerhub) Categories() map[string]struct{} {
	// IMPL:
	return map[string]struct{}{}
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSDockerhub) ResumeNeedsOrigin(ctx *Ctx) bool {
	return j.MultiOrigin
}

// Origin - return current origin
func (j *DSDockerhub) Origin(ctx *Ctx) string {
	// IMPL: you must change this, for example to j.URL/j.GroupName or somethign like this
	return ctx.Tag
}

// ItemID - return unique identifier for an item
func (j *DSDockerhub) ItemID(item interface{}) string {
	// IMPL:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// AddMetadata - add metadata to the item
func (j *DSDockerhub) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	// IMPL:
	mItem = make(map[string]interface{})
	origin := "TODO"
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = "0.0.0"
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem["updated_on"] = updatedOn
	mItem["category"] = j.ItemCategory(item)
	//mItem["search_fields"] = j.GenSearchFields(ctx, issue, uuid)
	//mItem["search_fields"] = make(map[string]interface{})
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSDockerhub) ItemUpdatedOn(item interface{}) time.Time {
	// IMPL:
	return time.Now()
}

// ItemCategory - return unique identifier for an item
func (j *DSDockerhub) ItemCategory(item interface{}) string {
	// IMPL:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSDockerhub) ElasticRawMapping() []byte {
	// IMPL:
	return []byte{}
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSDockerhub) ElasticRichMapping() []byte {
	// IMPL:
	return []byte{}
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSDockerhub) GetItemIdentities(ctx *Ctx, doc interface{}) (map[[3]string]struct{}, error) {
	// IMPL:
	return map[[3]string]struct{}{}, nil
}

// DockerhubEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func DockerhubEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	// IMPL:
	if ctx.Debug > 0 {
		Printf("stub enrich items %d/%d func\n", len(items), len(*docs))
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

return
}
if thrN > 1 {
for i := range items {
go func (i int) {
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
func (j *DSDockerhub) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, DockerhubEnrichItemsFunc, nil)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSDockerhub) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	// IMPL:
	rich = item
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSDockerhub) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSDockerhub) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSDockerhub) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	// IMPL:
	return []string{Author}, true
}
*/
