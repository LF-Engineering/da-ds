package dads

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	neturl "net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// GroupsioBackendVersion - backend version
	GroupsioBackendVersion = "0.0.0"
	// GroupsioURLRoot - root url for group name origin
	GroupsioURLRoot = "https://groups.io/g/"
	// GroupsioAPIURL - Groups.io API URL
	GroupsioAPIURL = "https://groups.io/api/v1"
	// GroupsioAPILogin - login API
	GroupsioAPILogin = "/login"
	// GroupsioAPIGetsubs - getsubs API
	GroupsioAPIGetsubs = "/getsubs"
	// GroupsioAPIDownloadArchives - download archives API
	GroupsioAPIDownloadArchives = "/downloadarchives"
	// GroupsioDefaultArchPath - default path where archives are stored
	GroupsioDefaultArchPath = "$HOME/.perceval/mailinglists"
	// GroupsioMBoxFile - default messages file name
	GroupsioMBoxFile = "messages.zip"
	// GroupsioMessageIDField - message ID field from email
	GroupsioMessageIDField = "Message-ID"
	// GroupsioMessageDateField - message ID field from email
	GroupsioMessageDateField = "Date"
	// GroupsioDefaultSearchField - default search field
	GroupsioDefaultSearchField = "item_id"
)

var (
	// GroupsioRawMapping - Groupsio raw index mapping
	GroupsioRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}`)
	// GroupsioRichMapping - Groupsio rich index mapping
	GroupsioRichMapping = []byte(`{"properties":{"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}`)
	// GroupsioCategories - categories defined for Groupsio
	GroupsioCategories = map[string]struct{}{"message": {}}
	// GroupsioMBoxMsgSeparator - used to split mbox file into separate messages
	GroupsioMBoxMsgSeparator = []byte("\nFrom ")
)

// DSGroupsio - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGroupsio struct {
	DS           string
	GroupName    string // From DA_GROUPSIO_URL - Group name like GROUP-topic
	NoSSLVerify  bool   // From DA_GROUPSIO_NO_SSL_VERIFY
	Email        string // From DA_GROUPSIO_EMAIL
	Password     string // From DA_GROUPSIO_PASSWORD
	MultiOrigin  bool   // From DA_GROUPSIO_MULTI_ORIGIN - allow multiple groups in a single index
	SaveArchives bool   // From DA_GROUPSIO_SAVE_ARCHIVES
	ArchPath     string // From DA_GROUPSIO_ARCH_PATH - default GroupsioDefaultArchPath
}

// ParseArgs - parse stub specific environment variables
func (j *DSGroupsio) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Groupsio
	prefix := "DA_GROUPSIO_"
	j.GroupName = os.Getenv(prefix + "GROUP_NAME")
	j.NoSSLVerify = os.Getenv(prefix+"NO_SSL_VERIFY") != ""
	j.Email = os.Getenv(prefix + "EMAIL")
	j.Password = os.Getenv(prefix + "PASSWORD")
	AddRedacted(j.Email, false)
	AddRedacted(j.Password, false)
	AddRedacted(neturl.QueryEscape(j.Email), false)
	AddRedacted(neturl.QueryEscape(j.Password), false)
	j.MultiOrigin = os.Getenv(prefix+"MULTI_ORIGIN") != ""
	j.SaveArchives = os.Getenv(prefix+"SAVE_ARCHIVES") != ""
	if os.Getenv(prefix+"ARCH_PATH") != "" {
		j.ArchPath = os.Getenv(prefix + "ARCH_PATH")
	} else {
		j.ArchPath = GroupsioDefaultArchPath
	}
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
	j.ArchPath = os.ExpandEnv(j.ArchPath)
	if strings.HasSuffix(j.ArchPath, "/") {
		j.ArchPath = j.ArchPath[:len(j.ArchPath)-1]
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

// AddMetadata - add metadata to the item
func (j *DSGroupsio) AddMetadata(ctx *Ctx, msg interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := GroupsioURLRoot + j.GroupName
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	msgID := j.ItemID(msg)
	updatedOn := j.ItemUpdatedOn(msg)
	uuid := UUIDNonEmpty(ctx, origin, msgID)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = GroupsioBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem["updated_on"] = updatedOn
	mItem["category"] = j.ItemCategory(msg)
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", GroupsioDefaultSearchField}, msgID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "group_name"}, j.GroupName, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSGroupsio) FetchItems(ctx *Ctx) (err error) {
	var dirPath string
	if j.SaveArchives {
		dirPath := j.ArchPath + "/" + GroupsioURLRoot + j.GroupName
		dirPath, err = EnsurePath(dirPath)
		FatalOnError(err)
		Printf("path to store mailing archives: %s\n", dirPath)
	} else {
		Printf("processing erchives in memory, archive file not saved\n")
	}
	// Login to groups.io
	method := Get
	url := GroupsioAPIURL + GroupsioAPILogin + `?email=` + neturl.QueryEscape(j.Email) + `&password=` + neturl.QueryEscape(j.Password)
	// headers := map[string]string{"Content-Type": "application/json"}
	// By checking cookie expiration data I know that I can (probably) cache this even for 14 days
	// In that case other dads groupsio instances will reuse login data from L2 cache :-D
	// But we cache for 48 hours at most, because new subscriptions are added
	cacheLoginDur := time.Duration(48) * time.Hour
	var res interface{}
	var cookies []string
	res, _, cookies, err = Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		false,                               // retry
		&cacheLoginDur,                      // cache duration
		false,                               // skip in dry-run mode
	)
	if err != nil {
		return
	}
	type Result struct {
		User struct {
			Token string `json:"csrf_token"`
			Subs  []struct {
				GroupID   int64  `json:"group_id"`
				GroupName string `json:"group_name"`
				Perms     struct {
					DownloadArchives bool `json:"download_archives"`
				} `json:"perms"`
			} `json:"subscriptions"`
		} `json:"user"`
	}
	var result Result
	err = jsoniter.Unmarshal(res.([]byte), &result)
	if err != nil {
		Printf("Cannot unmarshal result from %s\n", string(res.([]byte)))
		return
	}
	groupID := int64(-1)
	for _, sub := range result.User.Subs {
		if sub.GroupName == j.GroupName {
			if !sub.Perms.DownloadArchives {
				Fatalf("download archives not enabled on %s (group id %d)\n", sub.GroupName, sub.GroupID)
				return
			}
			groupID = sub.GroupID
			break
		}
	}
	if groupID < 0 {
		subs := []string{}
		for _, sub := range result.User.Subs {
			subs = append(subs, sub.GroupName)
		}
		sort.Strings(subs)
		Fatalf("you are not subscribed to %s, your subscriptions(%d): %v\n", j.GroupName, len(subs), strings.Join(subs, ", "))
		return
	}
	Printf("found group ID %d\n", groupID)
	// We do have cookies now (from either real request or from the L2 cache)
	//url := GroupsioAPIURL + GroupsioAPILogin + `?email=` + neturl.QueryEscape(j.Email) + `&password=` + neturl.QueryEscape(j.Password)
	url = GroupsioAPIURL + GroupsioAPIDownloadArchives + `?group_id=` + fmt.Sprintf("%d", groupID)
	var from time.Time
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
		from = from.Add(-1 * time.Second)
		url += `&start_time=` + neturl.QueryEscape(ToYMDTHMSZDate(from))
	}
	Printf("fetching messages from: %s\n", url)
	// FIXME: remove caching or lower to 3 hours at most
	cacheMsgDur := time.Duration(48) * time.Hour
	res, _, _, err = Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		cookies,
		nil,
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		false,                               // retry
		&cacheMsgDur,                        // cache duration
		false,                               // skip in dry-run mode
	)
	if err != nil {
		return
	}
	nBytes := int64(len(res.([]byte)))
	if j.SaveArchives {
		path := dirPath + "/" + GroupsioMBoxFile
		err = ioutil.WriteFile(path, res.([]byte), 0644)
		if err != nil {
			return
		}
		Printf("written %s (%d bytes)\n", path, nBytes)
	} else {
		Printf("read %d bytes\n", nBytes)
	}
	bytesReader := bytes.NewReader(res.([]byte))
	var zipReader *zip.Reader
	zipReader, err = zip.NewReader(bytesReader, nBytes)
	if err != nil {
		return
	}
	var messages [][]byte
	for _, file := range zipReader.File {
		var rc io.ReadCloser
		rc, err = file.Open()
		if err != nil {
			return
		}
		var data []byte
		data, err = ioutil.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return
		}
		Printf("%s uncomressed %d bytes\n", file.Name, len(data))
		ary := bytes.Split(data, GroupsioMBoxMsgSeparator)
		Printf("%s # of messages: %d\n", file.Name, len(ary))
		messages = append(messages, ary...)
	}
	Printf("number of messages to parse: %d\n", len(messages))
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
		nBytes := len(msg)
		if nBytes < len(GroupsioMBoxMsgSeparator) {
			return
		}
		if !bytes.HasPrefix(msg, GroupsioMBoxMsgSeparator[1:]) {
			msg = append(GroupsioMBoxMsgSeparator[1:], msg...)
		}
		if ctx.Debug > 1 {
			Printf("message length %d\n", len(msg))
		}
		var (
			valid   bool
			message map[string]interface{}
		)
		// FIXME: implement
		message, valid, e = ParseMBoxMsg(msg)
		if e != nil || !valid {
			return
		}
		esItem := j.AddMetadata(ctx, message)
		if ctx.Project != "" {
			message["project"] = ctx.Project
		}
		esItem["data"] = message
		// Real data processing here
		if allMsgsMtx != nil {
			allMsgsMtx.Lock()
		}
		allMsgs = append(allMsgs, esItem)
		nMsgs := len(allMsgs)
		if nMsgs >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				if ctx.Debug > 0 {
					Printf("sending %d items to elastic\n", len(allMsgs))
				}
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
				var esch chan error
				esch, err = processMsg(ch, msg)
				if err != nil {
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
		if ctx.Debug > 0 {
			Printf("joining %d threads\n", nThreads)
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
	if ctx.Debug > 0 {
		Printf("%d wait channels\n", len(escha))
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
	return GroupsioCategories
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
	id, ok := item.(map[string]interface{})[GroupsioMessageIDField].(string)
	if !ok {
		Fatalf("%s: ItemID() - cannot extract %s from %+v", j.DS, GroupsioMessageIDField, jDumpKeys(item))
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGroupsio) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := Dig(item, []string{GroupsioMessageDateField}, true, false)
	sUpdated, ok := iUpdated.(string)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %+v", j.DS, GroupsioMessageDateField, DumpKeys(item))
	}
	updated, err := TimeParseES(sUpdated)
	FatalOnError(err)
	return updated
}

// ItemCategory - return unique identifier for an item
func (j *DSGroupsio) ItemCategory(item interface{}) string {
	return Message
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

// AllRoles - return all roles defined for Groupsio backend
func (j *DSGroupsio) AllRoles(ctx *Ctx) []string {
	return []string{"author"}
}
