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
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// GroupsioBackendVersion - backend version
	GroupsioBackendVersion = "0.1.3"
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
	GroupsioMessageIDField = "message-id"
	// GroupsioMessageDateField - message ID field from email
	GroupsioMessageDateField = "date"
	// GroupsioMessageReceivedField - message Received filed
	GroupsioMessageReceivedField = "received"
	// GroupsioDefaultSearchField - default search field
	GroupsioDefaultSearchField = "item_id"
	// GroupsioMaxMessageBodyLength - trucacte message bodies longer than this (per each multi-body email part)
	GroupsioMaxMessageBodyLength = 1000
	// GroupsioMaxRichMessageLines - maximum numbe rof message text/plain lines copied to rich index
	GroupsioMaxRichMessageLines = 10
	// GroupsioMaxRecipients - maximum number of emails parsed from To:
	GroupsioMaxRecipients = 50
	// GroupsioMaxMessageProperties - maximum properties that can be set on the message object
	GroupsioMaxMessageProperties = 255
)

var (
	// GroupsioRawMapping - Groupsio raw index mapping
	GroupsioRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}`)
	// GroupsioRichMapping - Groupsio rich index mapping
	GroupsioRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}`)
	// GroupsioCategories - categories defined for Groupsio
	GroupsioCategories = map[string]struct{}{Message: {}}
	// GroupsioMBoxMsgSeparator - used to split mbox file into separate messages
	GroupsioMBoxMsgSeparator = []byte("\nFrom ")
	// GroupsioMsgLineSeparator - used to split mbox message into its separate lines
	GroupsioMsgLineSeparator = []byte("\r\n")
)

// DSGroupsio - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGroupsio struct {
	DS           string
	GroupName    string // From DA_GROUPSIO_URL - Group name like GROUP-topic
	Email        string // From DA_GROUPSIO_EMAIL
	Password     string // From DA_GROUPSIO_PASSWORD
	NoSSLVerify  bool   // From DA_GROUPSIO_NO_SSL_VERIFY
	SaveArchives bool   // From DA_GROUPSIO_SAVE_ARCHIVES
	ArchPath     string // From DA_GROUPSIO_ARCH_PATH - default GroupsioDefaultArchPath
	MultiOrigin  bool   // From DA_GROUPSIO_MULTI_ORIGIN - allow multiple groups in a single index
}

// ParseArgs - parse stub specific environment variables
func (j *DSGroupsio) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Groupsio
	prefix := "DA_GROUPSIO_"
	j.GroupName = os.Getenv(prefix + "GROUP_NAME")
	j.NoSSLVerify = StringToBool(os.Getenv(prefix + "NO_SSL_VERIFY"))
	j.Email = os.Getenv(prefix + "EMAIL")
	j.Password = os.Getenv(prefix + "PASSWORD")
	AddRedacted(j.Email, false)
	AddRedacted(j.Password, false)
	AddRedacted(neturl.QueryEscape(j.Email), false)
	AddRedacted(neturl.QueryEscape(j.Password), false)
	j.MultiOrigin = StringToBool(os.Getenv(prefix + "MULTI_ORIGIN"))
	j.SaveArchives = StringToBool(os.Getenv(prefix + "SAVE_ARCHIVES"))
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
func (j *DSGroupsio) Validate(ctx *Ctx) (err error) {
	url := strings.TrimSpace(j.GroupName)
	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}
	ary := strings.Split(url, "/")
	j.GroupName = ary[len(ary)-1]
	if j.GroupName == "" {
		err = fmt.Errorf("Group name must be set: [https://groups.io/g/]GROUP+channel")
		return
	}
	if j.Email == "" || j.Password == "" {
		err = fmt.Errorf("Email and Password must be set")
		return
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
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(msg)
	mItem["search_fields"] = make(map[string]interface{})
	FatalOnError(DeepSet(mItem, []string{"search_fields", GroupsioDefaultSearchField}, msgID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "group_name"}, j.GroupName, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	if ctx.Debug > 1 {
		Printf("%s: %s: %v %v\n", origin, uuid, msgID, updatedOn)
	}
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSGroupsio) FetchItems(ctx *Ctx) (err error) {
	var dirPath string
	if j.SaveArchives {
		dirPath = j.ArchPath + "/" + GroupsioURLRoot + j.GroupName
		dirPath, err = EnsurePath(dirPath, false)
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
	// But we cache for 24:05 hours at most, because new subscriptions are added
	cacheLoginDur := time.Duration(24)*time.Hour + time.Duration(5)*time.Minute
	var res interface{}
	var cookies []string
	Printf("groupsio login via: %s\n", url)
	res, _, cookies, _, err = Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
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
		dls := []string{}
		for _, sub := range result.User.Subs {
			subs = append(subs, sub.GroupName)
			if sub.Perms.DownloadArchives {
				dls = append(dls, sub.GroupName)
			}
		}
		sort.Strings(subs)
		sort.Strings(dls)
		Fatalf("you are not subscribed to %s, your subscriptions(%d): %s\ndownload allowed for(%d): %s", j.GroupName, len(subs), strings.Join(subs, ", "), len(dls), strings.Join(dls, ", "))
		return
	}
	Printf("%s found group ID %d\n", j.GroupName, groupID)
	// We do have cookies now (from either real request or from the L2 cache)
	//url := GroupsioAPIURL + GroupsioAPILogin + `?email=` + neturl.QueryEscape(j.Email) + `&password=` + neturl.QueryEscape(j.Password)
	url = GroupsioAPIURL + GroupsioAPIDownloadArchives + `?group_id=` + fmt.Sprintf("%d", groupID)
	var (
		from   time.Time
		status int
	)
	if ctx.DateFrom != nil {
		from = *ctx.DateFrom
		from = from.Add(-1 * time.Second)
	} else {
		from = time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	url += `&start_time=` + neturl.QueryEscape(ToYMDTHMSZDate(from))
	Printf("fetching messages from: %s\n", url)
	// Groups.io blocks downloading archives more often than 24 hours
	cacheMsgDur := time.Duration(24)*time.Hour + time.Duration(5)*time.Minute
	res, status, _, _, err = Request(
		ctx,
		url,
		method,
		nil,
		[]byte{},
		cookies,
		nil,                                 // JSON statuses
		nil,                                 // Error statuses
		map[[2]int]struct{}{{200, 200}: {}}, // OK statuses: 200
		map[[2]int]struct{}{{200, 200}: {}}, // Cache statuses: 200
		false,                               // retry
		&cacheMsgDur,                        // cache duration
		false,                               // skip in dry-run mode
	)
	if status == 429 {
		Fatalf("Too many requests for %s, aborted\n", url)
		return
	}
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
		statMtx    *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allMsgsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	empty := 0
	warns := 0
	invalid := 0
	filtered := 0
	if thrN > 1 {
		statMtx = &sync.Mutex{}
	}
	stat := func(emp, warn, valid, oor bool) {
		if thrN > 1 {
			statMtx.Lock()
		}
		if emp {
			empty++
		}
		if warn {
			warns++
		}
		if !valid {
			invalid++
		}
		if oor {
			filtered++
		}
		if thrN > 1 {
			statMtx.Unlock()
		}
	}
	processMsg := func(c chan error, msg []byte) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		nBytes := len(msg)
		if nBytes < len(GroupsioMBoxMsgSeparator) {
			stat(true, false, false, false)
			return
		}
		if !bytes.HasPrefix(msg, GroupsioMBoxMsgSeparator[1:]) {
			msg = append(GroupsioMBoxMsgSeparator[1:], msg...)
		}
		var (
			valid   bool
			warn    bool
			message map[string]interface{}
		)
		message, valid, warn = ParseMBoxMsg(ctx, j.GroupName, msg)
		stat(false, warn, valid, false)
		if !valid {
			return
		}
		updatedOn := j.ItemUpdatedOn(message)
		if ctx.DateFrom != nil && updatedOn.Before(from) {
			stat(false, false, false, true)
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
				var (
					e    error
					esch chan error
				)
				esch, e = processMsg(ch, msg)
				if e != nil {
					Printf("process message error: %v\n", e)
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
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
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
	if empty > 0 {
		Printf("%d empty messages\n", empty)
	}
	if warns > 0 {
		Printf("%d parse message warnings\n", warns)
	}
	if invalid > 0 {
		Printf("%d invalid messages\n", invalid)
	}
	if filtered > 0 {
		Printf("%d filtered messages (updated before %v)\n", invalid, from)
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
	// Because in groups.io one raw item generates no more than 1 rich item
	return UUID
}

// RichAuthorField - return rich author field name
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
func (j *DSGroupsio) ResumeNeedsOrigin(ctx *Ctx, raw bool) bool {
	return j.MultiOrigin
}

// ResumeNeedsCategory - is category field needed when resuming
// Category should be needed when multiple types of categories save to the same index
// or there are multiple types of documents within the same category
func (j *DSGroupsio) ResumeNeedsCategory(ctx *Ctx, raw bool) bool {
	return false
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
		Fatalf("%s: ItemID() - cannot extract %s from %+v", j.DS, GroupsioMessageIDField, DumpKeys(item))
	}
	return id
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGroupsio) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := Dig(item, []string{GroupsioMessageDateField}, true, false)
	updated, ok := iUpdated.(time.Time)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract %s from %+v", j.DS, GroupsioMessageDateField, DumpKeys(item))
	}
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

// GetItemIdentitiesEx return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
// This one (Ex) also returns information about identity's origins (from, to, or both)
func (j *DSGroupsio) GetItemIdentitiesEx(ctx *Ctx, doc interface{}) (identities map[[3]string]map[string]struct{}, nRecipients int) {
	init := false
	props := []string{"From", "To"}
	for _, prop := range props {
		lProp := strings.ToLower(prop)
		ifroms, ok := Dig(doc, []string{"data", prop}, false, true)
		if !ok {
			ifroms, ok = Dig(doc, []string{"data", lProp}, false, true)
			if !ok {
				if ctx.Debug > 1 || lProp == From {
					Printf("cannot get identities: cannot dig %s/%s in %v\n", prop, lProp, doc)
				}
				continue
			}
		}
		// Property can be an array
		froms, ok := ifroms.([]interface{})
		if !ok {
			// Or can be a string
			sfroms, ok := ifroms.(string)
			if !ok {
				Printf("cannot get identities: cannot read string or interface array from %v\n", ifroms)
				continue
			}
			froms = []interface{}{sfroms}
		}
		for _, ifrom := range froms {
			from, ok := ifrom.(string)
			if !ok {
				Printf("cannot get identities: cannot read string from %v\n", ifrom)
				continue
			}
			emails, ok := ParseAddresses(ctx, from, GroupsioMaxRecipients)
			if !ok {
				if ctx.Debug > 1 {
					Printf("cannot get identities: cannot read email address(es) from %s\n", from)
				}
				continue
			}
			for _, obj := range emails {
				if !init {
					identities = make(map[[3]string]map[string]struct{})
					init = true
				}
				identity := [3]string{obj.Name, Nil, obj.Address}
				_, ok := identities[identity]
				if !ok {
					identities[identity] = make(map[string]struct{})
				}
				identities[identity][lProp] = struct{}{}
			}
			if lProp == "to" {
				nRecipients = len(emails)
			}
		}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGroupsio) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	sIdentities, _ := j.GetItemIdentitiesEx(ctx, doc)
	if sIdentities == nil || len(sIdentities) == 0 {
		return
	}
	identities = make(map[[3]string]struct{})
	for k := range sIdentities {
		identities[k] = struct{}{}
	}
	return
}

// GroupsioEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func GroupsioEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("groupsio enrich items %d/%d func\n", len(items), len(*docs))
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
	groupsio, _ := ds.(*DSGroupsio)
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
		identities, nRecipients := groupsio.GetItemIdentitiesEx(ctx, doc)
		if identities == nil || len(identities) == 0 {
			if ctx.Debug > 1 {
				Printf("no identities to enrich in %v\n", doc)
			}
			return
		}
		counts := make(map[string]int)
		getAuthorPrefix := func(origin string) (author string) {
			origin = strings.ToLower(origin)
			cnt, _ := counts[origin]
			cnt++
			counts[origin] = cnt
			author = Author
			if origin != From {
				author = Recipient
			}
			if cnt > 1 {
				author += strconv.Itoa(cnt)
			}
			return
		}
		var rich map[string]interface{}
		authorFound := false
		for identity, origins := range identities {
			for origin := range origins {
				var richPart map[string]interface{}
				auth := getAuthorPrefix(origin)
				if rich == nil {
					rich, e = ds.EnrichItem(ctx, doc, auth, dbConfigured, identity)
				} else {
					richPart, e = ds.EnrichItem(ctx, doc, auth, dbConfigured, identity)
				}
				if e != nil {
					return
				}
				if auth == Author {
					authorFound = true
				}
				if richPart != nil {
					for k, v := range richPart {
						_, ok := rich[k]
						if !ok {
							rich[k] = v
						}
					}
				}
			}
		}
		if !authorFound {
			if ctx.Debug > 1 {
				Printf("no author found in\n%v\n%v\n", identities, item)
			} else if ctx.Debug > 0 {
				Printf("skipping email due to missing usable from email %v\n", identities)
			}
			return
		}
		rich["n_recipients"] = nRecipients
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
func (j *DSGroupsio) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GroupsioEnrichItemsFunc, nil, true)
	return
}

// EnrichItem - return rich item from raw item for a given author type/role
func (j *DSGroupsio) EnrichItem(ctx *Ctx, item map[string]interface{}, role string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	msg, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	msgDate, ok := Dig(msg, []string{GroupsioMessageDateField}, false, true)
	// original raw format support
	if !ok {
		msgDate, ok = Dig(msg, []string{"Date"}, false, true)
		if !ok {
			Fatalf("cannot find date/Date field in %+v\n", DumpKeys(msg))
			return
		}
	}
	var (
		msgTz       float64
		msgDateInTz time.Time
	)
	iMsgDateInTz, ok1 := Dig(msg, []string{"date_in_tz"}, false, true)
	if ok1 {
		msgDateInTz, ok1 = iMsgDateInTz.(time.Time)
	}
	iMsgTz, ok2 := Dig(msg, []string{"date_tz"}, false, true)
	if ok2 {
		msgTz, ok2 = iMsgTz.(float64)
	}
	if !ok1 || !ok2 {
		sdt := fmt.Sprintf("%v", msgDate)
		_, msgDateInTzN, msgTzN, ok := ParseDateWithTz(sdt)
		if ok {
			if !ok1 {
				msgDateInTz = msgDateInTzN
			}
			if !ok2 {
				msgTz = msgTzN
			}
		}
		if !ok && ctx.Debug > 0 {
			Printf("unable to determine tz for %v/%v/%v\n", msgDate, iMsgDateInTz, iMsgTz)
		}
	}
	// copy RawFields
	if role == Author {
		for _, field := range RawFields {
			v, _ := item[field]
			rich[field] = v
		}
		getStr := func(i interface{}) (o string, ok bool) {
			o, ok = i.(string)
			if ok {
				//Printf("getStr(%v) -> string:%s\n", i, o)
				return
			}
			var a []interface{}
			a, ok = i.([]interface{})
			if !ok {
				//Printf("getStr(%v) -> neither string nor []interface{}: %T\n", i, i)
				return
			}
			if len(a) == 0 {
				ok = false
				//Printf("getStr(%v) -> empty array\n", i)
				return
			}
			la := len(a)
			o, ok = a[la-1].(string)
			//Printf("getStr(%v) -> string[0]:%s\n", i, o)
			return
		}
		getStringValue := func(it map[string]interface{}, key string) (val string, ok bool) {
			var i interface{}
			i, ok = Dig(it, []string{key}, false, true)
			if ok {
				val, ok = getStr(i)
				if ok {
					//Printf("getStringValue(%v) -> string:%s\n", key, val)
					return
				}
				//Printf("getStringValue(%v) - was not able to get string from %v\n", key, i)
			}
			lKey := strings.ToLower(key)
			//Printf("getStringValue(%v) -> key not found, trying %s\n", key, lKey)
			for k := range it {
				if k == key {
					continue
				}
				lK := strings.ToLower(k)
				if lK == lKey {
					//Printf("getStringValue(%v) -> %s matches\n", key, k)
					i, ok = Dig(it, []string{k}, false, true)
					if ok {
						val, ok = getStr(i)
						if ok {
							//Printf("getStringValue(%v) -> %s string:%s\n", key, k, val)
							return
						}
						//Printf("getStringValue(%v) - %s was not able to get string from %v\n", key, k, i)
					}
				}
			}
			//Printf("getStringValue(%v) -> key not found\n", key)
			return
		}
		getIValue := func(it map[string]interface{}, key string) (i interface{}, ok bool) {
			i, ok = Dig(it, []string{key}, false, true)
			if ok {
				//Printf("getIValue(%v) -> %T:%v\n", key, i, i)
				return
			}
			lKey := strings.ToLower(key)
			//Printf("getIValue(%v) -> key not found, trying %s\n", key, lKey)
			for k := range it {
				if k == key {
					continue
				}
				lK := strings.ToLower(k)
				if lK == lKey {
					//Printf("getIValue(%v) -> %s matches\n", key, k)
					i, ok = Dig(it, []string{k}, false, true)
					if ok {
						//Printf("getIValue(%v) -> %s %T:%v\n", key, k, i, i)
						return
					}
				}
			}
			//Printf("getIValue(%v) -> key not found\n", key)
			return
		}
		rich["Message-ID"], ok = Dig(msg, []string{GroupsioMessageIDField}, false, true)
		// original raw format support
		if !ok {
			rich["Message-ID"], ok = Dig(msg, []string{"Message-ID"}, false, true)
			if !ok {
				Fatalf("cannot find message-id/Message-ID field in %v\n", DumpKeys(msg))
				return
			}
		}
		rich["Date"] = msgDate
		rich["Date_in_tz"] = msgDateInTz
		rich["tz"] = msgTz
		subj, _ := getStringValue(msg, "Subject")
		rich["Subject_analyzed"] = subj
		if len(subj) > GroupsioMaxMessageBodyLength {
			subj = subj[:GroupsioMaxMessageBodyLength]
		}
		rich["Subject"] = subj
		rich["email_date"], _ = getIValue(item, DefaultDateField)
		rich["list"], _ = getStringValue(item, "origin")
		lks := make(map[string]struct{})
		for k := range msg {
			lks[strings.ToLower(k)] = struct{}{}
		}
		_, ok = lks["in-reply-to"]
		rich["root"] = !ok
		var (
			plain interface{}
			text  string
			found bool
		)
		plain, ok = Dig(msg, []string{"data", "text", "plain"}, false, true)
		if ok {
			a, ok := plain.([]interface{})
			if ok {
				if len(a) > 0 {
					body, ok := a[0].(map[string]interface{})
					if ok {
						data, ok := body["data"]
						if ok {
							text, found = data.(string)
						}
					}
				}
			}
		} else {
			// original raw format support
			plain, ok = Dig(msg, []string{"body", "plain"}, false, true)
			if ok {
				text, found = plain.(string)
			}
		}
		if found {
			rich["size"] = len(text)
			ary := strings.Split(text, "\n")
			if len(ary) > GroupsioMaxRichMessageLines {
				ary = ary[:GroupsioMaxRichMessageLines]
			}
			text = strings.Join(ary, "\n")
			if len(text) > GroupsioMaxMessageBodyLength {
				text = text[:GroupsioMaxMessageBodyLength]
			}
			rich["body_extract"] = text
		} else {
			rich["size"] = nil
			rich["body_extract"] = ""
		}
		rich["mbox_parse_warning"], _ = Dig(msg, []string{"MBox-Warn"}, false, true)
		rich["mbox_bytes_length"], _ = Dig(msg, []string{"MBox-Bytes-Length"}, false, true)
		rich["mbox_n_lines"], _ = Dig(msg, []string{"MBox-N-Lines"}, false, true)
		rich["mbox_n_bodies"], _ = Dig(msg, []string{"MBox-N-Bodies"}, false, true)
		rich["mbox_from"], _ = Dig(msg, []string{"MBox-From"}, false, true)
		rich["mbox_date"] = nil
		rich["mbox_date_str"] = ""
		dtStr, ok := Dig(msg, []string{"MBox-Date"}, false, true)
		if ok {
			sdt, ok := dtStr.(string)
			if ok {
				rich["mbox_date_str"] = sdt
				dt, dttz, tz, valid := ParseDateWithTz(sdt)
				if valid {
					rich["mbox_date"] = dt
					rich["mbox_date_tz"] = tz
					rich["mbox_date_in_tz"] = dttz
				}
			}
		}
		for prop, value := range CommonFields(j, msgDate, Message) {
			rich[prop] = value
		}
	}
	if affs {
		affsData := make(map[string]interface{})
		var dt time.Time
		dt, err = TimeParseInterfaceString(msgDate)
		if err != nil {
			switch vdt := msgDate.(type) {
			case string:
				dt, _, _, ok = ParseDateWithTz(vdt)
				if !ok {
					err = fmt.Errorf("cannot parse date %s", vdt)
					return
				}
			case time.Time:
				dt = vdt
			default:
				err = fmt.Errorf("cannot parse date %T %v", vdt, vdt)
				return
			}
			err = nil
		}
		ary, _ := extra.([3]string)
		// (name, username, email)
		identity := map[string]interface{}{
			"name":     ary[0],
			"username": ary[1],
			"email":    ary[2],
		}
		affsIdentity, empty, e := IdentityAffsData(ctx, j, identity, nil, dt, role)
		if e != nil {
			Printf("AffsItems/IdentityAffsData: error: %v for %v,%v,%v\n", e, identity, dt, role)
			if ctx.AffsAPIFailFatal {
				err = e
				return
			}
		}
		if empty {
			Printf("no identity affiliation data for identity %+v\n", identity)
		} else {
			for prop, value := range affsIdentity {
				affsData[prop] = value
			}
			for _, suff := range RequiredAffsFields {
				k := role + suff
				_, ok := affsIdentity[k]
				if !ok {
					affsIdentity[k] = Unknown
				}
			}
			for prop, value := range affsData {
				rich[prop] = value
			}
			orgsKey := role + MultiOrgNames
			_, ok := Dig(rich, []string{orgsKey}, false, true)
			if !ok {
				rich[orgsKey] = []interface{}{}
			}
		}
	}
	if role == Author {
		rich["mbox_author_domain"], _ = Dig(rich, []string{"author_domain"}, false, true)
		CopyAffsRoleData(rich, rich, "From", Author)
	}
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGroupsio) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// Groups.io is not using this
	return
}

// GetRoleIdentity - return identity data for a given role
// groupsio si not using this
func (j *DSGroupsio) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for Groupsio backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGroupsio) AllRoles(ctx *Ctx, rich map[string]interface{}) (roles []string, static bool) {
	roles = []string{Author}
	if rich == nil {
		return
	}
	_, ok := Dig(rich, []string{"recipient_uuid"}, false, true)
	if !ok {
		return
	}
	roles = append(roles, Recipient)
	i := 2
	for {
		role := Recipient + strconv.Itoa(i)
		_, ok := Dig(rich, []string{role + "_uuid"}, false, true)
		if !ok {
			break
		}
		roles = append(roles, role)
		i++
		if i >= GroupsioMaxRecipients {
			break
		}
	}
	return
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSGroupsio) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}

// HasIdentities - does this data source support identity data
func (j *DSGroupsio) HasIdentities() bool {
	return true
}

// UseDefaultMapping - apply MappingNotAnalyzeString for raw/rich (raw=fals/true) index in this DS?
func (j *DSGroupsio) UseDefaultMapping(ctx *Ctx, raw bool) bool {
	return true
}
