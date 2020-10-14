package dads

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/mail"
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
	GroupsioMessageIDField = "message-id"
	// GroupsioMessageDateField - message ID field from email
	GroupsioMessageDateField = "date"
	// GroupsioMessageReceivedField - message Received filed
	GroupsioMessageReceivedField = "received"
	// GroupsioDefaultSearchField - default search field
	GroupsioDefaultSearchField = "item_id"
	// MaxMessageBodyLength - trucacte message bodies longer than this (per each multi-body email part)
	MaxMessageBodyLength = 1000
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
	// GroupsioMsgLineSeparator - used to split mbox message into its separate lines
	GroupsioMsgLineSeparator = []byte("\r\n")
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
		dirPath = j.ArchPath + "/" + GroupsioURLRoot + j.GroupName
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
	Printf("groupsio login via: %s\n", url)
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
		url += `&start_time=` + neturl.QueryEscape(ToYMDTHMSZDate(from))
	}
	Printf("fetching messages from: %s\n", url)
	// FIXME: remove caching or lower to 3 hours at most
	cacheMsgDur := time.Duration(48) * time.Hour
	res, status, _, err = Request(
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
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
// This one (Ex) also returns information about identity's origin (from or to)
func (j *DSGroupsio) GetItemIdentitiesEx(ctx *Ctx, doc interface{}) (identities map[[3]string]string) {
	init := false
	props := []string{"From", "To"}
	for _, prop := range props {
		lProp := strings.ToLower(prop)
		ifroms, ok := Dig(doc, []string{"data", prop}, false, true)
		if !ok {
			ifroms, ok = Dig(doc, []string{"data", lProp}, false, true)
			if !ok {
				if ctx.Debug > 1 {
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
		patterns := []string{" at ", "_at_", " en "}
		for _, ifrom := range froms {
			from, ok := ifrom.(string)
			if !ok {
				Printf("cannot get identities: cannot read string from %v\n", ifrom)
				continue
			}
			from = strings.TrimSpace(from)
			for _, pattern := range patterns {
				from = strings.Replace(from, pattern, "@", -1)
			}
			emails, e := mail.ParseAddressList(from)
			if e != nil {
				nFrom := strings.Replace(from, `"`, "", -1)
				emails, e = mail.ParseAddressList(nFrom)
				if e != nil {
					emails = []*mail.Address{}
					ary := strings.Split(nFrom, ",")
					for _, f := range ary {
						f = strings.TrimSpace(f)
						email, e := mail.ParseAddress(f)
						if e == nil {
							emails = append(emails, email)
							if ctx.Debug > 1 {
								Printf("unable to parse '%s' but '%s' parsed to %v ('%s','%s')\n", nFrom, f, email, email.Name, email.Address)
							}
						}
					}
					if len(emails) == 0 {
						if ctx.Debug > 1 {
							Printf("cannot get identities: cannot read email address(es) from %s\n", from)
						}
						continue
					}
				}
			}
			for _, obj := range emails {
				// remove leading/trailing ' "
				// skip if starts with =?
				// should we allow empty name?
				obj.Name = strings.Trim(obj.Name, `"'`)
				obj.Address = strings.Trim(obj.Address, `"'`)
				if strings.HasPrefix(obj.Name, "=?") {
					continue
				}
				if !init {
					identities = make(map[[3]string]string)
					init = true
				}
				identities[[3]string{obj.Name, Nil, obj.Address}] = lProp
			}
		}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGroupsio) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	sIdentities := j.GetItemIdentitiesEx(ctx, doc)
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
			e = fmt.Errorf("Failed to parse document %+v\n", doc)
			return
		}
		identities := groupsio.GetItemIdentitiesEx(ctx, doc)
		if identities == nil || len(identities) == 0 {
			Printf("no identities to enrich in %v\n", doc)
			return
		}
		counts := make(map[string]int)
		getAuthorPrefix := func(origin string) (author string) {
			origin = strings.ToLower(origin)
			cnt, _ := counts[origin]
			cnt++
			counts[origin] = cnt
			author = Author
			if origin != "from" {
				author = "recipient"
			}
			if cnt > 1 {
				author += strconv.Itoa(cnt)
			}
			return
		}
		var rich map[string]interface{}
		for identity, origin := range identities {
			auth := getAuthorPrefix(origin)
			rich, e = ds.EnrichItem(ctx, doc, auth, dbConfigured, identity)
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
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GroupsioEnrichItemsFunc, nil)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSGroupsio) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
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
	return []string{Author}
}
