package dads

import (
	"fmt"
	neturl "net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/emoji"
)

const (
	// RocketchatBackendVersion - backend version
	RocketchatBackendVersion = "0.0.1"
)

var (
	// RocketchatRawMapping - Rocketchat raw index mapping
	RocketchatRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"dynamic":false,"properties":{}}}}`)
	// RocketchatRichMapping - Rocketchat rich index mapping
	RocketchatRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"msg_analyzed":{"type":"text","fielddata":true,"index":true}}}`)
	// RocketchatCategories - categories defined for Rocketchat
	RocketchatCategories = map[string]struct{}{Message: {}}
	// RocketchatDefaultMaxItems - max items to retrieve from API via a single request
	RocketchatDefaultMaxItems = 100
	// RocketchatDefaultMinRate - default min rate points (when not set)
	RocketchatDefaultMinRate = 10
	// RocketchatDefaultSearchField - default search field
	RocketchatDefaultSearchField = "item_id"
	// RocketchatRoles - roles to fetch affiliation data for rocketchat messages
	RocketchatRoles = []string{"u"}
	// MustWaitRE - parse too many requests error message
	MustWaitRE = regexp.MustCompile(`must wait (\d+) seconds before`)
)

// DSRocketchat - DS implementation for rocketchat - does nothing at all, just presents a skeleton code
type DSRocketchat struct {
	DS           string
	URL          string // From DA_ROCKETCHAT_URL - rocketchat server url
	Channel      string // From DA_ROCKETCHAT_CHANNEL - rocketchat channel
	User         string // From DA_ROCKETCHAT_USER - user name
	Token        string // From DA_ROCKETCHAT_TOKEN - token
	MaxItems     int    // From DA_ROCKETCHAT_MAX_ITEMS, defaults to RocketchatDefaultMaxItems (100)
	MinRate      int    // From DA_ROCKETCHAT_MIN_RATE - min API points, if we reach this value we wait for refresh, default RocketchatDefaultMinRate(10)
	WaitRate     bool   // From DA_ROCKETCHAT_WAIT_RATE - will wait for rate limit refresh if set, otherwise will fail is rate limit is reached
	NoSSLVerify  bool   // From DA_ROCKETCHAT_NO_SSL_VERIFY
	SingleOrigin bool   // From DA_ROCKETCHAT_SINGLE_ORIGIN - if you want to store only one rocketchat endpoint in the index
}

// ParseArgs - parse rocketchat specific environment variables
func (j *DSRocketchat) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Rocketchat
	prefix := "DA_ROCKETCHAT_"
	j.URL = os.Getenv(prefix + "URL")
	j.Channel = os.Getenv(prefix + "CHANNEL")
	j.User = os.Getenv(prefix + "USER")
	j.Token = os.Getenv(prefix + "TOKEN")
	AddRedacted(j.User, false)
	AddRedacted(j.Token, false)
	if ctx.Env("MAX_ITEMS") != "" {
		maxItems, err := strconv.Atoi(ctx.Env("MAX_ITEMS"))
		FatalOnError(err)
		if maxItems > 0 {
			j.MaxItems = maxItems
		}
	} else {
		j.MaxItems = RocketchatDefaultMaxItems
	}
	if ctx.Env("MIN_RATE") != "" {
		minRate, err := strconv.Atoi(ctx.Env("MIN_RATE"))
		FatalOnError(err)
		if minRate > 0 {
			j.MinRate = minRate
		}
	} else {
		j.MinRate = RocketchatDefaultMinRate
	}
	j.WaitRate = StringToBool(os.Getenv(prefix + "WAIT_RATE"))
	j.NoSSLVerify = StringToBool(os.Getenv(prefix + "NO_SSL_VERIFY"))
	if j.NoSSLVerify {
		NoSSLVerify()
	}
	j.SingleOrigin = StringToBool(os.Getenv(prefix + "SINGLE_ORIGIN"))
	return
}

// Validate - is current DS configuration OK?
func (j *DSRocketchat) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	j.Channel = strings.TrimSpace(j.Channel)
	if j.URL == "" || j.Channel == "" || j.User == "" || j.Token == "" {
		err = fmt.Errorf("URL, Channel, User, Token must all be set")
	}
	return
}

// Name - return data source name
func (j *DSRocketchat) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSRocketchat) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSRocketchat) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for rocketchat datasource
func (j *DSRocketchat) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSRocketchat) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for rocketchat datasource
func (j *DSRocketchat) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSRocketchat) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = (int(int64(rateLimitReset)-(time.Now().UnixNano()/int64(1000000))) / 1000) + 1
	if seconds < 0 {
		seconds = 0
	}
	if ctx.Debug > 1 {
		Printf("CalculateTimeToReset(%d,%d) -> %d\n", rateLimit, rateLimitReset, seconds)
	}
	return
}

// GetRocketchatMessages - get confluence historical contents
func (j *DSRocketchat) GetRocketchatMessages(ctx *Ctx, fromDate string, offset, rateLimit, rateLimitReset int) (messages []map[string]interface{}, newOffset, total, outRateLimit, outRateLimitReset int, err error) {
	query := `{"_updatedAt": {"$gte": {"$date": "` + fromDate + `"}}}`
	url := j.URL + fmt.Sprintf(
		`/api/v1/channels.messages?roomName=%s&count=%d&offset=%d&sort=%s&query=%s`,
		neturl.QueryEscape(j.Channel),
		j.MaxItems,
		offset,
		neturl.QueryEscape(`{"_updatedAt": 1}`),
		neturl.QueryEscape(query),
	)
	// Let's cache messages for 1 hour (so there are no rate limit hits during the development)
	cacheDur := time.Duration(1) * time.Hour
	method := Get
	headers := map[string]string{"X-User-ID": j.User, "X-Auth-Token": j.Token}
	//Printf("%s %+v\n", method, headers)
	//Printf("URL: %s\n", url)
	var (
		res        interface{}
		status     int
		outHeaders map[string][]string
	)
	for {
		err = SleepForRateLimit(ctx, j, rateLimit, rateLimitReset, j.MinRate, j.WaitRate)
		if err != nil {
			return
		}
		res, status, _, outHeaders, err = Request(
			ctx,
			url,
			method,
			headers,
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}, {429, 429}: {}}, // JSON statuses: 200, 429
			nil, // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {429, 429}: {}}, // OK statuses: 200, 429
			true,      // retry
			&cacheDur, // cache duration
			false,     // skip in dry-run mode
		)
		rateLimit, rateLimitReset, _ = UpdateRateLimit(ctx, j, outHeaders, "", "")
		if status == 413 {
			continue
		}
		// Too many requests
		if status == 429 {
			j.SleepAsRequested(res)
			continue
		}
		if err != nil {
			return
		}
		break
	}
	data, _ := res.(map[string]interface{})
	fTotal, _ := data["total"].(float64)
	total = int(fTotal)
	iMessages, _ := data["messages"].([]interface{})
	for _, iMessage := range iMessages {
		messages = append(messages, iMessage.(map[string]interface{}))
	}
	// Printf("MESSAGES: %d, TOTAL: %d, OFFSET: %d\n", len(messages), total, offset)
	outRateLimit, outRateLimitReset, newOffset = rateLimit, rateLimitReset, offset+len(messages)
	return
}

// SleepAsRequested - parse server's:
// {"success":false,"error":"Error, too many requests. Please slow down. You must wait 23 seconds before trying this endpoint again. [error-too-many-requests]"}
// And sleep N+1 requested seconds
func (j *DSRocketchat) SleepAsRequested(res interface{}) {
	iErrorMsg, ok := res.(map[string]interface{})["error"]
	if !ok {
		Printf("Unable to parse sleep duration, assuming 30s\n")
		time.Sleep(time.Duration(30) * time.Second)
		return
	}
	errorMsg, _ := iErrorMsg.(string)
	match := MustWaitRE.FindAllStringSubmatch(errorMsg, -1)
	if len(match) < 1 {
		Printf("Unable to parse sleep duration from '%s', assuming 30s\n", errorMsg)
		time.Sleep(time.Duration(30) * time.Second)
		return
	}
	sleepFor, _ := strconv.Atoi(match[0][1])
	Printf("Sleeping for %d seconds, as requested in '%s'\n", errorMsg)
	sleepFor++
	time.Sleep(time.Duration(sleepFor) * time.Second)
}

// FetchItems - implement enrich data for rocketchat datasource
func (j *DSRocketchat) FetchItems(ctx *Ctx) (err error) {
	var (
		dateFrom  time.Time
		sDateFrom string
	)
	if ctx.DateFrom != nil {
		dateFrom = *ctx.DateFrom
	} else {
		dateFrom = DefaultDateFrom
	}
	sDateFrom = ToESDate(dateFrom)
	rateLimit, rateLimitReset := -1, -1
	cacheDur := time.Duration(48) * time.Hour
	url := j.URL + "/api/v1/channels.info?roomName=" + neturl.QueryEscape(j.Channel)
	method := Get
	headers := map[string]string{"X-User-ID": j.User, "X-Auth-Token": j.Token}
	var (
		res        interface{}
		status     int
		outHeaders map[string][]string
	)
	for {
		err = SleepForRateLimit(ctx, j, rateLimit, rateLimitReset, j.MinRate, j.WaitRate)
		if err != nil {
			return
		}
		// curl -s -H 'X-Auth-Token: token' -H 'X-User-ID: user' URL/api/v1/channels.info?roomName=channel | jq '.'
		// 48 hours for caching channel info
		res, status, _, outHeaders, err = Request(
			ctx,
			url,
			method,
			headers,
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}, {429, 429}: {}}, // JSON statuses: 200, 429
			nil, // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {429, 429}: {}}, // OK statuses: 200, 429
			true,      // retry
			&cacheDur, // cache duration
			false,     // skip in dry-run mode
		)
		rateLimit, rateLimitReset, _ = UpdateRateLimit(ctx, j, outHeaders, "", "")
		// Rate limit
		if status == 413 {
			continue
		}
		// Too many requests
		if status == 429 {
			j.SleepAsRequested(res)
			continue
		}
		if err != nil {
			return
		}
		break
	}
	channelInfo, ok := res.(map[string]interface{})["channel"]
	if !ok {
		data, _ := res.(map[string]interface{})
		err = fmt.Errorf("Cannot read channel info from:\n%s", data)
		return
	}
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
	processMsg := func(c chan error, item map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		esItem := j.AddMetadata(ctx, item)
		if ctx.Project != "" {
			item["project"] = ctx.Project
		}
		esItem["data"] = item
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
	offset, total := 0, 0
	if thrN > 1 {
		for {
			var messages []map[string]interface{}
			messages, offset, total, rateLimit, rateLimitReset, err = j.GetRocketchatMessages(ctx, sDateFrom, offset, rateLimit, rateLimitReset)
			if err != nil {
				return
			}
			for _, message := range messages {
				message["channel_info"] = channelInfo
				go func(message map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processMsg(ch, message)
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
			if offset >= total {
				break
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
		for {
			var messages []map[string]interface{}
			messages, offset, total, rateLimit, rateLimitReset, err = j.GetRocketchatMessages(ctx, sDateFrom, offset, rateLimit, rateLimitReset)
			if err != nil {
				return
			}
			for _, message := range messages {
				message["channel_info"] = channelInfo
				_, err = processMsg(nil, message)
				if err != nil {
					return
				}
			}
			if offset >= total {
				break
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
func (j *DSRocketchat) SupportDateFrom() bool {
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSRocketchat) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSRocketchat) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSRocketchat) RichIDField(*Ctx) string {
	return UUID
}

// RichAuthorField - return rich author field name
func (j *DSRocketchat) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSRocketchat) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSRocketchat) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSRocketchat) Categories() map[string]struct{} {
	return RocketchatCategories
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSRocketchat) ResumeNeedsOrigin(ctx *Ctx) bool {
	return !j.SingleOrigin
}

// Origin - return current origin
func (j *DSRocketchat) Origin(ctx *Ctx) string {
	return j.URL + "/" + j.Channel
}

// ItemID - return unique identifier for an item
func (j *DSRocketchat) ItemID(item interface{}) string {
	id, _ := Dig(item, []string{"_id"}, true, false)
	return id.(string)
}

// AddMetadata - add metadata to the item
func (j *DSRocketchat) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	// Change to unique datasource origin
	origin := j.Origin(ctx)
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = RocketchatBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	channelID, _ := Dig(item, []string{"channel_info", "_id"}, true, false)
	channelName, _ := Dig(item, []string{"channel_info", "name"}, true, false)
	FatalOnError(DeepSet(mItem, []string{"search_fields", RocketchatDefaultSearchField}, itemID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "channel_id"}, channelID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "channel_name"}, channelName, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSRocketchat) ItemUpdatedOn(item interface{}) time.Time {
	iUpdated, _ := Dig(item, []string{"_updatedAt"}, true, false)
	updated, err := TimeParseAny(iUpdated.(string))
	FatalOnError(err)
	return updated
}

// ItemCategory - return unique identifier for an item
func (j *DSRocketchat) ItemCategory(item interface{}) string {
	return Message
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSRocketchat) ElasticRawMapping() []byte {
	return RocketchatRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSRocketchat) ElasticRichMapping() []byte {
	return RocketchatRichMapping
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "none" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSRocketchat) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("GetItemIdentities: %+v -> %+v\n", DumpPreview(doc, 100), identities)
		}()
	}
	iUser, ok := Dig(doc, []string{"data", "u"}, false, true)
	if !ok {
		return
	}
	user, _ := iUser.(map[string]interface{})
	username := Nil
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	}
	name := Nil
	iName, ok := user["name"]
	if ok {
		name, _ = iName.(string)
	}
	if name == Nil && username == Nil {
		return
	}
	identities = map[[3]string]struct{}{{name, username, Nil}: {}}
	return
}

// RocketchatEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func RocketchatEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("rocketchat enrich items %d/%d func\n", len(items), len(*docs))
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
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		// Actual item enrichment
		var rich map[string]interface{}
		rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
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
func (j *DSRocketchat) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, RocketchatEnrichItemsFunc, nil, true)
	return
}

// EnrichItem - return rich item from raw item for a given author type
func (j *DSRocketchat) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	message, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpKeys(item))
		return
	}
	msg, _ := message["msg"]
	rich["msg_analyzed"] = msg
	rich["msg"] = msg
	rich["rid"], _ = message["rid"]
	rich["msg_id"], _ = message["_id"]
	rich["msg_parent"], _ = message["parent"]
	iAuthor, ok := message["u"]
	if ok {
		author, _ := iAuthor.(map[string]interface{})
		rich["user_id"], _ = author["_id"]
		rich["user_name"], _ = author["name"]
		rich["user_username"], _ = author["username"]
	}
	rich["is_edited"] = 0
	iEditor, ok := message["editedBy"]
	if ok {
		editor, _ := iEditor.(map[string]interface{})
		iEdited, ok := editor["editedAt"]
		if ok {
			edited, err := TimeParseAny(iEdited.(string))
			if err == nil {
				rich["edited_at"] = edited
			}
		}
		rich["edited_by_username"], _ = editor["username"]
		rich["edited_by_user_id"], _ = editor["_id"]
		rich["is_edited"] = 1
	}
	iFile, ok := message["file"]
	if ok {
		file, _ := iFile.(map[string]interface{})
		rich["file_id"], _ = file["_id"]
		rich["file_name"], _ = file["name"]
		rich["file_type"], _ = file["type"]
	}
	iReplies, ok := message["replies"]
	if ok {
		replies, ok := iReplies.([]interface{})
		if ok {
			rich["replies"] = len(replies)
		} else {
			rich["replies"] = 0
		}
	} else {
		rich["replies"] = 0
	}
	rich["total_reactions"] = 0
	iReactions, ok := message["reactions"]
	if ok {
		reactions, _ := iReactions.(map[string]interface{})
		rich["reactions"], rich["total_reactions"] = j.GetReactions(reactions)
	}
	rich["total_mentions"] = 0
	iMentions, ok := message["mentions"]
	if ok {
		mentions, _ := iMentions.([]interface{})
		mentionsAry := j.GetMentions(mentions)
		rich["mentions"] = mentionsAry
		rich["total_mentions"] = len(mentionsAry)
	}
	iChannelInfo, ok := message["channel_info"]
	if ok {
		channelInfo, _ := iChannelInfo.(map[string]interface{})
		j.SetChannelInfo(rich, channelInfo)
	}
	rich["total_urls"] = 0
	iURLs, ok := message["urls"]
	if ok {
		urls, _ := iURLs.([]interface{})
		urlsAry := []interface{}{}
		for _, iURL := range urls {
			url, _ := iURL.(map[string]interface{})
			urlsAry = append(urlsAry, url["url"])
		}
		rich["message_urls"] = urlsAry
		rich["total_urls"] = len(urlsAry)
	}
	updatedOn, _ := Dig(item, []string{j.DateField(ctx)}, true, false)
	if affs {
		authorKey := "u"
		var affsItems map[string]interface{}
		affsItems, err = j.AffsItems(ctx, item, RocketchatRoles, updatedOn)
		if err != nil {
			return
		}
		for prop, value := range affsItems {
			rich[prop] = value
		}
		for _, suff := range AffsFields {
			rich[Author+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + MultiOrgNames
		_, ok := Dig(rich, []string{orgsKey}, false, true)
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
	}
	for prop, value := range CommonFields(j, updatedOn, Message) {
		rich[prop] = value
	}
	return
}

// SetChannelInfo - set rich channel info from raw channel info
func (j *DSRocketchat) SetChannelInfo(rich, channel map[string]interface{}) {
	rich["channel_id"], _ = channel["_id"]
	iUpdated, ok := channel["_updatedAt"]
	if ok {
		updated, err := TimeParseAny(iUpdated.(string))
		if err == nil {
			rich["channel_updated_at"] = updated
		}
	}
	rich["channel_num_messages"], _ = channel["msgs"]
	rich["channel_name"], _ = channel["name"]
	rich["channel_num_users"], _ = channel["usersCount"]
	rich["channel_topic"], _ = channel["topic"]
	rich["avatar"], _ = Dig(channel, []string{"lastMessage", "avatar"}, false, true)
}

// GetMentions - convert raw mentions to rich mentions
func (j *DSRocketchat) GetMentions(mentions []interface{}) (richMentions []map[string]interface{}) {
	for _, iUsr := range mentions {
		usr, _ := iUsr.(map[string]interface{})
		userName, _ := usr["username"]
		id, _ := usr["_id"]
		name, _ := usr["name"]
		richMentions = append(richMentions, map[string]interface{}{
			"username": userName,
			"id":       id,
			"name":     name,
		})
	}
	return
}

// GetReactions - convert raw reactions to rich reactions
func (j *DSRocketchat) GetReactions(reactions map[string]interface{}) (richReactions []map[string]interface{}, nReactions int) {
	for reactionType, iReactionData := range reactions {
		reactionData, _ := iReactionData.(map[string]interface{})
		userNames := []interface{}{}
		names := []interface{}{}
		iUserNames, ok := reactionData["usernames"]
		if ok {
			userNames, _ = iUserNames.([]interface{})
		}
		iNames, ok := reactionData["names"]
		if ok {
			names, _ = iNames.([]interface{})
		}
		data := emoji.GetEmojiUnicode(reactionType)
		nUserNames := len(userNames)
		richReactions = append(richReactions, map[string]interface{}{
			"type":     reactionType,
			"emoji":    data,
			"username": userNames,
			"names":    names,
			"count":    nUserNames,
		})
		nReactions += nUserNames
	}
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSRocketchat) AffsItems(ctx *Ctx, message map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	affsItems = make(map[string]interface{})
	var dt time.Time
	dt, err = TimeParseInterfaceString(date)
	if err != nil {
		return
	}
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, message, role)
		if len(identity) == 0 {
			continue
		}
		affsIdentity, empty := IdenityAffsData(ctx, j, identity, nil, dt, role)
		if empty {
			Printf("no identity affiliation data for identity %+v\n", identity)
			continue
		}
		for prop, value := range affsIdentity {
			affsItems[prop] = value
		}
		for _, suff := range RequiredAffsFields {
			k := role + suff
			_, ok := affsIdentity[k]
			if !ok {
				affsIdentity[k] = Unknown
			}
		}
	}
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSRocketchat) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	iUser, ok := Dig(item, []string{"data", "u"}, true, false)
	user, _ := iUser.(map[string]interface{})
	username := Nil
	iUserName, ok := user["username"]
	if ok {
		username, _ = iUserName.(string)
	}
	name := Nil
	iName, ok := user["name"]
	if ok {
		name, _ = iName.(string)
	}
	identity = map[string]interface{}{"name": name, "username": username, "email": Nil}
	return
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSRocketchat) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	return []string{"u"}, true
}
