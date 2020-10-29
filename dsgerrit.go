package dads

import (
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const (
	// GerritBackendVersion - backend version
	GerritBackendVersion = "0.0.0"
	// GerritDefaultSSHKeyPath - default path to look for gerrit ssh private key
	GerritDefaultSSHKeyPath = "$HOME/.ssh/id_rsa"
	// GerritDefaultSSHPort - default gerrit ssh port
	GerritDefaultSSHPort = 29418
	// GerritDefaultMaxReviews = default max reviews when processing gerrit
	GerritDefaultMaxReviews = 500
)

var (
	// GerritRawMapping - Gerrit raw index mapping
	GerritRawMapping = []byte(`{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"commitMessage":{"type":"text","index":true},"comments":{"properties":{"message":{"type":"text","index":true}}},"subject":{"type":"text","index":true},"patchSets":{"properties":{"approvals":{"properties":{"description":{"type":"text","index":true}}},"comments":{"properties":{"message":{"type":"text","index":true}}}}}}}}}`)
	// GerritRichMapping - Gerrit rich index mapping
	GerritRichMapping = []byte(`{"properties":{"metadata__updated_on":{"type":"date"},"approval_description_analyzed":{"type":"text","index":true},"comment_message_analyzed":{"type":"text","index":true},"status":{"type":"keyword"},"summary_analyzed":{"type":"text","index":true},"timeopen":{"type":"double"}}}`)
	// GerritCategories - categories defined for gerrit
	GerritCategories = map[string]struct{}{Review: {}}
	// GerritVersionRegexp - gerrit verion pattern
	GerritVersionRegexp = regexp.MustCompile(`gerrit version (\d+)\.(\d+).*`)
	// GerritDefaultSearchField - default search field
	GerritDefaultSearchField = "item_id"
)

// DSGerrit - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGerrit struct {
	DS                  string
	URL                 string // From DA_GERRIT_URL - gerrit repo path
	SingleOrigin        bool   // From DA_GERRIT_SINGLE_ORIGIN - if you want to store only one gerrit endpoint in the index
	User                string // From DA_GERRIT_USER - gerrit user name
	SSHKey              string // From DA_GERRIT_SSH_KEY - must contain full SSH private key - has higher priority than key path
	SSHKeyPath          string // From DA_GERRIT_SSH_KEY_PATH - path to SSH private key, default GerritDefaultSSHKeyPath '~/.ssh/id_rsa'
	SSHPort             int    // From DA_GERRIT_SSH_PORT, defaults to GerritDefaultSSHPort (29418)
	MaxReviews          int    // From DA_GERRIT_MAX_REVIEWS, defaults to GerritDefaultMaxReviews (500)
	NoSSLVerify         bool   // From DA_GERRIT_NO_SSL_VERIFY
	DisableHostKeyCheck bool   // From DA_GERRIT_DISABLE_HOST_KEY_CHECK
	// Non-config variables
	SSHOpts        string   // SSH Options
	SSHKeyTempPath string   // if used SSHKey - temp file with this name was used to store key contents
	GerritCmd      []string // gerrit remote command used to fetch data
	VersionMajor   int      // gerrit major version
	VersionMinor   int      // gerrit minor version
}

// ParseArgs - parse stub specific environment variables
func (j *DSGerrit) ParseArgs(ctx *Ctx) (err error) {
	j.DS = Gerrit
	prefix := "DA_GERRIT_"
	j.URL = os.Getenv(prefix + "URL")
	j.User = os.Getenv(prefix + "USER")
	j.SingleOrigin = StringToBool(os.Getenv(prefix + "SINGLE_ORIGIN"))
	if os.Getenv(prefix+"SSH_KEY_PATH") != "" {
		j.SSHKeyPath = os.Getenv(prefix + "SSH_KEY_PATH")
	} else {
		j.SSHKeyPath = GerritDefaultSSHKeyPath
	}
	j.SSHKey = os.Getenv(prefix + "SSH_KEY")
	j.NoSSLVerify = StringToBool(os.Getenv(prefix + "NO_SSL_VERIFY"))
	if j.NoSSLVerify {
		NoSSLVerify()
	}
	j.DisableHostKeyCheck = StringToBool(os.Getenv(prefix + "DISABLE_HOST_KEY_CHECK"))
	if ctx.Env("SSH_PORT") != "" {
		sshPort, err := strconv.Atoi(ctx.Env("SSH_PORT"))
		FatalOnError(err)
		if sshPort > 0 {
			j.SSHPort = sshPort
		}
	} else {
		j.SSHPort = GerritDefaultSSHPort
	}
	if ctx.Env("MAX_REVIEWS") != "" {
		maxReviews, err := strconv.Atoi(ctx.Env("MAX_REVIEWS"))
		FatalOnError(err)
		if maxReviews > 0 {
			j.MaxReviews = maxReviews
		}
	} else {
		j.MaxReviews = GerritDefaultMaxReviews
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSGerrit) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	ary := strings.Split(j.URL, "://")
	if len(ary) > 1 {
		j.URL = ary[1]
	}
	j.SSHKeyPath = os.ExpandEnv(j.SSHKeyPath)
	if j.SSHKeyPath == "" && j.SSHKey == "" {
		err = fmt.Errorf("Either SSH key or SSH key path must be set")
		return
	}
	if j.User == "" {
		err = fmt.Errorf("User must be set")
	}
	return
}

// Name - return data source name
func (j *DSGerrit) Name() string {
	return j.DS
}

// Info - return DS configuration in a human readable form
func (j DSGerrit) Info() string {
	return fmt.Sprintf("%+v", j)
}

// CustomFetchRaw - is this datasource using custom fetch raw implementation?
func (j *DSGerrit) CustomFetchRaw() bool {
	return false
}

// FetchRaw - implement fetch raw data for stub datasource
func (j *DSGerrit) FetchRaw(ctx *Ctx) (err error) {
	Printf("%s should use generic FetchRaw()\n", j.DS)
	return
}

// CustomEnrich - is this datasource using custom enrich implementation?
func (j *DSGerrit) CustomEnrich() bool {
	return false
}

// Enrich - implement enrich data for stub datasource
func (j *DSGerrit) Enrich(ctx *Ctx) (err error) {
	Printf("%s should use generic Enrich()\n", j.DS)
	return
}

// InitGerrit - initializes gerrit client
func (j *DSGerrit) InitGerrit(ctx *Ctx) (err error) {
	if j.DisableHostKeyCheck {
		j.SSHOpts += "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
	}
	if j.SSHKey != "" {
		var f *os.File
		f, err = ioutil.TempFile("", "id_rsa")
		if err != nil {
			return
		}
		j.SSHKeyTempPath = f.Name()
		_, err = f.Write([]byte(j.SSHKey))
		if err != nil {
			return
		}
		err = f.Close()
		if err != nil {
			return
		}
		err = os.Chmod(j.SSHKeyTempPath, 0600)
		if err != nil {
			return
		}
		j.SSHOpts += "-i " + j.SSHKeyTempPath + " "
	} else {
		if j.SSHKeyPath != "" {
			j.SSHOpts += "-i " + j.SSHKeyPath + " "
		}
	}
	if strings.HasSuffix(j.SSHOpts, " ") {
		j.SSHOpts = j.SSHOpts[:len(j.SSHOpts)-1]
	}
	gerritCmd := fmt.Sprintf("ssh %s -p %d %s@%s gerrit", j.SSHOpts, j.SSHPort, j.User, j.URL)
	ary := strings.Split(gerritCmd, " ")
	for _, item := range ary {
		if item == "" {
			continue
		}
		j.GerritCmd = append(j.GerritCmd, item)
	}
	return
}

// GetGerritVersion - get gerrit version
func (j *DSGerrit) GetGerritVersion(ctx *Ctx) (err error) {
	cmdLine := j.GerritCmd
	cmdLine = append(cmdLine, "version")
	var (
		sout string
		serr string
	)
	sout, serr, err = ExecCommand(ctx, cmdLine, "", nil)
	if err != nil {
		Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
		return
	}
	match := GerritVersionRegexp.FindAllStringSubmatch(sout, -1)
	if len(match) < 1 {
		err = fmt.Errorf("cannot parse gerrit version '%s'", sout)
		return
	}
	j.VersionMajor, _ = strconv.Atoi(match[0][1])
	j.VersionMinor, _ = strconv.Atoi(match[0][2])
	if ctx.Debug > 0 {
		Printf("Detected gerrit %d.%d\n", j.VersionMajor, j.VersionMinor)
	}
	return
}

// GetGerritReviews - get gerrit reviews
func (j *DSGerrit) GetGerritReviews(ctx *Ctx, after string, afterEpoch float64, startFrom int) (reviews []map[string]interface{}, newStartFrom int, err error) {
	cmdLine := j.GerritCmd
	// https://gerrit-review.googlesource.com/Documentation/user-search.html:
	// ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ./ssh-key.secret -p XYZ usr@gerrit-url gerrit query after:'1970-01-01 00:00:00' limit: 2 (status:open OR status:closed) --all-approvals --all-reviewers --comments --format=JSON
	// For unknown reasons , gerrit is not returning data if number of seconds is not equal to 00 - so I'm updating query string to set seconds to ":00"
	after = after[:len(after)-3] + ":00"
	cmdLine = append(cmdLine, "query")
	if ctx.Project != "" {
		cmdLine = append(cmdLine, "project:", ctx.Project)
	}
	cmdLine = append(cmdLine, `after:"`+after+`"`, "limit:", strconv.Itoa(j.MaxReviews), "(status:open OR status:closed)", "--all-approvals", "--all-reviewers", "--comments", "--format=JSON")
	// 2006-01-02[ 15:04:05[.890][ -0700]]
	if startFrom > 0 {
		cmdLine = append(cmdLine, "--start="+strconv.Itoa(startFrom))
	}
	var (
		sout string
		serr string
	)
	if ctx.Debug > 0 {
		Printf("getting reviews via: %v\n", cmdLine)
	}
	sout, serr, err = ExecCommand(ctx, cmdLine, "", nil)
	if err != nil {
		Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
		return
	}
	data := strings.Replace("["+strings.Replace(sout, "\n", ",", -1)+"]", ",]", "]", -1)
	var items []interface{}
	err = jsoniter.Unmarshal([]byte(data), &items)
	if err != nil {
		return
	}
	for i, iItem := range items {
		item, _ := iItem.(map[string]interface{})
		//Printf("#%d) %v\n", i, DumpKeys(item))
		iMoreChanges, ok := item["moreChanges"]
		if ok {
			moreChanges, ok := iMoreChanges.(bool)
			if ok {
				if moreChanges {
					newStartFrom = startFrom + i
					if ctx.Debug > 0 {
						Printf("#%d) moreChanges: %v, newStartFrom: %d\n", i, moreChanges, newStartFrom)
					}
				}
			} else {
				Printf("cannot read boolean value from %v\n", iMoreChanges)
			}
			return
		}
		_, ok = item["project"]
		if !ok {
			if ctx.Debug > 0 {
				Printf("#%d) project not found: %+v", i, item)
			}
			continue
		}
		iLastUpdated, ok := item["lastUpdated"]
		if ok {
			lastUpdated, ok := iLastUpdated.(float64)
			if ok {
				if lastUpdated < afterEpoch {
					if ctx.Debug > 1 {
						Printf("#%d) lastUpdated: %v < afterEpoch: %v, skipping\n", i, lastUpdated, afterEpoch)
					}
					continue
				}
			} else {
				Printf("cannot read float value from %v\n", iLastUpdated)
			}
		} else {
			Printf("cannot read lastUpdated from %v\n", item)
		}
		reviews = append(reviews, item)
	}
	return
}

// FetchItems - implement enrich data for stub datasource
func (j *DSGerrit) FetchItems(ctx *Ctx) (err error) {
	err = j.InitGerrit(ctx)
	if err != nil {
		return
	}
	if j.SSHKeyTempPath != "" {
		defer func() {
			Printf("removing temporary SSH key %s\n", j.SSHKeyTempPath)
			_ = os.Remove(j.SSHKeyTempPath)
		}()
	}
	// We don't have ancient gerrit versions like < 2.9 - this check is only for debugging
	if ctx.Debug > 1 {
		err = j.GetGerritVersion(ctx)
		if err != nil {
			return
		}
	}
	var (
		startFrom  int
		after      string
		afterEpoch float64
	)
	if ctx.DateFrom != nil {
		after = ToYMDHMSDate(*ctx.DateFrom)
		afterEpoch = float64(ctx.DateFrom.Unix())
	} else {
		after = "1970-01-01 00:00:00"
		afterEpoch = 0.0
	}
	var (
		ch            chan error
		allReviews    []interface{}
		allReviewsMtx *sync.Mutex
		escha         []chan error
		eschaMtx      *sync.Mutex
	)
	thrN := GetThreadsNum(ctx)
	if thrN > 1 {
		ch = make(chan error)
		allReviewsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processReview := func(c chan error, review map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		esItem := j.AddMetadata(ctx, review)
		if ctx.Project != "" {
			review["project"] = ctx.Project
		}
		esItem["data"] = review
		if allReviewsMtx != nil {
			allReviewsMtx.Lock()
		}
		allReviews = append(allReviews, esItem)
		nReviews := len(allReviews)
		if nReviews >= ctx.ESBulkSize {
			sendToElastic := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = SendToElastic(ctx, j, true, UUID, allReviews)
				if ee != nil {
					Printf("error %v sending %d reviews to ElasticSearch\n", ee, len(allReviews))
				}
				allReviews = []interface{}{}
				if allReviewsMtx != nil {
					allReviewsMtx.Unlock()
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
			if allReviewsMtx != nil {
				allReviewsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for {
			var reviews []map[string]interface{}
			reviews, startFrom, err = j.GetGerritReviews(ctx, after, afterEpoch, startFrom)
			if err != nil {
				return
			}
			for _, review := range reviews {
				go func(review map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processReview(ch, review)
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
				}(review)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			}
			if startFrom == 0 {
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
			var reviews []map[string]interface{}
			reviews, startFrom, err = j.GetGerritReviews(ctx, after, afterEpoch, startFrom)
			if err != nil {
				return
			}
			for _, review := range reviews {
				_, err = processReview(nil, review)
				if err != nil {
					return
				}
			}
			if startFrom == 0 {
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
	nReviews := len(allReviews)
	if ctx.Debug > 0 {
		Printf("%d remaining reviews to send to ES\n", nReviews)
	}
	if nReviews > 0 {
		err = SendToElastic(ctx, j, true, UUID, allReviews)
		if err != nil {
			Printf("Error %v sending %d reviews to ES\n", err, len(allReviews))
		}
	}
	return
}

// SupportDateFrom - does DS support resuming from date?
func (j *DSGerrit) SupportDateFrom() bool {
	// A bit dangerous, because if any run failed then one review can set max update to some recent date
	// while other reviews can be left at a lower date
	return true
}

// SupportOffsetFrom - does DS support resuming from offset?
func (j *DSGerrit) SupportOffsetFrom() bool {
	return false
}

// DateField - return date field used to detect where to restart from
func (j *DSGerrit) DateField(*Ctx) string {
	return DefaultDateField
}

// RichIDField - return rich ID field name
func (j *DSGerrit) RichIDField(*Ctx) string {
	// IMPL:
	return DefaultIDField
}

// RichAuthorField - return rich ID field name
func (j *DSGerrit) RichAuthorField(*Ctx) string {
	return DefaultAuthorField
}

// OffsetField - return offset field used to detect where to restart from
func (j *DSGerrit) OffsetField(*Ctx) string {
	return DefaultOffsetField
}

// OriginField - return origin field used to detect where to restart from
func (j *DSGerrit) OriginField(ctx *Ctx) string {
	if ctx.Tag != "" {
		return DefaultTagField
	}
	return DefaultOriginField
}

// Categories - return a set of configured categories
func (j *DSGerrit) Categories() map[string]struct{} {
	return GerritCategories
}

// ResumeNeedsOrigin - is origin field needed when resuming
// Origin should be needed when multiple configurations save to the same index
func (j *DSGerrit) ResumeNeedsOrigin(ctx *Ctx) bool {
	return !j.SingleOrigin
}

// Origin - return current origin
func (j *DSGerrit) Origin(ctx *Ctx) string {
	return j.URL
}

// ItemID - return unique identifier for an item
func (j *DSGerrit) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})["number"].(float64)
	if !ok {
		Fatalf("%s: ItemID() - cannot extract number from %+v", j.DS, DumpKeys(item))
	}
	return fmt.Sprintf("%.0f", id)
}

// AddMetadata - add metadata to the item
func (j *DSGerrit) AddMetadata(ctx *Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tag := ctx.Tag
	if tag == "" {
		tag = origin
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = j.DS
	mItem["backend_version"] = GerritBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e3)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem["updated_on"] = updatedOn
	mItem["category"] = j.ItemCategory(item)
	mItem["search_fields"] = make(map[string]interface{})
	project, _ := Dig(item, []string{"project"}, true, false)
	hash, _ := Dig(item, []string{"id"}, true, false)
	FatalOnError(DeepSet(mItem, []string{"search_fields", GerritDefaultSearchField}, itemID, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "project_name"}, project, false))
	FatalOnError(DeepSet(mItem, []string{"search_fields", "review_hash"}, hash, false))
	mItem[DefaultDateField] = ToESDate(updatedOn)
	mItem[DefaultTimestampField] = ToESDate(timestamp)
	mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGerrit) ItemUpdatedOn(item interface{}) time.Time {
	epoch, ok := item.(map[string]interface{})["lastUpdated"].(float64)
	if !ok {
		Fatalf("%s: ItemUpdatedOn() - cannot extract lastUpdated from %+v", j.DS, DumpKeys(item))
	}
	return time.Unix(int64(epoch), 0)
}

// ItemCategory - return unique identifier for an item
func (j *DSGerrit) ItemCategory(item interface{}) string {
	return Review
}

// ElasticRawMapping - Raw index mapping definition
func (j *DSGerrit) ElasticRawMapping() []byte {
	return GerritRawMapping
}

// ElasticRichMapping - Rich index mapping definition
func (j *DSGerrit) ElasticRichMapping() []byte {
	return GerritRichMapping
}

// IdentityForObject - construct identity from a given object
func (j *DSGerrit) IdentityForObject(ctx *Ctx, obj map[string]interface{}) (identity [3]string) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("%+v -> %+v\n", obj, identity)
		}()
	}
	item := obj
	data, ok := Dig(item, []string{"data"}, false, true)
	if ok {
		mp, ok := data.(map[string]interface{})
		if ok {
			if ctx.Debug > 2 {
				Printf("digged in data: %+v\n", obj)
			}
			item = mp
		}
	}
	for i, prop := range []string{"name", "username", "email"} {
		iVal, ok := Dig(item, []string{prop}, false, true)
		if ok {
			val, ok := iVal.(string)
			if ok {
				identity[i] = val
			}
		} else {
			identity[i] = Nil
		}
	}
	return
}

// GetItemIdentities return list of item's identities, each one is [3]string
// (name, username, email) tripples, special value Nil "<nil>" means null
// we use string and not *string which allows nil to allow usage as a map key
func (j *DSGerrit) GetItemIdentities(ctx *Ctx, doc interface{}) (identities map[[3]string]struct{}, err error) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("%+v -> %+v\n", DumpPreview(doc, 100), identities)
		}()
	}
	init := false
	item, _ := Dig(doc, []string{"data"}, true, false)
	iUser, ok := Dig(item, []string{"owner"}, false, true)
	if ok {
		user, ok := iUser.(map[string]interface{})
		if ok {
			if !init {
				identities = make(map[[3]string]struct{})
				init = true
			}
			identities[j.IdentityForObject(ctx, user)] = struct{}{}
		}
	}
	iPatchSets, ok := Dig(item, []string{"patchSets"}, false, true)
	if ok {
		patchSets, ok := iPatchSets.([]interface{})
		if ok {
			for _, iPatch := range patchSets {
				patch, ok := iPatch.(map[string]interface{})
				if !ok {
					continue
				}
				iUploader, ok := Dig(patch, []string{"uploader"}, false, true)
				if ok {
					uploader, ok := iUploader.(map[string]interface{})
					if ok {
						if !init {
							identities = make(map[[3]string]struct{})
							init = true
						}
						identities[j.IdentityForObject(ctx, uploader)] = struct{}{}
					}
				}
				iAuthor, ok := Dig(patch, []string{"author"}, false, true)
				if ok {
					author, ok := iAuthor.(map[string]interface{})
					if ok {
						if !init {
							identities = make(map[[3]string]struct{})
							init = true
						}
						identities[j.IdentityForObject(ctx, author)] = struct{}{}
					}
				}
				iApprovals, ok := Dig(patch, []string{"approvals"}, false, true)
				if ok {
					approvals, ok := iApprovals.([]interface{})
					if ok {
						for _, iApproval := range approvals {
							approval, ok := iApproval.(map[string]interface{})
							if !ok {
								continue
							}
							iBy, ok := Dig(approval, []string{"by"}, false, true)
							if ok {
								by, ok := iBy.(map[string]interface{})
								if ok {
									if !init {
										identities = make(map[[3]string]struct{})
										init = true
									}
									identities[j.IdentityForObject(ctx, by)] = struct{}{}
								}
							}
						}
					}
				}
			}
		}
	}
	iComments, ok := Dig(item, []string{"comments"}, false, true)
	if ok {
		comments, ok := iComments.([]interface{})
		if ok {
			for _, iComment := range comments {
				comment, ok := iComment.(map[string]interface{})
				if !ok {
					continue
				}
				iReviewer, ok := Dig(comment, []string{"reviewer"}, false, true)
				if ok {
					reviewer, ok := iReviewer.(map[string]interface{})
					if ok {
						if !init {
							identities = make(map[[3]string]struct{})
							init = true
						}
						identities[j.IdentityForObject(ctx, reviewer)] = struct{}{}
					}
				}
			}
		}
	}
	return
}

// GerritEnrichItemsFunc - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func GerritEnrichItemsFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("gerrit enrich items %d/%d func\n", len(items), len(*docs))
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
	gerrit, _ := ds.(*DSGerrit)
	getRichItems := func(doc map[string]interface{}) (richItems []interface{}, e error) {
		var rich map[string]interface{}
		rich, e = ds.EnrichItem(ctx, doc, "", dbConfigured, nil)
		if e != nil {
			return
		}
		richItems = append(richItems, rich)
		data, _ := Dig(doc, []string{"data"}, true, false)
		iPatchSets, ok := Dig(data, []string{"patchSets"}, false, true)
		if ok {
			patchSets, ok := iPatchSets.([]interface{})
			if ok {
				var patches []map[string]interface{}
				for _, iPatch := range patchSets {
					patch, ok := iPatch.(map[string]interface{})
					if !ok {
						continue
					}
					patches = append(patches, patch)
				}
				if len(patches) > 0 {
					var riches []interface{}
					riches, e = gerrit.EnrichPatchsets(ctx, patches, dbConfigured)
					if e != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
		iComments, ok := Dig(data, []string{"comments"}, false, true)
		if ok {
			comments, ok := iComments.([]interface{})
			if ok {
				var comms []map[string]interface{}
				for _, iComment := range comments {
					comment, ok := iComment.(map[string]interface{})
					if !ok {
						continue
					}
					comms = append(comms, comment)
				}
				if len(comms) > 0 {
					var riches []interface{}
					riches, e = gerrit.EnrichComments(ctx, comms, dbConfigured)
					if e != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
		return
	}
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
		richItems, e := getRichItems(doc)
		if e != nil {
			return
		}
		for _, rich := range richItems {
			e = EnrichItem(ctx, ds, rich.(map[string]interface{}))
			if e != nil {
				return
			}
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, richItems...)
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
func (j *DSGerrit) EnrichItems(ctx *Ctx) (err error) {
	Printf("enriching items\n")
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GerritEnrichItemsFunc, nil)
	return
}

// EnrichItem - return rich item from raw item
func (j *DSGerrit) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	// FIXME
	return
}

// EnrichPatchsets - return rich items from raw patch sets
func (j *DSGerrit) EnrichPatchsets(ctx *Ctx, patches []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// FIXME
	return
}

// EnrichComments - return rich items from raw patch sets
func (j *DSGerrit) EnrichComments(ctx *Ctx, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	// FIXME
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGerrit) AffsItems(ctx *Ctx, rawItem map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	// IMPL:
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGerrit) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) map[string]interface{} {
	// IMPL:
	return map[string]interface{}{"name": nil, "username": nil, "email": nil}
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGerrit) AllRoles(ctx *Ctx, item map[string]interface{}) ([]string, bool) {
	// IMPL:
	return []string{Author}, true
}
