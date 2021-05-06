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
	GerritBackendVersion = "0.1.1"
	// GerritDefaultSSHKeyPath - default path to look for gerrit ssh private key
	GerritDefaultSSHKeyPath = "$HOME/.ssh/id_rsa"
	// GerritDefaultSSHPort - default gerrit ssh port
	GerritDefaultSSHPort = 29418
	// GerritDefaultMaxReviews = default max reviews when processing gerrit
	GerritDefaultMaxReviews = 1000
	// GerritCodeReviewApprovalType - code review approval type
	GerritCodeReviewApprovalType = "Code-Review"
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
	// GerritReviewRoles - roles to fetch affiliation data for review
	GerritReviewRoles = []string{"owner"}
	// GerritCommentRoles - roles to fetch affiliation data for comment
	GerritCommentRoles = []string{"reviewer"}
	// GerritPatchsetRoles - roles to fetch affiliation data for patchset
	GerritPatchsetRoles = []string{"author", "uploader"}
	// GerritApprovalRoles - roles to fetch affiliation data for approval
	GerritApprovalRoles = []string{"by"}
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
	MaxReviews          int    // From DA_GERRIT_MAX_REVIEWS, defaults to GerritDefaultMaxReviews (1000)
	NoSSLVerify         bool   // From DA_GERRIT_NO_SSL_VERIFY
	DisableHostKeyCheck bool   // From DA_GERRIT_DISABLE_HOST_KEY_CHECK
	// Non-config variables
	SSHOpts        string   // SSH Options
	SSHKeyTempPath string   // if used SSHKey - temp file with this name was used to store key contents
	GerritCmd      []string // gerrit remote command used to fetch data
	VersionMajor   int      // gerrit major version
	VersionMinor   int      // gerrit minor version
}

// ParseArgs - parse gerrit specific environment variables
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
func (j *DSGerrit) Validate(ctx *Ctx) (err error) {
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
	if j.URL == "" || j.User == "" {
		err = fmt.Errorf("URL and user must be set")
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
func (j *DSGerrit) ResumeNeedsOrigin(ctx *Ctx, raw bool) bool {
	return !j.SingleOrigin
}

// ResumeNeedsCategory - is category field needed when resuming
// Category should be needed when multiple types of categories save to the same index
// or there are multiple types of documents within the same category
func (j *DSGerrit) ResumeNeedsCategory(ctx *Ctx, raw bool) bool {
	return false
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
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem[UUID] = uuid
	mItem[DefaultOriginField] = origin
	mItem[DefaultTagField] = tag
	mItem[DefaultOffsetField] = float64(updatedOn.Unix())
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
// (name, username, email) tripples, special value Nil "none" means null
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
		/*
			defer func() {
				m := make(map[string]struct{})
				if len(richItems) < 10 {
					return
				}
				for _, iRich := range richItems {
					rich, ok := iRich.(map[string]interface{})
					if !ok {
						continue
					}
					it, ok := rich["type"]
					if !ok {
						continue
					}
					t, ok := it.(string)
					if !ok {
						continue
					}
					m[t] = struct{}{}
				}
				if len(m) < 4 {
					return
				}
				s := "\n"
				for i, rich := range richItems {
					s += fmt.Sprintf("%d) %+v\n", i+1, PreviewOnly(rich, 128))
				}
				Printf("%s\n", s)
			}()
		*/
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
					riches, e = gerrit.EnrichPatchsets(ctx, rich, patches, dbConfigured)
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
					riches, e = gerrit.EnrichComments(ctx, rich, comms, dbConfigured)
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
			e = fmt.Errorf("Failed to parse document %+v", doc)
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
	err = ForEachESItem(ctx, j, true, ESBulkUploadFunc, GerritEnrichItemsFunc, nil, true)
	return
}

// ConvertDates - convert floating point dates to datetimes
func (j *DSGerrit) ConvertDates(ctx *Ctx, review map[string]interface{}) {
	for _, field := range []string{"timestamp", "createdOn", "lastUpdated"} {
		idt, ok := Dig(review, []string{field}, false, true)
		if !ok {
			continue
		}
		fdt, ok := idt.(float64)
		if !ok {
			continue
		}
		review[field] = time.Unix(int64(fdt), 0)
		// Printf("converted %s: %v -> %v\n", field, idt, review[field])
	}
	iPatchSets, ok := Dig(review, []string{"patchSets"}, false, true)
	if ok {
		patchSets, ok := iPatchSets.([]interface{})
		if ok {
			for _, iPatch := range patchSets {
				patch, ok := iPatch.(map[string]interface{})
				if !ok {
					continue
				}
				field := "createdOn"
				idt, ok := Dig(patch, []string{field}, false, true)
				if ok {
					fdt, ok := idt.(float64)
					if ok {
						patch[field] = time.Unix(int64(fdt), 0)
						// Printf("converted patch %s: %v -> %v\n", field, idt, patch[field])
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
							field := "grantedOn"
							idt, ok := Dig(approval, []string{field}, false, true)
							if ok {
								fdt, ok := idt.(float64)
								if ok {
									approval[field] = time.Unix(int64(fdt), 0)
									// Printf("converted patch approval %s: %v -> %v\n", field, idt, approval[field])
								}
							}
						}
					}
				}
			}
		}
	}
	iComments, ok := Dig(review, []string{"comments"}, false, true)
	if ok {
		comments, ok := iComments.([]interface{})
		if ok {
			for _, iComment := range comments {
				comment, ok := iComment.(map[string]interface{})
				if !ok {
					continue
				}
				field := "timestamp"
				idt, ok := Dig(comment, []string{field}, false, true)
				if ok {
					fdt, ok := idt.(float64)
					if ok {
						comment[field] = time.Unix(int64(fdt), 0)
						// Printf("converted comment %s: %v -> %v\n", field, idt, comment[field])
					}
				}
			}
		}
	}
}

// FirstReviewDatetime - return first review date/time
func (j *DSGerrit) FirstReviewDatetime(ctx *Ctx, review map[string]interface{}, patchSets []interface{}) (reviewDatetime interface{}) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("FirstReviewDatetime: %+v -> %+v\n", patchSets, reviewDatetime)
		}()
	}
	if ctx.Debug > 2 {
		Printf("FirstReviewDatetime: %d patch sets\n", len(patchSets))
	}
	ownerUsername, okOwnerUsername := Dig(review, []string{"owner", "username"}, false, true)
	ownerEmail, okOwnerEmail := Dig(review, []string{"owner", "email"}, false, true)
	if ownerUsername == "" {
		okOwnerUsername = false
	}
	if ownerEmail == "" {
		okOwnerEmail = false
	}
	var createdOn time.Time
	iCreatedOn, okCreatedOn := review["createdOn"]
	if okCreatedOn {
		createdOn, okCreatedOn = iCreatedOn.(time.Time)
	}
	for _, iPatchSet := range patchSets {
		patchSet, ok := iPatchSet.(map[string]interface{})
		if !ok {
			continue
		}
		iApprovals, ok := patchSet["approvals"]
		if !ok {
			if ctx.Debug > 2 {
				Printf("FirstReviewDatetime: no approvals\n")
			}
			continue
		}
		approvals, ok := iApprovals.([]interface{})
		if !ok {
			continue
		}
		if ctx.Debug > 2 {
			Printf("FirstReviewDatetime: %d approvals\n", len(approvals))
		}
		for _, iApproval := range approvals {
			approval, ok := iApproval.(map[string]interface{})
			if !ok {
				continue
			}
			iApprovalType, ok := approval["type"]
			if !ok {
				continue
			}
			approvalType, ok := iApprovalType.(string)
			if !ok || approvalType != GerritCodeReviewApprovalType {
				if ctx.Debug > 2 {
					Printf("FirstReviewDatetime: incorrect type %+v\n", iApprovalType)
				}
				continue
			}
			iGrantedOn, okGrantedOn := approval["grantedOn"]
			var grantedOn time.Time
			if okCreatedOn && okGrantedOn {
				grantedOn, okGrantedOn = iGrantedOn.(time.Time)
				if okGrantedOn && grantedOn.Before(createdOn) {
					Printf("approval granted before patchset was created %+v < %+v, skipping\n", grantedOn, createdOn)
					continue
				}
			}
			// Printf("FirstReviewDatetime: (%+v,%T,%v) <=> (%+v,%T,%+v)\n", createdOn, createdOn, okCreatedOn, grantedOn, grantedOn, okGrantedOn)
			byUsername, okByUsername := Dig(approval, []string{"by", "username"}, false, true)
			byEmail, okByEmail := Dig(approval, []string{"by", "email"}, false, true)
			if byUsername == "" {
				okByUsername = false
			}
			if byEmail == "" {
				okByEmail = false
			}
			// Printf("FirstReviewDatetime: (%s,%s,%s,%s) (%v,%v,%v,%v)\n", ownerUsername, ownerEmail, byUsername, byEmail, okOwnerUsername, okOwnerEmail, okByUsername, okByEmail)
			var okReviewDatetime bool
			if okByUsername && okOwnerUsername {
				// Printf("FirstReviewDatetime: usernames set\n")
				byUName, _ := byUsername.(string)
				ownerUName, _ := ownerUsername.(string)
				if byUName != ownerUName {
					reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
				}
			} else if okByEmail && okOwnerEmail {
				// Printf("FirstReviewDatetime: emails set\n")
				byMail, _ := byEmail.(string)
				ownerMail, _ := ownerEmail.(string)
				if byMail != ownerMail {
					reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
				}
			} else {
				// Printf("FirstReviewDatetime: else case\n")
				reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
			}
			if ctx.Debug > 2 {
				Printf("FirstReviewDatetime: final (%+v,%+v)\n", reviewDatetime, okReviewDatetime)
			}
			if okReviewDatetime && reviewDatetime != nil {
				return
			}
		}
	}
	return
}

// FirstPatchsetReviewDatetime - return first patchset review date/time
func (j *DSGerrit) FirstPatchsetReviewDatetime(ctx *Ctx, patchSet map[string]interface{}) (reviewDatetime interface{}) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("FirstPatchsetReviewDatetime: %+v -> %+v\n", patchSet, reviewDatetime)
		}()
	}
	patchsetUsername, okPatchsetUsername := Dig(patchSet, []string{"author", "username"}, false, true)
	patchsetEmail, okPatchsetEmail := Dig(patchSet, []string{"author", "email"}, false, true)
	if patchsetUsername == "" {
		okPatchsetUsername = false
	}
	if patchsetEmail == "" {
		okPatchsetEmail = false
	}
	var createdOn time.Time
	iCreatedOn, okCreatedOn := patchSet["createdOn"]
	if okCreatedOn {
		createdOn, okCreatedOn = iCreatedOn.(time.Time)
	}
	iApprovals, ok := patchSet["approvals"]
	if !ok {
		if ctx.Debug > 2 {
			Printf("FirstPatchsetReviewDatetime: no approvals\n")
		}
		return
	}
	approvals, ok := iApprovals.([]interface{})
	if !ok {
		return
	}
	if ctx.Debug > 2 {
		Printf("FirstPatchsetReviewDatetime: %d approvals\n", len(approvals))
	}
	for _, iApproval := range approvals {
		approval, ok := iApproval.(map[string]interface{})
		if !ok {
			continue
		}
		iApprovalType, ok := approval["type"]
		if !ok {
			continue
		}
		approvalType, ok := iApprovalType.(string)
		if !ok || approvalType != GerritCodeReviewApprovalType {
			if ctx.Debug > 2 {
				Printf("FirstPatchsetReviewDatetime: incorrect type %+v\n", iApprovalType)
			}
			continue
		}
		iGrantedOn, okGrantedOn := approval["grantedOn"]
		var grantedOn time.Time
		if okCreatedOn && okGrantedOn {
			grantedOn, okGrantedOn = iGrantedOn.(time.Time)
			if okGrantedOn && grantedOn.Before(createdOn) {
				Printf("approval granted before patchset was created %+v < %+v, skipping\n", grantedOn, createdOn)
				continue
			}
		}
		// Printf("FirstPatchsetReviewDatetime: (%+v,%T,%v) <=> (%+v,%T,%+v)\n", createdOn, createdOn, okCreatedOn, grantedOn, grantedOn, okGrantedOn)
		byUsername, okByUsername := Dig(approval, []string{"by", "username"}, false, true)
		byEmail, okByEmail := Dig(approval, []string{"by", "email"}, false, true)
		if byUsername == "" {
			okByUsername = false
		}
		if byEmail == "" {
			okByEmail = false
		}
		// Printf("FirstPatchesReviewDatetime: (%s,%s,%s,%s) (%v,%v,%v,%v)\n", patchsetUsername, patchsetEmail, byUsername, byEmail, okPatchsetUsername, okPatchsetEmail, okByUsername, okByEmail)
		var okReviewDatetime bool
		if okByUsername && okPatchsetUsername {
			//Printf("FirstPatchsetReviewDatetime: usernames set\n")
			byUName, _ := byUsername.(string)
			patchsetUName, _ := patchsetUsername.(string)
			if byUName != patchsetUName {
				reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
			}
		} else if okByEmail && okPatchsetEmail {
			// Printf("FirstPatchsetReviewDatetime: emails set\n")
			byMail, _ := byEmail.(string)
			patchsetMail, _ := patchsetEmail.(string)
			if byMail != patchsetMail {
				reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
			}
		} else {
			// Printf("FirstPatchsetReviewDatetime: else case\n")
			reviewDatetime, okReviewDatetime = grantedOn, okGrantedOn
		}
		if ctx.Debug > 2 {
			Printf("FirstPatchsetReviewDatetime: final (%+v,%+v)\n", reviewDatetime, okReviewDatetime)
		}
		if okReviewDatetime && reviewDatetime != nil {
			// Printf("FirstPatchsetReviewDatetime: hit (%+v,%+v)\n%+v\n", reviewDatetime, okReviewDatetime, patchSet)
			return
		}
	}
	return
}

// LastChangesetApprovalValue - return last approval status
func (j *DSGerrit) LastChangesetApprovalValue(ctx *Ctx, patchSets []interface{}) (status interface{}) {
	if ctx.Debug > 2 {
		defer func() {
			Printf("LastChangesetApprovalValue: %+v -> %+v\n", patchSets, status)
		}()
	}
	nPatchSets := len(patchSets)
	if ctx.Debug > 2 {
		Printf("LastChangesetApprovalValue: %d patch sets\n", nPatchSets)
	}
	for i := nPatchSets - 1; i >= 0; i-- {
		iPatchSet := patchSets[i]
		patchSet, ok := iPatchSet.(map[string]interface{})
		if !ok {
			continue
		}
		iApprovals, ok := patchSet["approvals"]
		if !ok {
			if ctx.Debug > 2 {
				Printf("LastChangesetApprovalValue: no approvals\n")
			}
			continue
		}
		approvals, ok := iApprovals.([]interface{})
		if !ok {
			continue
		}
		authorUsername, okAuthorUsername := Dig(patchSet, []string{"author", "username"}, false, true)
		authorEmail, okAuthorEmail := Dig(patchSet, []string{"author", "email"}, false, true)
		if authorUsername == "" {
			okAuthorUsername = false
		}
		if authorEmail == "" {
			okAuthorEmail = false
		}
		nApprovals := len(approvals)
		if ctx.Debug > 2 {
			Printf("LastChangesetApprovalValue: %d approvals\n", nApprovals)
		}
		for j := nApprovals - 1; j >= 0; j-- {
			iApproval := approvals[j]
			approval, ok := iApproval.(map[string]interface{})
			if !ok {
				continue
			}
			iApprovalType, ok := approval["type"]
			if !ok {
				continue
			}
			approvalType, ok := iApprovalType.(string)
			if !ok || approvalType != GerritCodeReviewApprovalType {
				if ctx.Debug > 2 {
					Printf("LastChangesetApprovalValue: incorrect type %+v\n", iApprovalType)
				}
				continue
			}
			byUsername, okByUsername := Dig(approval, []string{"by", "username"}, false, true)
			byEmail, okByEmail := Dig(approval, []string{"by", "email"}, false, true)
			if byUsername == "" {
				okByUsername = false
			}
			if byEmail == "" {
				okByEmail = false
			}
			// Printf("LastChangesetApprovalValue: (%s,%s,%s,%s) (%v,%v,%v,%v)\n", authorUsername, authorEmail, byUsername, byEmail, okAuthorUsername, okAuthorEmail, okByUsername, okByEmail)
			var okStatus bool
			if okByUsername && okAuthorUsername {
				// Printf("LastChangesetApprovalValue: usernames set\n")
				byUName, _ := byUsername.(string)
				authorUName, _ := authorUsername.(string)
				if byUName != authorUName {
					status, okStatus = approval["value"]
				}
			} else if okByEmail && okAuthorEmail {
				// Printf("LastChangesetApprovalValue: emails set\n")
				byMail, _ := byEmail.(string)
				authorMail, _ := authorEmail.(string)
				if byMail != authorMail {
					status, okStatus = approval["value"]
				}
			} else {
				// Printf("LastChangesetApprovalValue: else case\n")
				status, okStatus = approval["value"]
			}
			if ctx.Debug > 2 {
				Printf("LastChangesetApprovalValue: final (%+v,%+v)\n", status, okStatus)
			}
			if okStatus && status != nil {
				return
			}
		}
	}
	return
}

// EnrichItem - return rich item from raw item
func (j *DSGerrit) EnrichItem(ctx *Ctx, item map[string]interface{}, author string, affs bool, extra interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	for _, field := range RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	updatedOn, _ := Dig(item, []string{DefaultDateField}, true, false)
	rich["closed"] = updatedOn
	review, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", DumpPreview(item, 100))
		return
	}
	j.ConvertDates(ctx, review)
	iReviewStatus, ok := review["status"]
	var reviewStatus string
	if ok {
		reviewStatus, _ = iReviewStatus.(string)
	}
	rich["status"] = reviewStatus
	rich["branch"], _ = review["branch"]
	rich["url"], _ = review["url"]
	rich["githash"], _ = review["id"]
	var createdOn time.Time
	iCreatedOn, ok := review["createdOn"]
	if ok {
		createdOn, _ = iCreatedOn.(time.Time)
	}
	rich["opened"] = createdOn
	rich["repository"], _ = review["project"]
	rich["repo_short_name"], _ = rich["repository"]
	rich["changeset_number"], _ = review["number"]
	uuid, ok := rich[UUID].(string)
	if !ok {
		err = fmt.Errorf("cannot read string uuid from %+v", DumpPreview(rich, 100))
		return
	}
	changesetNumber := j.ItemID(review)
	rich["id"] = uuid + "_changeset_" + changesetNumber
	summary := ""
	iSummary, ok := review["subject"]
	if ok {
		summary, _ = iSummary.(string)
	}
	rich["summary_analyzed"] = summary
	if len(summary) > KeywordMaxlength {
		summary = summary[:KeywordMaxlength]
	}
	rich["summary"] = summary
	rich["name"] = nil
	rich["domain"] = nil
	ownerName, ok := Dig(review, []string{"owner", "name"}, false, true)
	if ok {
		rich["name"] = ownerName
		iOwnerEmail, ok := Dig(review, []string{"owner", "email"}, false, true)
		if ok {
			ownerEmail, ok := iOwnerEmail.(string)
			if ok {
				ary := strings.Split(ownerEmail, "@")
				if len(ary) > 1 {
					rich["domain"] = strings.TrimSpace(ary[1])
				}
			}
		}
	}
	iPatchSets, ok := Dig(review, []string{"patchSets"}, false, true)
	nPatchSets := 0
	var patchSets []interface{}
	if ok {
		patchSets, ok = iPatchSets.([]interface{})
		if ok {
			nPatchSets = len(patchSets)
			firstPatch, ok := patchSets[0].(map[string]interface{})
			if ok {
				iCreatedOn, ok = firstPatch["createdOn"]
				if ok {
					createdOn, _ = iCreatedOn.(time.Time)
				}
			}
		}
	}
	rich["created_on"] = createdOn
	rich["patchsets"] = nPatchSets
	status := j.LastChangesetApprovalValue(ctx, patchSets)
	rich["status_value"] = status
	rich["changeset_status_value"] = status
	rich["changeset_status"] = reviewStatus
	iFirstReviewDt := j.FirstReviewDatetime(ctx, review, patchSets)
	rich["first_review_date"] = iFirstReviewDt
	rich["time_to_first_review"] = nil
	if iFirstReviewDt != nil {
		firstReviewDt, ok := iFirstReviewDt.(time.Time)
		if ok {
			rich["time_to_first_review"] = float64(firstReviewDt.Sub(createdOn).Seconds()) / 86400.0
		}
	}
	var lastUpdatedOn time.Time
	iLastUpdatedOn, ok := review["lastUpdated"]
	if ok {
		lastUpdatedOn, _ = iLastUpdatedOn.(time.Time)
	}
	rich["last_updated"] = lastUpdatedOn
	if reviewStatus == "MERGED" || reviewStatus == "ABANDONED" {
		rich["timeopen"] = float64(lastUpdatedOn.Sub(createdOn).Seconds()) / 86400.0
	} else {
		rich["timeopen"] = float64(time.Now().Sub(createdOn).Seconds()) / 86400.0
	}
	wip, ok := Dig(review, []string{"wip"}, false, true)
	if ok {
		rich["wip"] = wip
	} else {
		rich["wip"] = false
	}
	rich["open"], _ = Dig(review, []string{"open"}, false, true)
	rich["type"] = Changeset
	if affs {
		authorKey := "owner"
		var affsItems map[string]interface{}
		affsItems, err = j.AffsItems(ctx, review, GerritReviewRoles, updatedOn)
		if err != nil {
			return
		}
		for prop, value := range affsItems {
			rich[prop] = value
		}
		changesetRole := Changeset + "_" + Author
		for _, suff := range AffsFields {
			rich[Author+suff] = rich[authorKey+suff]
			// Copy to changeset object
			rich[changesetRole+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + MultiOrgNames
		_, ok := Dig(rich, []string{orgsKey}, false, true)
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
		// Copy to changeset object
		rich[changesetRole+MultiOrgNames] = rich[orgsKey]
	}
	for prop, value := range CommonFields(j, createdOn, Review) {
		rich[prop] = value
	}
	for prop, value := range CommonFields(j, createdOn, Changeset) {
		rich[prop] = value
	}
	return
}

// EnrichPatchsets - return rich items from raw patch sets
func (j *DSGerrit) EnrichPatchsets(ctx *Ctx, review map[string]interface{}, patchSets []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "changeset_status", "changeset_status_value", "repo_short_name"}
	iReviewID, ok := review["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of review: %+v", review)
		return
	}
	reviewID, ok := iReviewID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of review: %+v", iReviewID)
		return
	}
	for _, patchSet := range patchSets {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := review[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = review[field]
		}
		rich["patchset_author_name"] = nil
		rich["patchset_author_domain"] = nil
		authorName, ok := Dig(patchSet, []string{"author", "name"}, false, true)
		if ok {
			rich["patchset_author_name"] = authorName
			iAuthorEmail, ok := Dig(patchSet, []string{"author", "email"}, false, true)
			if ok {
				authorEmail, ok := iAuthorEmail.(string)
				if ok {
					ary := strings.Split(authorEmail, "@")
					if len(ary) > 1 {
						rich["patchset_author_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		rich["patchset_uploader_name"] = nil
		rich["patchset_uploader_domain"] = nil
		uploaderName, ok := Dig(patchSet, []string{"uploader", "name"}, false, true)
		if ok {
			rich["patchset_uploader_name"] = uploaderName
			iUploaderEmail, ok := Dig(patchSet, []string{"uploader", "email"}, false, true)
			if ok {
				uploaderEmail, ok := iUploaderEmail.(string)
				if ok {
					ary := strings.Split(uploaderEmail, "@")
					if len(ary) > 1 {
						rich["patchset_uploader_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		var created time.Time
		iCreated, ok := patchSet["createdOn"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read createdOn property from patchSet: %+v", patchSet)
			return
		}
		rich["patchset_created_on"] = created
		number := patchSet["number"]
		rich["patchset_number"] = number
		rich["patchset_isDraft"], _ = patchSet["isDraft"]
		rich["patchset_kind"], _ = patchSet["kind"]
		rich["patchset_ref"], _ = patchSet["ref"]
		rich["patchset_revision"], _ = patchSet["revision"]
		rich["patchset_sizeDeletions"], _ = patchSet["sizeDeletions"]
		rich["patchset_sizeInsertions"], _ = patchSet["sizeInsertions"]
		iFirstReviewDt := j.FirstPatchsetReviewDatetime(ctx, patchSet)
		rich["patchset_first_review_date"] = iFirstReviewDt
		rich["patchset_time_to_first_review"] = nil
		if iFirstReviewDt != nil {
			firstReviewDt, ok := iFirstReviewDt.(time.Time)
			if ok {
				rich["patchset_time_to_first_review"] = float64(firstReviewDt.Sub(created).Seconds()) / 86400.0
			}
		}
		rich["type"] = Patchset
		rich["id"] = reviewID + "_patchset_" + fmt.Sprintf("%v", number)
		if affs {
			sCreated := ToYMDTHMSZDate(created)
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, patchSet, GerritPatchsetRoles, sCreated)
			if err != nil {
				return
			}
			for prop, value := range affsItems {
				rich[prop] = value
			}
			role := Changeset + "_" + Author
			CopyAffsRoleData(rich, review, role, role)
		}
		for prop, value := range CommonFields(j, iCreated, Review) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, iCreated, Patchset) {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
		iApprovals, ok := Dig(patchSet, []string{"approvals"}, false, true)
		if ok {
			approvalsAry, ok := iApprovals.([]interface{})
			if ok {
				var approvals []map[string]interface{}
				for _, iApproval := range approvalsAry {
					approval, ok := iApproval.(map[string]interface{})
					if !ok {
						continue
					}
					approvals = append(approvals, approval)
				}
				if len(approvals) > 0 {
					var riches []interface{}
					riches, err = j.EnrichApprovals(ctx, review, rich, approvals, affs)
					if err != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
	}
	return
}

// EnrichApprovals - return rich items from raw approvals
func (j *DSGerrit) EnrichApprovals(ctx *Ctx, review, patchSet map[string]interface{}, approvals []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	iPatchSetID, ok := patchSet["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of patchset: %+v", patchSet)
		return
	}
	patchSetID, ok := iPatchSetID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of patchset: %+v", iPatchSetID)
		return
	}
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "changeset_status", "changeset_status_value", "patchset_number", "patchset_revision", "patchset_ref", "repo_short_name"}
	for _, approval := range approvals {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := patchSet[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = patchSet[field]
		}
		rich["approval_author_name"] = nil
		rich["approval_author_domain"] = nil
		authorName, ok := Dig(approval, []string{"by", "name"}, false, true)
		if ok {
			rich["approval_author_name"] = authorName
			iAuthorEmail, ok := Dig(approval, []string{"by", "email"}, false, true)
			if ok {
				authorEmail, ok := iAuthorEmail.(string)
				if ok {
					ary := strings.Split(authorEmail, "@")
					if len(ary) > 1 {
						rich["approval_author_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		//
		var created time.Time
		iCreated, ok := approval["grantedOn"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read grantedOn property from approval: %+v", approval)
			return
		}
		rich["approval_granted_on"] = created
		rich["approval_value"], _ = approval["value"]
		rich["approval_type"], _ = approval["type"]
		desc := ""
		iDesc, ok := approval["description"]
		if ok {
			desc, _ = iDesc.(string)
		}
		rich["approval_description_analyzed"] = desc
		if len(desc) > KeywordMaxlength {
			desc = desc[:KeywordMaxlength]
		}
		rich["approval_description"] = desc
		rich["type"] = Approval
		rich["id"] = patchSetID + "_approval_" + fmt.Sprintf("%d.0", created.Unix())
		rich["changeset_created_on"], _ = review["created_on"]
		if affs {
			sCreated := ToYMDTHMSZDate(created)
			authorKey := "by"
			var affsItems map[string]interface{}
			affsItems, err = j.AffsItems(ctx, approval, GerritApprovalRoles, sCreated)
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
			role := Changeset + "_" + Author
			CopyAffsRoleData(rich, patchSet, role, role)
		}
		for prop, value := range CommonFields(j, iCreated, Review) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, iCreated, Approval) {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// EnrichComments - return rich items from raw patch sets
func (j *DSGerrit) EnrichComments(ctx *Ctx, review map[string]interface{}, comments []map[string]interface{}, affs bool) (richItems []interface{}, err error) {
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "repo_short_name"}
	iReviewID, ok := review["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of review: %+v", review)
		return
	}
	reviewID, ok := iReviewID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of review: %+v", iReviewID)
		return
	}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range RawFields {
			v, _ := review[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = review[field]
		}
		rich["reviewer_name"] = nil
		rich["reviewer_domain"] = nil
		reviewerName, ok := Dig(comment, []string{"reviewer", "name"}, false, true)
		if ok {
			rich["reviewer_name"] = reviewerName
			iReviewerEmail, ok := Dig(comment, []string{"reviewer", "email"}, false, true)
			if ok {
				reviewerEmail, ok := iReviewerEmail.(string)
				if ok {
					ary := strings.Split(reviewerEmail, "@")
					if len(ary) > 1 {
						rich["reviewer_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		var created time.Time
		iCreated, ok := comment["timestamp"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read timestamp property from comment: %+v", comment)
			return
		}
		rich["comment_created_on"] = created
		message := ""
		iMessage, ok := comment["message"]
		if ok {
			message, _ = iMessage.(string)
		}
		rich["comment_message_analyzed"] = message
		if len(message) > KeywordMaxlength {
			message = message[:KeywordMaxlength]
		}
		rich["comment_message"] = message
		rich["type"] = Comment
		rich["id"] = reviewID + "_comment_" + fmt.Sprintf("%d.0", created.Unix())
		if affs {
			sCreated := ToYMDTHMSZDate(created)
			_, okReviewer := comment["reviewer"]
			if okReviewer {
				authorKey := "reviewer"
				var affsItems map[string]interface{}
				affsItems, err = j.AffsItems(ctx, comment, GerritCommentRoles, sCreated)
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
			role := Changeset + "_" + Author
			CopyAffsRoleData(rich, review, role, role)
		}
		for prop, value := range CommonFields(j, iCreated, Review) {
			rich[prop] = value
		}
		for prop, value := range CommonFields(j, iCreated, Comment) {
			rich[prop] = value
		}
		richItems = append(richItems, rich)
	}
	return
}

// AffsItems - return affiliations data items for given roles and date
func (j *DSGerrit) AffsItems(ctx *Ctx, review map[string]interface{}, roles []string, date interface{}) (affsItems map[string]interface{}, err error) {
	affsItems = make(map[string]interface{})
	var dt time.Time
	dt, err = TimeParseInterfaceString(date)
	if err != nil {
		return
	}
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, review, role)
		if len(identity) == 0 {
			continue
		}
		affsIdentity, empty, e := IdentityAffsData(ctx, j, identity, nil, dt, role)
		if e != nil {
			Printf("AffsItems/IdentityAffsData: error: %v for %v,%v,%v\n", e, identity, dt, role)
		}
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
func (j *DSGerrit) GetRoleIdentity(ctx *Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	iRole, ok := Dig(item, []string{role}, false, true)
	if ok {
		roleObj, ok := iRole.(map[string]interface{})
		if ok {
			ident := j.IdentityForObject(ctx, roleObj)
			identity = map[string]interface{}{"name": ident[0], "username": ident[1], "email": ident[2]}
		}
	}
	return
}

// AllRoles - return all roles defined for the backend
// roles can be static (always the same) or dynamic (per item)
// second return parameter is static mode (true/false)
// dynamic roles will use item to get its roles
func (j *DSGerrit) AllRoles(ctx *Ctx, rich map[string]interface{}) (roles []string, static bool) {
	roles = []string{Author}
	if rich == nil {
		return
	}
	iType, ok := Dig(rich, []string{"type"}, false, true)
	if !ok {
		return
	}
	tp, ok := iType.(string)
	if !ok {
		return
	}
	var possibleRoles []string
	switch tp {
	case Changeset:
		possibleRoles = GerritReviewRoles
	case Comment:
		possibleRoles = GerritCommentRoles
	case Patchset:
		possibleRoles = []string{"uploader"}
	case Approval:
		possibleRoles = GerritApprovalRoles
	}
	for _, possibleRole := range possibleRoles {
		_, ok := Dig(rich, []string{possibleRole + "_id"}, false, true)
		if ok {
			roles = append(roles, possibleRole)
		}
	}
	return
}

// CalculateTimeToReset - calculate time to reset rate limits based on rate limit value and rate limit reset value
func (j *DSGerrit) CalculateTimeToReset(ctx *Ctx, rateLimit, rateLimitReset int) (seconds int) {
	seconds = rateLimitReset
	return
}

// HasIdentities - does this data source support identity data
func (j *DSGerrit) HasIdentities() bool {
	return true
}
