package dads

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	jsoniter "github.com/json-iterator/go"
)

// Ctx - environment context packed in structure
type Ctx struct {
	DS                 string     // From DA_DS: ds type: for example jira, gerrit, slack etc., other env variablse will use this as a prefix
	DSPrefix           string     // uppercase(DS) + _: if DS is "slack" then prefix would be "DA_SLACK_"
	Debug              int        // From DA_DS_DEBUG Debug level: 0-no, 1-info, 2-verbose
	DebugSQL           int        // From DA_DS_DEBUG_SQL SQL Debug level
	Retry              int        // From DA_DS_RETRY: how many times retry failed operatins, default 5
	ST                 bool       // From DA_DS_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs              int        // From DA_DS_NCPUS, set to override number of CPUs to run, this overwrites DA_DS_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale         float64    // From DA_DS_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
	Enrich             bool       // From DA_DS_ENRICH, flag to run enrichment
	RawIndex           string     // From DA_DS_RAW_INDEX - raw index name
	RichIndex          string     // From DA_DS_RICH_INDEX - rich index name
	Tag                string     // From DA_DS_TAG - tag
	ESURL              string     // From DA_DS_ES_URL - ElasticSearch URL
	AffiliationAPIURL  string     // From DA_DS_AFFILIATION_API_URL - Affiliation API URL
	ESBulkSize         int        // From DA_DS_ES_BULK_SIZE - ElasticSearch bulk size
	ESScrollSize       int        // From DA_DS_ES_SCROLL_SIZE - ElasticSearch scroll size
	ESScrollWait       string     // From DA_DS_ES_SCROLL_WAIT - ElasticSearch scroll wait
	DBBulkSize         int        // From DA_DS_DB_BULK_SIZE - affiliations DB bulk size
	DBHost             string     // From DA_DS_DB_HOST - affiliation DB host
	DBName             string     // From DA_DS_DB_NAME - affiliation DB name
	DBUser             string     // From DA_DS_DB_USER - affiliation DB user
	DBPass             string     // From DA_DS_DB_PASS - affiliation DB pass
	DBPort             string     // From DA_DS_DB_PORT - affiliation DB port
	DBOpts             string     // From DA_DS_DB_OPTS - affiliation DB & separated iURL encoded options, for example "charset=utf8&parseTime=true"
	DBConn             string     // From DA_DS_DB_CONN - affiliation DB conn (full connection string - if set no other DB params will be used)
	NoRaw              bool       // From DA_DS_NO_RAW - do only the enrichment
	NoIdentities       bool       // From DA_DS_NO_IDENTITIES - do not upload identities to affiliations database (if you want to perform enrichment - only use this when you did that at least once)
	NoCache            bool       // From DA_DS_NO_CACHE - do not use L2(mem, ES) cache for selected requests
	DryRun             bool       // From DA_DS_DRY_RUN - do only requests that read data, no write to anything (excluding cache - this one can be written in dry-run mode - still can be disabled with NoCache)
	RefreshAffs        bool       // From DA_DS_REFRESH_AFFS - refresh affiliation data
	OnlyIdentities     bool       // From DA_DS_ONLY_IDENTITIES - only add identities to affiliation database
	ForceFull          bool       // From DA_DS_FORCE_FULL - force running full data source enrichment, do not attempt to detect where to start from
	Project            string     // From DA_DS_PROJECT - set project can be for example "ONAP"
	ProjectSlug        string     // From DA_DS_PROJECT_SLUG - set project slug - fixture slug, for example "lfn/onap"
	Category           string     // From DA_DS_CATEGORY - set category (some DS support this), for example "issue" (github/issue, github/pull_request etc.)
	DateFrom           *time.Time // From DA_DS_DATE_FROM
	DateTo             *time.Time // From DA_DS_DATE_TO
	OffsetFrom         float64    // From DA_DS_OFFSET_FROM
	OffsetTo           float64    // From DA_DS_OFFSET_TO
	LegacyUUID         bool       // From DA_DS_LEGACY_UUID - use python code for generating uuids
	AllowFail          int        // From DA_DS_ALLOW_FAIL - allow fail uploading single documents to elastic: 0 - send to GAP handler and continue, 1 - don't allow, 2-allow fail, if failed, skip entire pack (ignore), 3-allow fail, but each next document without retries, else-allow fail and retry each individual document
	DateFromDetected   bool
	OffsetFromDetected bool
	DB                 *sqlx.DB
	ESScrollWaitSecs   float64
	GapURL             string
	Retries            uint
	Delay              time.Duration
	Repository         []Repository
	AffAPI             string
	SlackWebHookURL    string
	// Bugzilla contains all bugzilla params
	BugZilla *BugZilla

	PiperMail *PiperMail

	GoogleGroups *GoogleGroups
}

// Repository dockerhub repository data
type Repository struct {
	Owner      string
	Repository string
	Project    string
	ESIndex    string
}

// BugZilla parameter context contains all required parameters to run Bugzilla fetch and enrich
type BugZilla struct {
	Origin      *Flag
	EsIndex     *Flag
	FromDate    *Flag
	Project     *Flag
	DoFetch     *Flag
	DoEnrich    *Flag
	FetchSize   *Flag
	EnrichSize  *Flag
	ProjectSlug *Flag
}

// PiperMail parameter context contains all required parameters to run Piper mail fetch and enrich
type PiperMail struct {
	Origin      *Flag
	Project     *Flag
	ProjectSlug *Flag
	GroupName   *Flag
	EsIndex     *Flag
	FromDate    *Flag
	DoFetch     *Flag
	DoEnrich    *Flag
	FetchSize   *Flag
	EnrichSize  *Flag
}

// GoogleGroups parameter context contains all required parameters to run google groups fetch and enrich
type GoogleGroups struct {
	Origin      *Flag
	Project     *Flag
	ProjectSlug *Flag
	GroupName   *Flag
	EsIndex     *Flag
	FromDate    *Flag
	DoFetch     *Flag
	DoEnrich    *Flag
	FetchSize   *Flag
	EnrichSize  *Flag
}

// Env - get env value using current DS prefix
func (ctx *Ctx) Env(v string) string {
	return os.Getenv(ctx.DSPrefix + v)
}

// ParseFlags declare and parse CLI flags
func (ctx *Ctx) ParseFlags() {
	flag.Var(ctx.BugZilla.Origin, "bugzilla-origin", "Bugzilla origin url")
	flag.Var(ctx.BugZilla.EsIndex, "bugzilla-es-index", "Bugzilla es index base name")
	flag.Var(ctx.BugZilla.FromDate, "bugzilla-from-date", "Optional, date to start syncing from")
	flag.Var(ctx.BugZilla.Project, "bugzilla-project", "Slug name of a project e.g. yocto")
	flag.Var(ctx.BugZilla.DoFetch, "bugzilla-do-fetch", "To decide whether will fetch raw data or not")
	flag.Var(ctx.BugZilla.DoEnrich, "bugzilla-do-enrich", "To decide whether will do enrich raw data or not.")
	flag.Var(ctx.BugZilla.FetchSize, "bugzilla-fetch-size", "Total number of fetched items per request.")
	flag.Var(ctx.BugZilla.EnrichSize, "bugzilla-enrich-size", "Total number of enriched items per request.")
	flag.Var(ctx.PiperMail.ProjectSlug, "bugzilla-slug", "Bugzilla project slug")

	flag.Var(ctx.PiperMail.Origin, "pipermail-origin", "Pipermail origin url")
	flag.Var(ctx.PiperMail.ProjectSlug, "pipermail-slug", "Pipermail project slug")
	flag.Var(ctx.PiperMail.GroupName, "pipermail-groupname", "Pipermail group name")
	flag.Var(ctx.PiperMail.EsIndex, "pipermail-es-index", "Pipermail es index base name")
	flag.Var(ctx.PiperMail.FromDate, "pipermail-from-date", "Optional, date to start syncing from")
	flag.Var(ctx.PiperMail.Project, "pipermail-project", "Slug name of a project e.g. yocto")
	flag.Var(ctx.PiperMail.DoFetch, "pipermail-do-fetch", "To decide whether will fetch raw data or not")
	flag.Var(ctx.PiperMail.DoEnrich, "pipermail-do-enrich", "To decide whether will do enrich raw data or not.")
	flag.Var(ctx.PiperMail.FetchSize, "pipermail-fetch-size", "Total number of fetched items per request.")
	flag.Var(ctx.PiperMail.EnrichSize, "pipermail-enrich-size", "Total number of enriched items per request.")

	flag.Var(ctx.GoogleGroups.Origin, "googlegroups-origin", "GoogleGroups origin url")
	flag.Var(ctx.GoogleGroups.ProjectSlug, "googlegroups-slug", "GoogleGroups project slug")
	flag.Var(ctx.GoogleGroups.GroupName, "googlegroups-groupname", "GoogleGroups group name")
	flag.Var(ctx.GoogleGroups.EsIndex, "googlegroups-es-index", "GoogleGroups es index base name")
	flag.Var(ctx.GoogleGroups.FromDate, "googlegroups-from-date", "Optional, date to start syncing from")
	flag.Var(ctx.GoogleGroups.Project, "googlegroups-project", "Slug name of a project e.g. yocto")
	flag.Var(ctx.GoogleGroups.DoFetch, "googlegroups-do-fetch", "To decide whether will fetch raw data or not")
	flag.Var(ctx.GoogleGroups.DoEnrich, "googlegroups-do-enrich", "To decide whether will do enrich raw data or not.")
	flag.Var(ctx.GoogleGroups.FetchSize, "googlegroups-fetch-size", "Total number of fetched items per request.")
	flag.Var(ctx.GoogleGroups.EnrichSize, "googlegroups-enrich-size", "Total number of enriched items per request.")

	flag.Parse()
}

// BoolEnv - parses env variable as bool
// returns false for anything that was parsed as false, zero, empty etc:
// f, F, false, False, fALSe, 0, "", 0.00
// else returns true
func (ctx *Ctx) BoolEnv(k string) bool {
	v := os.Getenv(ctx.DSPrefix + k)
	return StringToBool(v)
}

// Init - get context from environment variables
func (ctx *Ctx) Init() {
	// DS
	ctx.DS = os.Getenv("DA_DS")
	if ctx.DS == "" {
		Fatalf("DA_DS environment must be set")
		return
	}
	ctx.DSPrefix = "DA_" + strings.ToUpper(ctx.DS) + "_"

	// Debug
	if !ctx.BoolEnv("DEBUG") {
		ctx.Debug = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.Env("DEBUG"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.Debug = debugLevel
		}
	}
	if !ctx.BoolEnv("DEBUG_SQL") {
		ctx.DebugSQL = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.Env("DEBUG_SQL"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.DebugSQL = debugLevel
		}
	}

	// Retry
	if !ctx.BoolEnv("RETRY") {
		ctx.Retry = 5
	} else {
		retry, err := strconv.Atoi(ctx.Env("RETRY"))
		FatalOnError(err)
		if retry != 0 {
			ctx.Retry = retry
		}
	}

	// Threading
	ctx.ST = ctx.BoolEnv("ST")
	// NCPUs
	if !ctx.BoolEnv("NCPUS") {
		ctx.NCPUs = 0
	} else {
		nCPUs, err := strconv.Atoi(ctx.Env("NCPUS"))
		FatalOnError(err)
		if nCPUs > 0 {
			ctx.NCPUs = nCPUs
			if ctx.NCPUs == 1 {
				ctx.ST = true
			}
		}
	}
	if !ctx.BoolEnv("NCPUS_SCALE") {
		ctx.NCPUsScale = 1.0
	} else {
		nCPUsScale, err := strconv.ParseFloat(ctx.Env("NCPUS_SCALE"), 64)
		FatalOnError(err)
		if nCPUsScale > 0 {
			ctx.NCPUsScale = nCPUsScale
		}
	}

	// Enrich
	ctx.Enrich = ctx.BoolEnv("ENRICH")

	// Raw & Rich index names
	ctx.RawIndex = ctx.Env("RAW_INDEX")
	ctx.RichIndex = ctx.Env("RICH_INDEX")

	// Tag
	ctx.Tag = ctx.Env("TAG")

	// Elastic search params
	ctx.ESURL = ctx.Env("ES_URL")
	if ctx.Env("ES_BULK_SIZE") != "" {
		bulkSize, err := strconv.Atoi(ctx.Env("ES_BULK_SIZE"))
		FatalOnError(err)
		if bulkSize > 0 {
			ctx.ESBulkSize = bulkSize
		}
	} else {
		ctx.ESBulkSize = 1000
	}
	if ctx.Env("ES_SCROLL_SIZE") != "" {
		scrollSize, err := strconv.Atoi(ctx.Env("ES_SCROLL_SIZE"))
		FatalOnError(err)
		if scrollSize > 0 {
			ctx.ESScrollSize = scrollSize
		}
	} else {
		ctx.ESScrollSize = 1000
	}
	ctx.ESScrollWait = ctx.Env("ES_SCROLL_WAIT")
	if ctx.ESScrollWait == "" {
		ctx.ESScrollWait = "10m"
		ctx.ESScrollWaitSecs = 600.0
	} else {
		dur, err := time.ParseDuration(ctx.ESScrollWait)
		FatalOnError(err)
		ctx.ESScrollWaitSecs = dur.Seconds()
	}

	if ctx.Env("GAP_URL") != "" {
		ctx.GapURL = ctx.Env("GAP_URL")
	}
	if ctx.Env("RETRIES") != "" {
		r, _ := strconv.ParseUint(ctx.Env("RETRIES"), 10, 2)
		ctx.Retries = uint(r)
	}
	if ctx.Env("DELAY") != "" {
		delay, _ := time.ParseDuration(ctx.Env("DELAY"))
		ctx.Delay = delay
	}
	ctx.SlackWebHookURL = ctx.Env("SLACK_WEBHOOK_URL")

	// Affiliation API URL
	ctx.AffiliationAPIURL = ctx.Env("AFFILIATION_API_URL")

	if ctx.Env("REPOSITORIES_JSON") != "" {
		var repo []Repository
		b := []byte(ctx.Env("REPOSITORIES_JSON"))
		err := jsoniter.Unmarshal(b, &repo)
		if err != nil {
			Fatalf("unmarshaling dockerhub repositories failed")
		}

		ctx.Repository = repo
	}

	// Affiliation DB params
	ctx.DBHost = ctx.Env("DB_HOST")
	ctx.DBName = ctx.Env("DB_NAME")
	ctx.DBUser = ctx.Env("DB_USER")
	ctx.DBPass = ctx.Env("DB_PASS")
	ctx.DBPort = ctx.Env("DB_PORT")
	ctx.DBOpts = ctx.Env("DB_OPTS")
	ctx.DBConn = ctx.Env("DB_CONN")
	if ctx.Env("DB_BULK_SIZE") != "" {
		bulkSize, err := strconv.Atoi(ctx.Env("DB_BULK_SIZE"))
		FatalOnError(err)
		if bulkSize > 0 {
			ctx.DBBulkSize = bulkSize
		}
	} else {
		ctx.DBBulkSize = 1000
	}

	// Affiliations re-enrich special flags
	ctx.NoRaw = ctx.BoolEnv("NO_RAW")
	ctx.RefreshAffs = ctx.BoolEnv("REFRESH_AFFS")
	ctx.OnlyIdentities = ctx.BoolEnv("ONLY_IDENTITIES")
	ctx.ForceFull = ctx.BoolEnv("FORCE_FULL")
	ctx.NoIdentities = ctx.BoolEnv("NO_IDENTITIES")

	// No cache & dry-run modes
	ctx.NoCache = ctx.BoolEnv("NO_CACHE")
	ctx.DryRun = ctx.BoolEnv("DRY_RUN")

	// Legacy UUID
	ctx.LegacyUUID = ctx.BoolEnv("LEGACY_UUID")

	// Allow fail
	if ctx.BoolEnv("ALLOW_FAIL") {
		allowFail, err := strconv.Atoi(ctx.Env("ALLOW_FAIL"))
		FatalOnError(err)
		if allowFail != 0 {
			ctx.AllowFail = allowFail
		}
	}

	// Project, Project slug, Category
	ctx.Project = ctx.Env("PROJECT")
	ctx.ProjectSlug = ctx.Env("PROJECT_SLUG")
	if ctx.ProjectSlug == "" {
		ctx.ProjectSlug = os.Getenv("PROJECT_SLUG")
	}
	ctx.Category = ctx.Env("CATEGORY")

	// Date from/to (optional)
	if ctx.Env("DATE_FROM") != "" {
		t, err := TimeParseAny(ctx.Env("DATE_FROM"))
		FatalOnError(err)
		ctx.DateFrom = &t
	}
	if ctx.Env("DATE_TO") != "" {
		t, err := TimeParseAny(ctx.Env("DATE_TO"))
		FatalOnError(err)
		ctx.DateTo = &t
	}

	// Offset from/to (optional)
	if ctx.Env("OFFSET_FROM") == "" {
		ctx.OffsetFrom = -1.0
	} else {
		offset, err := strconv.ParseFloat(ctx.Env("OFFSET_FROM"), 64)
		FatalOnError(err)
		if offset >= 0.0 {
			ctx.OffsetFrom = offset
		}
	}
	if ctx.Env("OFFSET_TO") == "" {
		ctx.OffsetTo = -1.0
	} else {
		offset, err := strconv.ParseFloat(ctx.Env("OFFSET_TO"), 64)
		FatalOnError(err)
		if offset >= 0.0 {
			ctx.OffsetTo = offset
		}
	}
	ctx.BugZilla = &BugZilla{
		Origin:     NewFlag(),
		EsIndex:    NewFlag(),
		DoFetch:    NewFlag(),
		DoEnrich:   NewFlag(),
		FetchSize:  NewFlag(),
		EnrichSize: NewFlag(),
		Project:    NewFlag(),
	}

	ctx.PiperMail = &PiperMail{
		Origin:      NewFlag(),
		Project:     NewFlag(),
		ProjectSlug: NewFlag(),
		GroupName:   NewFlag(),
		EsIndex:     NewFlag(),
		FromDate:    NewFlag(),
		DoFetch:     NewFlag(),
		DoEnrich:    NewFlag(),
		FetchSize:   NewFlag(),
		EnrichSize:  NewFlag(),
	}

	// Redacted data
	AddRedacted(ctx.ESURL, false)
	AddRedacted(ctx.DBHost, false)
	AddRedacted(ctx.DBName, false)
	AddRedacted(ctx.DBUser, false)
	AddRedacted(ctx.DBPass, false)
	AddRedacted(ctx.DBConn, false)
	AddRedacted(ctx.GapURL, false)
	AddRedacted(ctx.AffiliationAPIURL, false)
}

// Validate - check if config is correct
func (ctx *Ctx) Validate() (err error) {
	if ctx.ESURL == "" {
		return fmt.Errorf("you must specify Elastic URL")
	}
	if strings.HasSuffix(ctx.ESURL, "/") {
		ctx.ESURL = ctx.ESURL[:len(ctx.ESURL)-1]
	}
	return
}

// Print context contents
func (ctx *Ctx) Print() {
	fmt.Printf("Environment Context Dump\n%+v\n", ctx)
}

// Info - return context in human readable form
func (ctx Ctx) Info() string {
	return fmt.Sprintf("%+v", ctx)
}

// AffsDBConfigured - is affiliations DB configured?
func (ctx *Ctx) AffsDBConfigured() bool {
	return ctx.DBHost != "" || ctx.DBName != "" || ctx.DBUser != "" || ctx.DBPass != "" || ctx.DBPort != "" || ctx.DBOpts != "" || ctx.DBConn != ""
}
