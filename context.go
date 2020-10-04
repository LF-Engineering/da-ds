package dads

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// Ctx - environment context packed in structure
type Ctx struct {
	DS                 string     // From DA_DS: ds type: for example jira, gerrit, slack etc., other env variablse will use this as a prefix
	DSPrefix           string     // uppercase(DS) + _: if DS is "slack" then prefix would be "DA_SLACK_"
	Debug              int        // From DA_DS_DEBUG Debug level: 0-no, 1-info, 2-verbose
	DebugSQL           int        // From DA_DS_DEBUG_SQL SQL Debug level
	Retry              int        // From DA_DS_RETRY: how many times retry failed operatins, default 5
	ST                 bool       // From DA_DS_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs              int        // From DA_DS_NCPUS, set to override number of CPUs to run, this overwrites DA_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale         float64    // From DA_DS_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
	Enrich             bool       // From DA_DS_ENRICH, flag to run enrichment
	RawIndex           string     // From DA_DS_RAW_INDEX - raw index name
	RichIndex          string     // From DA_DS_RICH_INDEX - rich index name
	Tag                string     // From DA_DS_TAG - tag
	ESURL              string     // From DA_DS_ES_URL - ElasticSearch URL
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
	DateFromDetected   bool
	OffsetFromDetected bool
	DB                 *sqlx.DB
	ESScrollWaitSecs   float64
}

func (ctx *Ctx) env(v string) string {
	return os.Getenv(ctx.DSPrefix + v)
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
	if ctx.env("DEBUG") == "" {
		ctx.Debug = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.env("DEBUG"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.Debug = debugLevel
		}
	}
	if ctx.env("DEBUG_SQL") == "" {
		ctx.DebugSQL = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.env("DEBUG_SQL"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.DebugSQL = debugLevel
		}
	}

	// Retry
	if ctx.env("RETRY") == "" {
		ctx.Retry = 5
	} else {
		retry, err := strconv.Atoi(ctx.env("RETRY"))
		FatalOnError(err)
		if retry != 0 {
			ctx.Retry = retry
		}
	}

	// Threading
	ctx.ST = ctx.env("ST") != ""
	// NCPUs
	if ctx.env("NCPUS") == "" {
		ctx.NCPUs = 0
	} else {
		nCPUs, err := strconv.Atoi(ctx.env("NCPUS"))
		FatalOnError(err)
		if nCPUs > 0 {
			ctx.NCPUs = nCPUs
			if ctx.NCPUs == 1 {
				ctx.ST = true
			}
		}
	}
	if ctx.env("NCPUS_SCALE") == "" {
		ctx.NCPUsScale = 1.0
	} else {
		nCPUsScale, err := strconv.ParseFloat(ctx.env("NCPUS_SCALE"), 64)
		FatalOnError(err)
		if nCPUsScale > 0 {
			ctx.NCPUsScale = nCPUsScale
		}
	}

	// Enrich
	ctx.Enrich = ctx.env("ENRICH") != ""

	// Raw & Rich index names
	ctx.RawIndex = ctx.env("RAW_INDEX")
	ctx.RichIndex = ctx.env("RICH_INDEX")

	// Tag
	ctx.Tag = ctx.env("TAG")

	// Elastic search params
	ctx.ESURL = ctx.env("ES_URL")
	if ctx.env("ES_BULK_SIZE") != "" {
		bulkSize, err := strconv.Atoi(ctx.env("ES_BULK_SIZE"))
		FatalOnError(err)
		if bulkSize > 0 {
			ctx.ESBulkSize = bulkSize
		}
	} else {
		ctx.ESBulkSize = 1000
	}
	if ctx.env("ES_SCROLL_SIZE") != "" {
		scrollSize, err := strconv.Atoi(ctx.env("ES_SCROLL_SIZE"))
		FatalOnError(err)
		if scrollSize > 0 {
			ctx.ESScrollSize = scrollSize
		}
	} else {
		ctx.ESScrollSize = 1000
	}
	ctx.ESScrollWait = ctx.env("ES_SCROLL_WAIT")
	if ctx.ESScrollWait == "" {
		ctx.ESScrollWait = "10m"
		ctx.ESScrollWaitSecs = 600.0
	} else {
		dur, err := time.ParseDuration(ctx.ESScrollWait)
		FatalOnError(err)
		ctx.ESScrollWaitSecs = dur.Seconds()
	}

	// Affiliation DB params
	ctx.DBHost = ctx.env("DB_HOST")
	ctx.DBName = ctx.env("DB_NAME")
	ctx.DBUser = ctx.env("DB_USER")
	ctx.DBPass = ctx.env("DB_PASS")
	ctx.DBPort = ctx.env("DB_PORT")
	ctx.DBOpts = ctx.env("DB_OPTS")
	ctx.DBConn = ctx.env("DB_CONN")
	if ctx.env("DB_BULK_SIZE") != "" {
		bulkSize, err := strconv.Atoi(ctx.env("DB_BULK_SIZE"))
		FatalOnError(err)
		if bulkSize > 0 {
			ctx.DBBulkSize = bulkSize
		}
	} else {
		ctx.DBBulkSize = 1000
	}

	// Affiliations re-enrich special flags
	ctx.NoRaw = ctx.env("NO_RAW") != ""
	ctx.RefreshAffs = ctx.env("REFRESH_AFFS") != ""
	ctx.OnlyIdentities = ctx.env("ONLY_IDENTITIES") != ""
	ctx.ForceFull = ctx.env("FORCE_FULL") != ""

	// Legacy UUID
	ctx.LegacyUUID = ctx.env("LEGACY_UUID") != ""

	// Project, Project slug, Category
	ctx.Project = ctx.env("PROJECT")
	ctx.ProjectSlug = ctx.env("PROJECT_SLUG")
	ctx.Category = ctx.env("CATEGORY")

	// Date from/to (optional)
	if ctx.env("DATE_FROM") != "" {
		t, err := TimeParseAny(ctx.env("DATE_FROM"))
		FatalOnError(err)
		ctx.DateFrom = &t
	}
	if ctx.env("DATE_TO") != "" {
		t, err := TimeParseAny(ctx.env("DATE_TO"))
		FatalOnError(err)
		ctx.DateTo = &t
	}

	// Offset from/to (optional)
	if ctx.env("OFFSET_FROM") == "" {
		ctx.OffsetFrom = -1.0
	} else {
		offset, err := strconv.ParseFloat(ctx.env("OFFSET_FROM"), 64)
		FatalOnError(err)
		if offset >= 0.0 {
			ctx.OffsetFrom = offset
		}
	}
	if ctx.env("OFFSET_TO") == "" {
		ctx.OffsetTo = -1.0
	} else {
		offset, err := strconv.ParseFloat(ctx.env("OFFSET_TO"), 64)
		FatalOnError(err)
		if offset >= 0.0 {
			ctx.OffsetTo = offset
		}
	}
}

// Validate - check if config is correct
func (ctx *Ctx) Validate() (err error) {
	if ctx.ESURL == "" {
		return fmt.Errorf("you must specify Elastic URL")
	}
	if strings.HasSuffix(ctx.ESURL, "/") {
		ctx.ESURL = ctx.ESURL[:len(ctx.ESURL)-1]
	}
	if !ctx.NoRaw && ctx.RawIndex == "" {
		return fmt.Errorf("you must specify raw index name unless skipping raw processing")
	}
	if ctx.Enrich && ctx.RichIndex == "" {
		return fmt.Errorf("you must specify rich index name unless skipping enrichment")
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
