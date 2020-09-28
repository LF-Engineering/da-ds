package dads

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Ctx - environment context packed in structure
type Ctx struct {
	DS           string  // From DA_DS: ds type: for example jira, gerrit, slack etc., other env variablse will use this as a prefix
	DSPrefix     string  // uppercase(DS) + _: if DS is "slack" then prefix would be "DA_SLACK_"
	Debug        int     // From DA_DS_DEBUG Debug level: 0-no, 1-info, 2-verbose
	ST           bool    // From DA_DS_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs        int     // From DA_DS_NCPUS, set to override number of CPUs to run, this overwrites DA_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale   float64 // From DA_DS_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
	Enrich       bool    // From DA_DS_ENRICH, flag to run enrichment
	RawIndex     string  // From DA_DS_RAW_INDEX - raw index name
	RichIndex    string  // From DA_DS_RICH_INDEX - rich index name
	ESURL        string  // From DA_DS_ES_URL - ElasticSearch URL
	ESBulkSize   int     // From DA_DS_ES_BULK_SIZE - ElasticSearch bulk size
	ESScrollSize int     // From DA_DS_ES_SCROLL_SIZE - ElasticSearch scroll size
	ESScrollWait string  // From DA_DS_ES_SCROLL_WAIT - ElasticSearch scroll wait
	DBHost       string  // From DA_DS_DB_HOST - affiliation DB host
	DBName       string  // From DA_DS_DB_NAME - affiliation DB name
	DBUser       string  // From DA_DS_DB_USER - affiliation DB user
	DBPass       string  // From DA_DS_DB_PASS - affiliation DB pass
	NoRaw        bool    // From DA_DS_NO_RAW - do only the enrichment
	RefreshAffs  bool    // From DA_DS_REFRESH_AFFS - refresh affiliation data
	ForceFull    bool    // From DA_DS_FORCE_FULL - force runnign full data source, do not attempt to detect where to start from
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

	// Elastic search params
	ctx.ESURL = ctx.env("ES_URL")
	if ctx.env("ES_BULK_SIZE") != "" {
		bulkSize, err := strconv.Atoi(ctx.env("ES_BULK_SIZE"))
		FatalOnError(err)
		if bulkSize > 0 {
			ctx.ESBulkSize = bulkSize
		}
	}
	if ctx.env("ES_SCROLL_SIZE") != "" {
		scrollSize, err := strconv.Atoi(ctx.env("ES_SCROLL_SIZE"))
		FatalOnError(err)
		if scrollSize > 0 {
			ctx.ESScrollSize = scrollSize
		}
	}
	ctx.ESScrollWait = ctx.env("ES_SCROLL_WAIT")

	// Affiliation DB params
	ctx.DBHost = ctx.env("DB_HOST")
	ctx.DBName = ctx.env("DB_NAME")
	ctx.DBUser = ctx.env("DB_USER")
	ctx.DBPass = ctx.env("DB_PASS")

	// Affiliations re-enrich special flags
	ctx.NoRaw = ctx.env("NO_RAW") != ""
	ctx.RefreshAffs = ctx.env("REFRESH_AFFS") != ""
	ctx.ForceFull = ctx.env("FORCE_FULL") != ""
}

// Print context contents
func (ctx *Ctx) Print() {
	fmt.Printf("Environment Context Dump\n%+v\n", ctx)
}

// Info - return context in human readable form
func (ctx Ctx) Info() string {
	return fmt.Sprintf("%+v", ctx)
}
