package dads

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Ctx - environment context packed in structure
type Ctx struct {
	DS         string  // From DA_DS: ds type: for example jira, gerrit, slack etc., other env variablse will use this as a prefix
	DSPrefix   string  // uppercase(DS) + _: if DS is "slack" then prefix would be "SLACK_"
	Debug      int     // From DS_DA_DEBUG Debug level: 0-no, 1-info, 2-verbose
	ST         bool    // From DS_DA_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs      int     // From DS_DA_NCPUS, set to override number of CPUs to run, this overwrites DA_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale float64 // From DS_DA_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
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
	ctx.DSPrefix = strings.ToUpper(ctx.DS) + "_"

	// Debug
	if ctx.env("DA_DEBUG") == "" {
		ctx.Debug = 0
	} else {
		debugLevel, err := strconv.Atoi(ctx.env("DA_DEBUG"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.Debug = debugLevel
		}
	}

	// Threading
	ctx.ST = ctx.env("DA_ST") != ""
	// NCPUs
	if ctx.env("DA_NCPUS") == "" {
		ctx.NCPUs = 0
	} else {
		nCPUs, err := strconv.Atoi(ctx.env("DA_NCPUS"))
		FatalOnError(err)
		if nCPUs > 0 {
			ctx.NCPUs = nCPUs
			if ctx.NCPUs == 1 {
				ctx.ST = true
			}
		}
	}
	if ctx.env("DA_NCPUS_SCALE") == "" {
		ctx.NCPUsScale = 1.0
	} else {
		nCPUsScale, err := strconv.ParseFloat(ctx.env("DA_NCPUS_SCALE"), 64)
		FatalOnError(err)
		if nCPUsScale > 0 {
			ctx.NCPUsScale = nCPUsScale
		}
	}
}

// Print context contents
func (ctx *Ctx) Print() {
	fmt.Printf("Environment Context Dump\n%+v\n", ctx)
}
