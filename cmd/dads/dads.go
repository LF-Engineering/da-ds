package main

import (
	"fmt"
	"time"

	lib "github.com/LF-Engineering/da-ds"
	// jsoniter "github.com/json-iterator/go"
	// yaml "gopkg.in/yaml.v2"
)

func runDS(ctx *lib.Ctx) (err error) {
	var ds lib.DS
	switch ctx.DS {
	case lib.Stub:
		ds = &lib.DSStub{}
	case lib.Jira:
		ds = &lib.DSJira{}
	default:
		err = fmt.Errorf("unknown data source type: " + ctx.DS)
		return
	}
	err = ds.ParseArgs(ctx)
	if err != nil {
		return
	}
	var lastDataDt *time.Time
	if !ctx.NoRaw {
		lastDataDt, err = ds.FetchRaw(ctx)
		if err != nil {
			lib.Printf("%s: FetchRaw(%s) error: %v, allowing continue\n", ds.Info(), ctx.Info(), err)
		}
	}
	if ctx.Enrich {
		err = ds.Enrich(ctx, lastDataDt)
		if err != nil {
			lib.Printf("%s: Enrich(%s,%v) error: %v, allowing continue\n", ds.Info(), ctx.Info(), lastDataDt, err)
		}
	}
	return
}

func main() {
	// args --only-enrich --refresh-identities --no_incremental --enrich --index jira-raw --index-enrich jira -e (...) --bulk-size 500 --scroll-size 500 --db-host (...) --db-sortinghat (...) --db-user (...) --db-password (...) jira https://jira.opendaylight.org --no-archive --no-ssl-verify
	// prefix DA_DS_
	// DA_DS=jira
	// NO_RAW=1 REFRESH_AFFS=1 FORCE_FULL=1
	// ENRICH=1 RAW_INDEX=sds-ds-raw RICH_INDEX=sds-ds-rich
	// ES_URL=... ES_BULK_SIZE=500 ES_SCROLL_SIZE=500
	// DB_HOST=... DB_NAME=... DB_USER=... DB_PASS=...
	// DA_JIRA_URL=... DA_JIRA_NO_SSL_VERIFY=1
	var ctx lib.Ctx
	dtStart := time.Now()
	ctx.Init()
	lib.FatalOnError(runDS(&ctx))
	dtEnd := time.Now()
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}
