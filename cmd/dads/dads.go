package main

import (
	"fmt"
	"math/rand"
	"time"

	lib "github.com/LF-Engineering/da-ds"
)

func runDS(ctx *lib.Ctx) (err error) {
	var ds lib.DS
	switch ctx.DS {
	case lib.Stub:
		ds = &lib.DSStub{}
	case lib.Jira:
		ds = &lib.DSJira{}
	case lib.Groupsio:
		ds = &lib.DSGroupsio{}
	default:
		err = fmt.Errorf("unknown data source type: " + ctx.DS)
		return
	}
	err = ds.ParseArgs(ctx)
	if err != nil {
		lib.Printf("%s: ParseArgs(%s) error: %v\n", ds.Info(), ctx.Info(), err)
		return
	}
	err = ds.Validate()
	if err != nil {
		lib.Printf("%s: Validate error: %v\n", ds.Info(), err)
		return
	}
	if !ctx.NoRaw {
		err = lib.FetchRaw(ctx, ds)
		if err != nil {
			lib.Printf("%s: FetchRaw(%s) error: %v\n", ds.Info(), ctx.Info(), err)
			return
		}
	}
	if ctx.Enrich {
		err = lib.Enrich(ctx, ds)
		if err != nil {
			lib.Printf("%s: Enrich(%s) error: %v\n", ds.Info(), ctx.Info(), err)
			return
		}
	}
	return
}

func main() {
	// prefix DA_DS_
	// DA_DS=jira
	// NO_RAW=1 REFRESH_AFFS=1 FORCE_FULL=1
	// ENRICH=1 RAW_INDEX=sds-ds-raw RICH_INDEX=sds-ds-rich
	// ES_URL=... ES_BULK_SIZE=500 ES_SCROLL_SIZE=500
	// DB_HOST=... DB_NAME=... DB_USER=... DB_PASS=...
	// DA_JIRA_URL=... DA_JIRA_NO_SSL_VERIFY=1
	var ctx lib.Ctx
	rand.Seed(time.Now().UnixNano())
	dtStart := time.Now()
	ctx.Init()
	defer lib.CacheSummary(&ctx)
	lib.FatalOnError(ctx.Validate())
	lib.CreateESCache(&ctx)
	lib.FatalOnError(runDS(&ctx))
	dtEnd := time.Now()
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}
