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
	case lib.Jira:
		ds = &lib.DSJira{}
	default:
		err = fmt.Errorf("unknown data source type: " + ctx.DS)
		return
	}
	err = ds.ParseArgs()
	if err != nil {
		return
	}
	return
}

func main() {
	// args --enrich --index jira-raw --index-enrich jira -e (...) --bulk-size 500 --scroll-size 500 --db-host (...) --db-sortinghat (...) --db-user (...) --db-password (...) jira https://jira.opendaylight.org --no-archive --no-ssl-verify
	// prefix DA_DS_
	// DA_DS=jira
	// ENRICH=1 RAW_INDEX=sds-ds-raw RICH_INDEX=sds-ds-rich
	// ES_URL=... ES_BULK_SIZE=500 ES_SCROLL_SIZE=500
	// DB_HOST=... DB_NAME=... DB_USER=... DB_PASS=...
	var ctx lib.Ctx
	dtStart := time.Now()
	ctx.Init()
	lib.FatalOnError(runDS(&ctx))
	dtEnd := time.Now()
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}
