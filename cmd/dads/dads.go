package main

import (
	"encoding/json"
	"fmt"
	"github.com/LF-Engineering/da-ds/dockerhub"
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
	case dockerhub.Dockerhub:
		manager, err := dockerhubEnvs(ctx)
		if err != nil {
			return err
		}
		return manager.Sync()
	case lib.Git:
		ds = &lib.DSGit{}
	case lib.Gerrit:
		ds = &lib.DSGerrit{}
	case lib.Confluence:
		ds = &lib.DSConfluence{}
	case lib.Rocketchat:
		ds = &lib.DSRocketchat{}
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
	_ = lib.GetThreadsNum(ctx)
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
	var ctx lib.Ctx

	rand.Seed(time.Now().UnixNano())
	dtStart := time.Now()
	ctx.Init()
	lib.FatalOnError(ctx.Validate())
	lib.CreateESCache(&ctx)
	lib.FatalOnError(runDS(&ctx))
	dtEnd := time.Now()
	lib.CacheSummary(&ctx)
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}

func dockerhubEnvs(ctx *lib.Ctx) (*dockerhub.Manager, error) {
	// Dockerhub credentials
	username := ctx.Env("USERNAME")
	password := ctx.Env("PASSWORD")
	fetcherBackendVersion := "0.0.1"  //ctx.Env("FETCHER_BACKEND_VERSION")
	enricherBackendVersion := "0.0.1" //ctx.Env("ENRICHER_BACKEND_VERSION")
	esUrl := ctx.ESURL
	httpTimeout := ctx.Env("HTTP_TIMEOUT") // "60s" 60 seconds...
	// flag projects json array
	repositoriesJson := ctx.Env("REPOSITORIES_JSON")
	enrichOnly := ctx.NoRaw
	enrich := ctx.Enrich
	fromDate := ctx.DateFrom
	noIncremental := ctx.BoolEnv("NO_INCREMENTAL")

	var repositories []*dockerhub.Repository
	if err := json.Unmarshal([]byte(repositoriesJson), &repositories); err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}

	return dockerhub.NewManager(username, password, fetcherBackendVersion, enricherBackendVersion,
		enrichOnly, enrich, esUrl, timeout, repositories, fromDate, noIncremental), nil
}
