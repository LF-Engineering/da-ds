package main

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/jenkins"
	"math/rand"
	"time"

	"github.com/LF-Engineering/da-ds/bugzilla"

	jsoniter "github.com/json-iterator/go"

	"github.com/LF-Engineering/da-ds/dockerhub"

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
		manager, err := buildDockerhubManager(ctx)
		if err != nil {
			return err
		}
		return manager.Sync()
	case jenkins.Jenkins:
		manager, err := buildJenkinsManager(ctx)
		if err != nil {
			return
    }
    return manager.Sync()
	case bugzilla.Bugzilla:
		manager, err := buildBugzillaManager(ctx)
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
	ctx.ParseFlags()
	lib.FatalOnError(ctx.Validate())
	lib.CreateESCache(&ctx)
	lib.FatalOnError(runDS(&ctx))
	dtEnd := time.Now()
	lib.CacheSummary(&ctx)
	lib.Printf("Took: %v\n", dtEnd.Sub(dtStart))
}

func buildDockerhubManager(ctx *lib.Ctx) (*dockerhub.Manager, error) {
	// Dockerhub credentials
	username := ctx.Env("USERNAME")
	password := ctx.Env("PASSWORD")
	fetcherBackendVersion := "0.0.1"  //ctx.Env("FETCHER_BACKEND_VERSION")
	enricherBackendVersion := "0.0.1" //ctx.Env("ENRICHER_BACKEND_VERSION")
	esURL := ctx.ESURL
	httpTimeout := ctx.Env("HTTP_TIMEOUT") // "60s" 60 seconds...
	repositoriesJSON := ctx.Env("REPOSITORIES_JSON")
	enrichOnly := ctx.NoRaw
	enrich := ctx.Enrich
	fromDate := ctx.DateFrom
	noIncremental := ctx.BoolEnv("NO_INCREMENTAL")

	var repositories []*dockerhub.Repository
	if err := jsoniter.Unmarshal([]byte(repositoriesJSON), &repositories); err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}

	return dockerhub.NewManager(username, password, fetcherBackendVersion, enricherBackendVersion,
		enrichOnly, enrich, esURL, timeout, repositories, fromDate, noIncremental), nil
}


func buildJenkinsManager(ctx *lib.Ctx) (*jenkins.Manager, error) {
	fetcherBackendVersion := "0.0.1"
	enricherBackendVersion := "0.0.1" 
	noIncremental := ctx.BoolEnv("NO_INCREMENTAL")
	httpTimeout := ctx.Env("HTTP_TIMEOUT") // "60s" 60 seconds...
	//example jenkinsJSON = `[{"username": "user", "password": "Admin123", "url":"https://jenkins.soramitsu.co.jp/job/iroha/job/iroha-hyperledger","project":"Iroha","index":"sds-hyperledger-iroha"}]`
	jenkinsJSON := ctx.Env("JENKINS_JSON")
	esURL := ctx.ESURL
	enrichOnly := ctx.NoRaw
	enrich := ctx.Enrich
	fromDate := ctx.DateFrom
	var buildServers []*jenkins.BuildServer
	if err := jsoniter.Unmarshal([]byte(jenkinsJSON), &buildServers); err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}
	return jenkins.NewManager(fetcherBackendVersion, enricherBackendVersion,
		enrichOnly, enrich, esURL, timeout, buildServers, fromDate, noIncremental), nil
}

func buildBugzillaManager(ctx *lib.Ctx) (*bugzilla.Manager, error) {

	origin := ctx.BugZilla.Origin.String()
	fetcherBackendVersion := "0.1.0"
	enricherBackendVersion := "0.1.0"
	doFetch := ctx.BugZilla.DoFetch.Bool()
	doEnrich := ctx.BugZilla.DoEnrich.Bool()
	fromDate := ctx.BugZilla.FromDate.Date()
	fetchSize := ctx.BugZilla.FetchSize.Int()
	enrichSize := ctx.BugZilla.EnrichSize.Int()
	project := ctx.BugZilla.Project.String()
	esIndex := ctx.BugZilla.EsIndex.String()
	mgr, err := bugzilla.NewManager(origin, ctx.DBConn, fetcherBackendVersion, enricherBackendVersion,
		doFetch, doEnrich, ctx.ESURL, "", "", esIndex, fromDate, project,
		fetchSize, enrichSize)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}
