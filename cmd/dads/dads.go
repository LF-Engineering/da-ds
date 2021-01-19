package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/LF-Engineering/da-ds/bugzillarest"

	"github.com/LF-Engineering/da-ds/jenkins"
	"github.com/LF-Engineering/da-ds/pipermail"

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
			return err
		}
		return manager.Sync()
	case bugzilla.Bugzilla:
		manager, err := buildBugzillaManager(ctx)
		if err != nil {
			return err
		}
		return manager.Sync()
	case bugzillarest.BugzillaRest:
		manager, err := buildBugzillaRestManager(ctx)
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
	case pipermail.Pipermail:
		manager, err := buildPipermailManager(ctx)
		if err != nil {
			fmt.Println(err)
			return err
		}
		return manager.Sync()
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
	retries := uint(ctx.Retry)
	delay := ctx.Delay
	gapURL := ctx.GapURL

	var repositories []*dockerhub.Repository
	if err := jsoniter.Unmarshal([]byte(repositoriesJSON), &repositories); err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}
	return dockerhub.NewManager(username, password, fetcherBackendVersion, enricherBackendVersion,
		enrichOnly, enrich, esURL, timeout, repositories, fromDate, noIncremental, retries, delay, gapURL), nil
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
	bulkSize := ctx.ESBulkSize
	if bulkSize == 0 {
		bulkSize = 1000
	}
	var buildServers []*jenkins.BuildServer
	if err := jsoniter.Unmarshal([]byte(jenkinsJSON), &buildServers); err != nil {
		return nil, err
	}
	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}
	return jenkins.NewManager(fetcherBackendVersion, enricherBackendVersion,
		enrichOnly, enrich, esURL, timeout, buildServers, fromDate, noIncremental, bulkSize), nil
}

func buildBugzillaManager(ctx *lib.Ctx) (*bugzilla.Manager, error) {
	var params bugzilla.Param
	params.EndPoint = ctx.BugZilla.Origin.String()
	params.ShConnStr = fmt.Sprintf("%s:%s@tcp(%s)/%s", ctx.DBUser, ctx.DBPass, ctx.DBHost, ctx.DBName)
	params.FetcherBackendVersion = "0.1.0"
	params.EnricherBackendVersion = "0.1.0"
	params.ESUrl = ctx.ESURL
	params.EsUser = ""
	params.EsPassword = ""
	params.Fetch = ctx.BugZilla.DoFetch.Bool()
	params.Enrich = ctx.BugZilla.DoEnrich.Bool()
	params.FromDate = ctx.BugZilla.FromDate.Date()
	params.FetchSize = ctx.BugZilla.FetchSize.Int()
	params.EnrichSize = ctx.BugZilla.EnrichSize.Int()
	params.Project = ctx.BugZilla.Project.String()
	params.EsIndex = ctx.RichIndex

	params.Retries = uint(3)
	if ctx.Retry != 0 {
		params.Retries = uint(ctx.Retry)
	}

	params.Delay = 2 * time.Second
	if ctx.Delay != 0*time.Second {
		params.Delay = ctx.Delay

	}

	params.GapURL = ctx.GapURL

	params.AffBaseURL = ctx.Env("AFFILIATIONS_API_BASE_URL")
	params.ESCacheURL = ctx.Env("ES_CACHE_URL")
	params.ESCacheUsername = ctx.Env("ES_CACHE_USERNAME")
	params.ESCachePassword = ctx.Env("ES_CACHE_PASSWORD")
	params.AuthGrantType = ctx.Env("AUTH0_GRANT_TYPE")

	params.AuthClientID = ctx.Env("AUTH0_CLIENT_ID")
	params.AuthClientSecret = ctx.Env("AUTH0_CLIENT_SECRET")
	params.AuthAudience = ctx.Env("AUTH0_AUDIENCE")

	params.AuthURL = ctx.Env("AUTH0_BASE_URL")
	params.Environment = ctx.Env("ENVIRONMENT")

	mgr, err := bugzilla.NewManager(params)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func buildPipermailManager(ctx *lib.Ctx) (*pipermail.Manager, error) {
	origin := ctx.PiperMail.Origin.String()
	slug := ctx.PiperMail.ProjectSlug.String()
	groupName := ctx.PiperMail.GroupName.String()
	fetcherBackendVersion := "0.0.1"
	enricherBackendVersion := "0.0.1"
	doFetch := ctx.PiperMail.DoFetch.Bool()
	doEnrich := ctx.PiperMail.DoEnrich.Bool()
	fromDate := ctx.PiperMail.FromDate.Date()
	fetchSize := ctx.PiperMail.FetchSize.Int()
	enrichSize := ctx.PiperMail.EnrichSize.Int()
	project := ctx.PiperMail.Project.String()
	esIndex := ctx.PiperMail.EsIndex.String()
	affBaseURL := ctx.Env("AFFILIATIONS_API_BASE_URL")
	esCacheURL := ctx.Env("ES_CACHE_URL")
	esCacheUsername := ctx.Env("ES_CACHE_USERNAME")
	esCachePassword := ctx.Env("ES_CACHE_PASSWORD")
	authGrantType := ctx.Env("AUTH0_GRANT_TYPE")
	authClientID := ctx.Env("AUTH0_CLIENT_ID")
	authClientSecret := ctx.Env("AUTH0_CLIENT_SECRET")
	authAudience := ctx.Env("AUTH0_AUDIENCE")
	authURL := ctx.Env("AUTH0_BASE_URL")
	env := ctx.Env("ENVIRONMENT")

	mgr, err := pipermail.NewManager(origin, slug, groupName, ctx.DBConn, fetcherBackendVersion, enricherBackendVersion,
		doFetch, doEnrich, ctx.ESURL, "", "", esIndex, fromDate, project,
		fetchSize, enrichSize, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, authURL, env)

	return mgr, err
}

func buildBugzillaRestManager(ctx *lib.Ctx) (*bugzillarest.Manager, error) {
	var params bugzillarest.Param
	params.EndPoint = ctx.BugZilla.Origin.String()
	params.ShConnStr = fmt.Sprintf("%s:%s@tcp(%s)/%s", ctx.DBUser, ctx.DBPass, ctx.DBHost, ctx.DBName)
	params.FetcherBackendVersion = "0.1.0"
	params.EnricherBackendVersion = "0.1.0"
	params.ESUrl = ctx.ESURL
	params.EsUser = ""
	params.EsPassword = ""
	params.Fetch = ctx.BugZilla.DoFetch.Bool()
	params.Enrich = ctx.BugZilla.DoEnrich.Bool()
	params.FromDate = ctx.BugZilla.FromDate.Date()
	params.FetchSize = ctx.BugZilla.FetchSize.Int()
	params.EnrichSize = ctx.BugZilla.EnrichSize.Int()
	params.Project = ctx.BugZilla.Project.String()
	params.EsIndex = ctx.RichIndex

	params.Retries = uint(3)
	if ctx.Retry != 0 {
		params.Retries = uint(ctx.Retry)
	}

	params.Delay = 2 * time.Second
	if ctx.Delay != 0*time.Second {
		params.Delay = ctx.Delay

	}

	params.GapURL = ctx.GapURL
	params.Slug = ctx.BugZilla.ProjectSlug.String()

	params.AffBaseURL = ctx.Env("AFFILIATIONS_API_BASE_URL")
	params.ESCacheURL = ctx.Env("ES_CACHE_URL")
	params.ESCacheUsername = ctx.Env("ES_CACHE_USERNAME")
	params.ESCachePassword = ctx.Env("ES_CACHE_PASSWORD")
	params.AuthGrantType = ctx.Env("AUTH0_GRANT_TYPE")
	params.AuthClientID = ctx.Env("AUTH0_CLIENT_ID")
	params.AuthClientSecret = ctx.Env("AUTH0_CLIENT_SECRET")
	params.AuthAudience = ctx.Env("AUTH0_AUDIENCE")
	params.AuthURL = ctx.Env("AUTH0_BASE_URL")
	params.Environment = ctx.Env("ENVIRONMENT")

	mgr, err := bugzillarest.NewManager(params)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}
