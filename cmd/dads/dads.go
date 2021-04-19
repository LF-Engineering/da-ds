package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/LF-Engineering/da-ds/build"

	"github.com/LF-Engineering/dev-analytics-libraries/slack"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"

	"github.com/LF-Engineering/da-ds/bugzillarest"
	"github.com/LF-Engineering/da-ds/googlegroups"

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
	case googlegroups.GoogleGroups:
		manager, err := buildGoogleGroupsManager(ctx)
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

	var params dockerhub.Param

	// Dockerhub credentials
	params.Username = ctx.Env("USERNAME")
	params.Password = ctx.Env("PASSWORD")
	params.FetcherBackendVersion = "0.0.1"
	params.EnricherBackendVersion = "0.0.1"
	params.ESUrl = ctx.ESURL
	params.EnrichOnly = ctx.NoRaw
	params.Enrich = ctx.Enrich
	params.FromDate = ctx.DateFrom
	params.NoIncremental = ctx.BoolEnv("NO_INCREMENTAL")
	params.Retries = uint(ctx.Retry)
	params.Delay = ctx.Delay
	params.GapURL = ctx.GapURL

	params.AffBaseURL = ctx.Env("AFFILIATION_API_URL") + "/v1"
	params.ESCacheURL = ctx.Env("ES_CACHE_URL")
	params.ESCacheUsername = ctx.Env("ES_CACHE_USERNAME")
	params.ESCachePassword = ctx.Env("ES_CACHE_PASSWORD")
	params.AuthGrantType = ctx.Env("AUTH0_GRANT_TYPE")
	params.AuthClientID = ctx.Env("AUTH0_CLIENT_ID")
	params.AuthClientSecret = ctx.Env("AUTH0_CLIENT_SECRET")
	params.AuthAudience = ctx.Env("AUTH0_AUDIENCE")
	params.Auth0URL = ctx.Env("AUTH0_URL")
	params.Environment = ctx.Env("BRANCH")
	params.SlackWebHookURL = ctx.SlackWebHookURL

	repositoriesJSON := ctx.Env("REPOSITORIES_JSON")
	if err := jsoniter.Unmarshal([]byte(repositoriesJSON), &params.Repositories); err != nil {
		return nil, err
	}

	httpTimeout := ctx.Env("HTTP_TIMEOUT") // "60s" 60 seconds...
	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}
	params.HTTPTimeout = timeout

	return dockerhub.NewManager(params), nil
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
	scrollSize := ctx.ESScrollSize
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
		enrichOnly, enrich, esURL, timeout, buildServers, fromDate, noIncremental, bulkSize, scrollSize), nil
}

func buildBugzillaManager(ctx *lib.Ctx) (*bugzilla.Manager, error) {
	var params bugzilla.Param
	params.EndPoint = ctx.BugZilla.Origin.String()
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

	params.AffBaseURL = ctx.Env("AFFILIATION_API_URL") + "/v1"
	params.ESCacheURL = ctx.Env("ES_CACHE_URL")
	params.ESCacheUsername = ctx.Env("ES_CACHE_USERNAME")
	params.ESCachePassword = ctx.Env("ES_CACHE_PASSWORD")
	params.AuthGrantType = ctx.Env("AUTH0_GRANT_TYPE")

	params.AuthClientID = ctx.Env("AUTH0_CLIENT_ID")
	params.AuthClientSecret = ctx.Env("AUTH0_CLIENT_SECRET")
	params.AuthAudience = ctx.Env("AUTH0_AUDIENCE")

	params.Auth0URL = ctx.Env("AUTH0_URL")
	params.Environment = ctx.Env("BRANCH")

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
	affBaseURL := ctx.Env("AFFILIATION_API_URL")
	esCacheURL := ctx.Env("ES_CACHE_URL")
	esCacheUsername := ctx.Env("ES_CACHE_USERNAME")
	esCachePassword := ctx.Env("ES_CACHE_PASSWORD")
	authGrantType := ctx.Env("AUTH0_GRANT_TYPE")
	authClientID := ctx.Env("AUTH0_CLIENT_ID")
	authClientSecret := ctx.Env("AUTH0_CLIENT_SECRET")
	authAudience := ctx.Env("AUTH0_AUDIENCE")
	auth0URL := ctx.Env("AUTH0_URL")
	env := ctx.Env("BRANCH")

	mgr, err := pipermail.NewManager(origin, slug, groupName, ctx.DBConn, fetcherBackendVersion, enricherBackendVersion,
		doFetch, doEnrich, ctx.ESURL, "", "", esIndex, fromDate, project,
		fetchSize, enrichSize, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, auth0URL, env, ctx.SlackWebHookURL)

	return mgr, err
}

func buildBugzillaRestManager(ctx *lib.Ctx) (*bugzillarest.Manager, error) {
	params := &bugzillarest.MgrParams{}
	params.EndPoint = ctx.BugZilla.Origin.String()
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

	params.AffBaseURL = ctx.Env("AFFILIATION_API_URL") + "/v1"
	params.ESCacheURL = ctx.Env("ES_CACHE_URL")
	params.ESCacheUsername = ctx.Env("ES_CACHE_USERNAME")
	params.ESCachePassword = ctx.Env("ES_CACHE_PASSWORD")
	params.AuthGrantType = ctx.Env("AUTH0_GRANT_TYPE")
	params.AuthClientID = ctx.Env("AUTH0_CLIENT_ID")
	params.AuthClientSecret = ctx.Env("AUTH0_CLIENT_SECRET")
	params.AuthAudience = ctx.Env("AUTH0_AUDIENCE")
	params.Auth0URL = ctx.Env("AUTH0_URL")
	params.Environment = ctx.Env("BRANCH")

	fetcher, enricher, esClientProvider, auth0ClientProvider, httpClientProvider, err := buildBugzillaRestMgrServices(params)
	if err != nil {
		return nil, err
	}

	params.Fetcher = fetcher
	params.Enricher = enricher
	params.ESClientProvider = esClientProvider
	params.Auth0ClientProvider = auth0ClientProvider
	params.HTTPClientProvider = httpClientProvider

	mgr, err := bugzillarest.NewManager(params)
	if err != nil {
		return nil, err
	}

	return mgr, nil
}

func buildBugzillaRestMgrServices(p *bugzillarest.MgrParams) (*bugzillarest.Fetcher, *bugzillarest.Enricher, *elastic.ClientProvider, *auth0.ClientProvider, *http.ClientProvider, error) {
	httpClientProvider := http.NewClientProvider(p.HTTPTimeout)

	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      p.ESUrl,
		Username: p.EsUser,
		Password: p.EsPassword,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	esCacheClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      p.ESCacheURL,
		Username: p.ESCacheUsername,
		Password: p.ESCachePassword,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	slackProvider := slack.New(p.WebHookURL)
	// Initialize fetcher object to get data from bugzilla rest api
	fetcher := bugzillarest.NewFetcher(&bugzillarest.FetcherParams{Endpoint: p.EndPoint, BackendVersion: p.FetcherBackendVersion}, httpClientProvider, esClientProvider)

	commitID := build.GitCommit[7]
	appNameVersion := fmt.Sprintf("%s-%s", build.AppName, commitID)
	auth0Client, err := auth0.NewAuth0Client(
		p.Environment,
		p.AuthGrantType,
		p.AuthClientID,
		p.AuthClientSecret,
		p.AuthAudience,
		p.Auth0URL,
		httpClientProvider,
		esCacheClientProvider,
		&slackProvider,
		appNameVersion)

	affiliationsClientProvider, err := libAffiliations.NewAffiliationsClient(p.AffBaseURL, p.Slug, httpClientProvider, esCacheClientProvider, auth0Client, &slackProvider)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// Initialize enrich object to enrich raw data
	enricher := bugzillarest.NewEnricher(&bugzillarest.EnricherParams{BackendVersion: p.EnricherBackendVersion, Project: p.Project}, affiliationsClientProvider)

	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return fetcher, enricher, esClientProvider, auth0Client, httpClientProvider, err
}

func buildGoogleGroupsManager(ctx *lib.Ctx) (*googlegroups.Manager, error) {
	slug := ctx.GoogleGroups.ProjectSlug.String()
	groupName := ctx.GoogleGroups.GroupName.String()
	fetcherBackendVersion := "0.0.1"
	enricherBackendVersion := "0.0.1"
	doFetch := ctx.GoogleGroups.DoFetch.Bool()
	doEnrich := ctx.GoogleGroups.DoEnrich.Bool()
	fromDate := ctx.GoogleGroups.FromDate.Date()
	fetchSize := ctx.GoogleGroups.FetchSize.Int()
	enrichSize := ctx.GoogleGroups.EnrichSize.Int()
	project := ctx.GoogleGroups.Project.String()
	esIndex := ctx.Env("RICH_INDEX")
	affBaseURL := ctx.Env("AFFILIATIONS_API_BASE_URL")
	esCacheURL := ctx.Env("ES_CACHE_URL")
	esCacheUsername := ctx.Env("ES_CACHE_USERNAME")
	esCachePassword := ctx.Env("ES_CACHE_PASSWORD")
	authGrantType := ctx.Env("AUTH0_GRANT_TYPE")
	authClientID := ctx.Env("AUTH0_CLIENT_ID")
	authClientSecret := ctx.Env("AUTH0_CLIENT_SECRET")
	authAudience := ctx.Env("AUTH0_AUDIENCE")
	authURL := ctx.Env("AUTH0_URL")
	env := os.Getenv("ENVIRONMENT")

	mgr, err := googlegroups.NewManager(slug, groupName, ctx.DBConn, fetcherBackendVersion, enricherBackendVersion,
		doFetch, doEnrich, ctx.ESURL, "", "", esIndex, fromDate, project,
		fetchSize, enrichSize, affBaseURL, esCacheURL, esCacheUsername, esCachePassword, authGrantType, authClientID, authClientSecret, authAudience, authURL, env)

	return mgr, err
}
