package main

import (
	"encoding/json"
	"flag"
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

// todo: if you want to use it later
func dockerhubFlags() (*dockerhub.Manager, error) {
	username := flag.String("username", "", "username")
	password := flag.String("password", "", "password")
	fetcherBackendVersion := flag.String("fetcher_version", "", "fetcher backend version")
	enricherBackendVersion := flag.String("enricher_version", "", "enricher backend version")
	esUrl := flag.String("enricher_version", "", "enricher backend version")
	esUsername := flag.String("es_username", "", "elasticsearch username")
	esPassword := flag.String("es_password", "", "elasticsearch password")
	httpTimeout := flag.Duration("http_timeout", time.Duration(0), "http timeout")
	repositoriesJson := flag.String("repositories", "", "repositories in json format e.g. [{'owner', 'repository'},...]")

	flag.Parse()

	var repositories []*dockerhub.Repository
	if err := json.Unmarshal([]byte(*repositoriesJson), &repositories); err != nil {
		return nil, err
	}

	return dockerhub.NewManager(*username, *password, *fetcherBackendVersion, *enricherBackendVersion,
		*esUrl, *esUsername, *esPassword, *httpTimeout,  repositories), nil
}

func dockerhubEnvs(ctx *lib.Ctx) (*dockerhub.Manager, error) {
	username := ctx.Env("USERNAME")
	password := ctx.Env("PASSWORD")
	fetcherBackendVersion := ctx.Env("FETCHER_BACKEND_VERSION")
	enricherBackendVersion := ctx.Env("ENRICHER_BACKEND_VERSION")
	esUrl := ctx.Env("ES_URL")
	esUsername := ctx.Env("ES_USERNAME")
	esPassword := ctx.Env("ES_PASSWORD")
	httpTimeout := ctx.Env("HTTP_TIMEOUT") // "60s" 60 seconds...
	repositoriesJson := ctx.Env("REPOSITORIES_JSON")

	var repositories []*dockerhub.Repository
	if err := json.Unmarshal([]byte(repositoriesJson), &repositories); err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, err
	}

	return dockerhub.NewManager(username, password, fetcherBackendVersion, enricherBackendVersion,
		esUrl, esUsername, esPassword, timeout,  repositories), nil
}