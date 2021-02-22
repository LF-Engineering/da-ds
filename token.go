package dads

import (
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	"github.com/LF-Engineering/dev-analytics-libraries/http"
	"github.com/LF-Engineering/dev-analytics-libraries/slack"
	jsoniter "github.com/json-iterator/go"
)

var (
	gAuth0Client *auth0.ClientProvider
	gTokenEnv    string
	gTokenEnvMtx *sync.Mutex
)

// InitializeAuth0 - initializes Auth0 client using data stored in AUTH0_DATA
func InitializeAuth0(ctx *Ctx) error {
	var err error
	auth0DataB64 := os.Getenv("AUTH0_DATA")
	if auth0DataB64 == "" {
		return fmt.Errorf("you must specify AUTH0_DATA (so the program can generate an API token) or specify token with JWT_TOKEN")
	}
	var auth0Data []byte
	auth0Data, err = base64.StdEncoding.DecodeString(auth0DataB64)
	if err != nil {
		Printf("decode base64 error: %+v\n", err)
		return err
	}
	//fmt.Printf("auth0Data: %v\n", auth0Data)
	var data map[string]string
	err = jsoniter.Unmarshal([]byte(auth0Data), &data)
	if err != nil {
		Printf("unmarshal error: %+v\n", err)
		return err
	}
	AddRedacted(data["ES_CACHE_URL"], false)
	AddRedacted(data["ES_CACHE_USERNAME"], false)
	AddRedacted(data["ES_CACHE_PASSWORD"], false)
	AddRedacted(data["client_id"], false)
	AddRedacted(data["client_secret"], false)
	AddRedacted(data["audience"], false)
	AddRedacted(data["url"], false)

	authSecret := os.Getenv("AUTH_SECRET")
	esCacheURL := ctx.Env("ES_CACHE_URL")
	slackProvider := slack.New(os.Getenv("SLACK_WEBHOOK_URL"))
	httpClientProvider := http.NewClientProvider(time.Minute)
	esClientProvider, err := elastic.NewClientProvider(&elastic.Params{
		URL:      esCacheURL,
		Username: data["ES_CACHE_USERNAME"],
		Password: data["ES_CACHE_PASSWORD"],
	})
	if err != nil {
		return err
	}
	gAuth0Client, err = auth0.NewAuth0Client(
		data["ES_CACHE_URL"],
		data["ES_CACHE_USERNAME"],
		data["ES_CACHE_PASSWORD"],
		data["env"],
		data["grant_type"],
		data["client_id"],
		data["client_secret"],
		data["audience"],
		data["url"],
		authSecret,
		httpClientProvider,
		esClientProvider,
		&slackProvider,
	)
	if err == nil {
		gTokenEnv = data["env"]
	}
	return err
}

// GetAPIToken - return an API token to use dev-analytics-api API calls
// If JWT_TOKEN env is specified - just use the provided token without any checks
// Else get auth0 data from AUTH0_DATA and generate/reuse a token stored in ES cache
func GetAPIToken(ctx *Ctx) (string, error) {
	envToken := os.Getenv("JWT_TOKEN")
	if envToken != "" {
		return envToken, nil
	}
	if gTokenEnvMtx != nil {
		gTokenEnvMtx.Lock()
		defer gTokenEnvMtx.Unlock()
	}
	if gTokenEnv == "" {
		err := InitializeAuth0(ctx)
		if err != nil {
			return "", err
		}
	}
	token, err := gAuth0Client.GetToken()
	return token, err
}
