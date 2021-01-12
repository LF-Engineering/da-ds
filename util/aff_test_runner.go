package util

import (
	"fmt"
	"testing"
	"time"

	auth "github.com/LF-Engineering/dev-analytics-libraries/auth0"
	"github.com/LF-Engineering/dev-analytics-libraries/http"

	"github.com/stretchr/testify/assert"
)

func TestGetAffiliationIdentity(t *testing.T) {

	var params Params
	authProvider, err := auth.NewAuth0Client("localhost:9200", "elastic", "changeme", "",
		"", "", "", "", "")

	//params.AffAPI = ""
	params.AuthProvider = authProvider
	params.HttpClientProvider = http.NewClientProvider(60 * time.Second)

	params.Key = "username"
	params.Value = "manuel.teira"
	params.ProjectSlug = "yoctoproject"

	ident, err := GetAffiliationIdentity(params)

	fmt.Println(ident)

	assert.NoError(t, err)
}
