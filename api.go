package dads

import (
	"fmt"
	"io/ioutil"
	"net/http"

	jsoniter "github.com/json-iterator/go"
)

// ExecuteAffiliationsAPICall - execute a call to Affiliations API
func ExecuteAffiliationsAPICall(ctx *Ctx, method, path string) (data map[string]interface{}, err error) {
	if ctx.AffiliationAPIURL == "" {
		err = fmt.Errorf("cannot execute DA affiliation API calls, no API URL specified")
		return
	}
	var token string
	token, err = GetAPIToken()
	if err != nil {
		return
	}
	rurl := path
	url := ctx.AffiliationAPIURL + rurl
	for i := 0; i < 2; i++ {
		req, e := http.NewRequest(method, url, nil)
		if e != nil {
			err = fmt.Errorf("new request error: %+v for %s url: %s", e, method, rurl)
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			err = fmt.Errorf("do request error: %+v for %s url: %s", e, method, rurl)
			return
		}
		if i == 0 && resp.StatusCode == 401 {
			_ = resp.Body.Close()
			Printf("token is invalid, trying to generate another one\n")
			token, err = GetAPIToken()
			if err != nil {
				return
			}
			continue
		}
		body, e := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			if e != nil {
				err = fmt.Errorf("readAll non-ok request error: %+v for %s url: %s", e, method, rurl)
				return
			}
			err = fmt.Errorf("method:%s url:%s status:%d\n%s", method, rurl, resp.StatusCode, body)
			return
		}
		err = jsoniter.Unmarshal(body, &data)
		if err != nil {
			Printf("unmarshal error: %+v\n", err)
			return
		}
		break
	}
	return
}
