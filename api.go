package dads

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var (
	gToken    string
	gTokenMtx *sync.Mutex
)

// ExecuteAffiliationsAPICall - execute a call to Affiliations API
func ExecuteAffiliationsAPICall(ctx *Ctx, method, path string, cacheToken bool) (data map[string]interface{}, err error) {
	if ctx.AffiliationAPIURL == "" {
		err = fmt.Errorf("cannot execute DA affiliation API calls, no API URL specified")
		return
	}
	var token string
	lock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Lock()
		}
	}
	unlock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Unlock()
		}
	}
	lock()
	if cacheToken {
		token = gToken
	}
	if token == "" {
		token, err = GetAPIToken()
		if err != nil {
			unlock()
			fmt.Printf("GetAPIToken error: %v\n", err)
			return
		}
		if cacheToken {
			gToken = token
		}
	}
	unlock()
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
			lock()
			token, err = GetAPIToken()
			if err != nil {
				unlock()
				fmt.Printf("GetAPIToken error: %v\n", err)
				return
			}
			if cacheToken {
				gToken = token
			}
			unlock()
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

// SendSingleDocumentToGAP - send failed ES item to GAP API
func SendSingleDocumentToGAP(ctx *Ctx, indexName, docID string, doc []byte, cacheToken bool) (err error) {
	// curl -is -XPOST \\
	// -H "Authorization: Bearer ${AUTH0_TOKEN}" \\
	// -H 'Content-Type: application/json' "${GAP_URL}" \\
	// -d"{\"index\":{\"content\":\"`echo -n '[{"IndexName": "my-index","ID":"my-id","Data":{"a":1,"b":"c"}}]' | base64 -w0`\"}}"
	if ctx.GapURL == "" {
		err = fmt.Errorf("SendSingleDocumentToGAP: GAP URL is not specified")
		return
	}
	// Token maintenance: start
	var token string
	lock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Lock()
		}
	}
	unlock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Unlock()
		}
	}
	lock()
	if cacheToken {
		token = gToken
	}
	if token == "" {
		token, err = GetAPIToken()
		if err != nil {
			unlock()
			fmt.Printf("GetAPIToken error: %v\n", err)
			return
		}
		if cacheToken {
			gToken = token
		}
	}
	unlock()
	// Token maintenance: end
	items := []map[string]interface{}{
		{
			"IndexName": indexName,
			"ID":        docID,
			"Data":      doc,
		},
	}
	bItems, _ := jsoniter.Marshal(items)
	b64Items := base64.StdEncoding.EncodeToString(bItems)
	Printf("Sending item (%d bytes) to the GAP API\n", len(b64Items))
	gapBody := map[string]interface{}{
		"index": map[string]interface{}{
			"content": b64Items,
		},
	}
	var bData []byte
	bData, err = jsoniter.Marshal(gapBody)
	if err != nil {
		Printf("Cannot marshal GAP body: %v: %v\n", gapBody, err)
		return
	}
	payloadBody := bytes.NewReader(bData)
	method := "POST"
	url := ctx.GapURL
	rurl := "redacted-gap-url"
	var req *http.Request
	for i := 0; i < 2; i++ {
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			Printf("new request error: %+v for %s url: %s, body: %s\n", err, method, url, prettyPrint(bData))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			Printf("do request error: %+v for %s url: %s, doc: %s\n", err, method, url, prettyPrint(bData))
			return
		}
		if i == 0 && (resp.StatusCode >= 400) {
			_ = resp.Body.Close()
			if resp.StatusCode == 401 {
				Printf("possibly token is invalid, trying to generate another one\n")
			} else {
				Printf("token is invalid, trying to generate another one\n")
			}
			lock()
			token, err = GetAPIToken()
			if err != nil {
				unlock()
				fmt.Printf("GetAPIToken error: %v\n", err)
				return
			}
			if cacheToken {
				gToken = token
			}
			unlock()
			continue
		}
		body, e := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			if e != nil {
				err = fmt.Errorf("readAll non-ok request error: %+v for %s url: %s", e, method, rurl)
				return
			}
			err = fmt.Errorf("method:%s url:%s status:%d\n%s", method, rurl, resp.StatusCode, body)
			return
		}
		Printf("Sent item (%d bytes) to the GAP API: status: %d\n", len(b64Items), resp.StatusCode)
		break
	}
	return
}

// SendMultipleDocumentsToGAP - send failed ES bulk to GAP API
func SendMultipleDocumentsToGAP(ctx *Ctx, indexNames, docIDs []string, docs [][]byte, cacheToken bool) (err error) {
	// curl -is -XPOST \\
	// -H "Authorization: Bearer ${AUTH0_TOKEN}" \\
	// -H 'Content-Type: application/json' "${GAP_URL}" \\
	// -d"{\"index\":{\"content\":\"`echo -n '[{"IndexName": "my-index","ID":"my-id","Data":{"a":1,"b":"c"}}]' | base64 -w0`\"}}"
	if ctx.GapURL == "" {
		err = fmt.Errorf("SendMultipleDocumentsToGAP: GAP URL is not specified")
		return
	}
	// Token maintenance: start
	var token string
	lock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Lock()
		}
	}
	unlock := func() {
		if cacheToken && gTokenMtx != nil {
			gTokenMtx.Unlock()
		}
	}
	lock()
	if cacheToken {
		token = gToken
	}
	if token == "" {
		token, err = GetAPIToken()
		if err != nil {
			unlock()
			fmt.Printf("GetAPIToken error: %v\n", err)
			return
		}
		if cacheToken {
			gToken = token
		}
	}
	unlock()
	// Token maintenance: end
	items := []map[string]interface{}{}
	for i := range docs {
		items = append(items, map[string]interface{}{"IndexName": indexNames[i], "ID": docIDs[i], "Data": docs[i]})
	}
	bItems, _ := jsoniter.Marshal(items)
	b64Items := base64.StdEncoding.EncodeToString(bItems)
	Printf("Sending %d items (%d bytes) to the GAP API\n", len(docs), len(b64Items))
	gapBody := map[string]interface{}{
		"index": map[string]interface{}{
			"content": b64Items,
		},
	}
	var bData []byte
	bData, err = jsoniter.Marshal(gapBody)
	if err != nil {
		Printf("Cannot marshal GAP body: %v: %v\n", gapBody, err)
		return
	}
	payloadBody := bytes.NewReader(bData)
	method := "POST"
	url := ctx.GapURL
	rurl := "redacted-gap-url"
	var req *http.Request
	for i := 0; i < 2; i++ {
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			Printf("new request error: %+v for %s url: %s, body: %s\n", err, method, url, prettyPrint(bData))
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			Printf("do request error: %+v for %s url: %s, doc: %s\n", err, method, url, prettyPrint(bData))
			return
		}
		if i == 0 && (resp.StatusCode >= 400) {
			_ = resp.Body.Close()
			if resp.StatusCode == 401 {
				Printf("possibly token is invalid, trying to generate another one\n")
			} else {
				Printf("token is invalid, trying to generate another one\n")
			}
			lock()
			token, err = GetAPIToken()
			if err != nil {
				unlock()
				fmt.Printf("GetAPIToken error: %v\n", err)
				return
			}
			if cacheToken {
				gToken = token
			}
			unlock()
			continue
		}
		body, e := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			if e != nil {
				err = fmt.Errorf("readAll non-ok request error: %+v for %s url: %s", e, method, rurl)
				return
			}
			err = fmt.Errorf("method:%s url:%s status:%d\n%s", method, rurl, resp.StatusCode, body)
			return
		}
		Printf("Sent item (%d bytes) to the GAP API: status: %d\n", len(b64Items), resp.StatusCode)
		break
	}
	return
}
