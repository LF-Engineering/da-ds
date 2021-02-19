package util

import (
	b64 "encoding/base64"
	"fmt"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	jsoniter "github.com/json-iterator/go"
)

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// Auth0Client ...
type Auth0Client interface {
	GetToken() (string, error)
}

// HandleGapData ...
func HandleGapData(gapURL string, HTTPRequest HTTPClientProvider, data []elastic.BulkData, auth0Client Auth0Client, env string) error {

	token, err := auth0Client.GetToken()
	if err != nil {
		return err
	}
	byteData, err := jsoniter.Marshal(data)
	if err != nil {
		return err
	}
	dataEnc := b64.StdEncoding.EncodeToString(byteData)
	gapBody := map[string]map[string]string{"index": {"content": dataEnc}}
	bData, err := jsoniter.Marshal(gapBody)
	if err != nil {
		return err
	}
	header := make(map[string]string)
	header["Authorization"] = fmt.Sprintf("Bearer %s", token)

	if gapURL != "" {
		_, _, err = HTTPRequest.Request(gapURL, "POST", header, bData, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// HandleFailedData ...
func HandleFailedData(data []elastic.BulkData, byteResponse []byte) (failedIndexes []elastic.BulkData, err error) {
	var esRes ElasticResponse
	err = jsoniter.Unmarshal(byteResponse, &esRes)
	if err != nil {
		return failedIndexes, err
	}

	// loop throw elastic response to get failed indexes
	for _, item := range esRes.Items {
		if item.Index.Status != 200 {
			var singleBulk elastic.BulkData
			// loop throw real data to get failed ones
			for _, el := range data {
				if el.ID == item.Index.ID {
					singleBulk = el
					break
				}
			}
			failedIndexes = append(failedIndexes, singleBulk)
		}
	}
	return failedIndexes, nil
}
