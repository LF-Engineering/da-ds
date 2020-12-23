package utils

import (
	"bytes"
	"encoding/csv"
	"log"
	"net/http"
	"time"
)

// HTTPClientProvider ...
type HTTPClientProvider struct {
	httpclient *http.Client
}

// NewHTTPClientProvider ...
func NewHTTPClientProvider(timeout time.Duration) *HTTPClientProvider {
	return &HTTPClientProvider{
		httpclient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Response returned from http request
type Response struct {
	StatusCode int
	Body       []byte
}

// Request http
func (h *HTTPClientProvider) Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return 0, nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	if header != nil {
		for k, v := range header {
			req.Header.Add(k, v)
		}
	}

	if params != nil {
		for k, v := range params {
			req.URL.Query().Add(k, v)
		}
	}

	// Do request
	res, err := h.httpclient.Do(req)
	if err != nil {
		return 0, nil, err
	}

	var buf bytes.Buffer
	_, err = buf.ReadFrom(res.Body)
	if err != nil {
		return 0, nil, err
	}

	return res.StatusCode, buf.Bytes(), nil
}

// RequestCSV requests http API that returns csv result
func (h *HTTPClientProvider) RequestCSV(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Err: %s", err.Error())
		}
	}()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}
