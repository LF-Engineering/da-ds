package utils

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"time"
)

// HttpClientProvider ...
type HttpClientProvider struct {
	httpclient *http.Client
}

// NewHttpClientProvider ...
func NewHttpClientProvider(timeout time.Duration) *HttpClientProvider {
	return &HttpClientProvider{
		httpclient: &http.Client{
			Timeout: timeout,
		},
	}
}

type Response struct {
	StatusCode int
	Body       []byte
}

func (h *HttpClientProvider) Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error) {
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

func (h *HttpClientProvider) RequestCSV(url string) ([][]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	reader := csv.NewReader(resp.Body)
	reader.Comma = ','
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}