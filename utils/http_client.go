package utils

import (
	"bytes"
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

func (h *HttpClientProvider) Request(url string, method string, header map[string]string, body []byte) (statusCode int, resBody []byte, err error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return 0,nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	if header!=nil{
		for k, v := range header {
			req.Header.Add(k, v)
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
