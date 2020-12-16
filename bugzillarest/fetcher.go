package bugzillarest

import (
	"fmt"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
)

type Comment struct {
	ID           int
	Creator      string
	Time         time.Time
	Count        int
	IsPrivate    bool
	CreationTime time.Time
	AttachmentID *int
	Tags         []string
}

type Comments []Comment

type CommentsResponse struct {
	Bugs map[string]interface {
	}
}

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

type Fetcher struct {
	HTTPClientProvider HTTPClientProvider
}

// NewFetcher initiates a new bugZillaRest fetcher
func NewFetcher(httpClientProvider HTTPClientProvider) *Fetcher {
	return &Fetcher{
		HTTPClientProvider: httpClientProvider,
	}
}

// FetchItem fetches bug item
func (f *Fetcher) FetchItem() error {
	url := "https://bugs.dpdk.org/rest/bug"
	bugId := 601
	bugUrl := fmt.Sprintf("%s/%v", url, bugId)
	_, res, err := f.HTTPClientProvider.Request(bugUrl, "GET", nil, nil, nil)
	if err != nil {
		return err
	}

	result := make(map[string]interface{})
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return err
	}

	err = f.FetchComments(url, bugId)
	if err != nil {
		return err
	}
	//fmt.Println("res....")
	//fmt.Println(result)
	return nil
}

func (f *Fetcher) FetchComments(url string, id int) error {
	commentsUrl := fmt.Sprintf("%s/%v/%s", url, id, "comment")
	_, res, err := f.HTTPClientProvider.Request(commentsUrl, "GET", nil, nil, nil)
	if err != nil {
		return err
	}

	result := map[string]map[string]map[string]Comments{}

	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return err
	}
	arr := result["bugs"][strconv.Itoa(id)]["comments"]
	fmt.Println(arr[0].Creator)

	return nil
}
