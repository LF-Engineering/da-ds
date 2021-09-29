package gitlab

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
)

type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	ReadWithScroll(index string, query map[string]interface{}, result interface{}, scrollID string) (err error)
	BulkInsert(data []elastic.BulkData) ([]byte, error)
}

type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
	RequestWithHeaders(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, resHeaders map[string][]string, err error)
}

type FetcherParams struct {
	BackendVersion string
	Project        string
	ProjectSlug    string
	Origin         string
	Repo           string
	Token          string
}

type Fetcher struct {
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	DSName                string
	Project               string
	ProjectSlug           string
	Origin                string
	Repo                  string
	Token                 string
}

func NewFetcher(params *FetcherParams, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
		Project:               params.Project,
		ProjectSlug:           params.ProjectSlug,
		Origin:                params.Origin,
		DSName:                DATASOURCE,
		Repo:                  params.Repo,
		Token:                 params.Token,
	}
}

func (f *Fetcher) FetchMergeRequests(projectID string, lastDate time.Time) ([]MergeRequestRaw, error) {
	var (
		rawAry                = make([]MergeRequestRaw, 0)
		mergeRequestResponses []MergeRequestData
	)
	lastDateISO := lastDate.Format(time.RFC3339)

	gitlabURL := fmt.Sprintf("%s/%s/projects/%s/merge_requests?per_page=100&updated_after=%s", GITLAB_API_BASE, GITLAB_API_VERSION, projectID, lastDateISO)

	headers := map[string]string{
		"PRIVATE-TOKEN": f.Token,
	}
	resStatus, resBody, resHeaders, err := f.HTTPClientProvider.RequestWithHeaders(gitlabURL, "GET", headers, nil, nil)
	if err != nil {
		return nil, err
	}

	if resStatus == http.StatusOK {
		err = jsoniter.Unmarshal(resBody, &mergeRequestResponses)
		if err != nil {
			return nil, err
		}
	}

	linkData := resHeaders["Link"]
	next := getNextLink(linkData)

	for next != "" {
		resStatus, resBody, resHeaders, err = f.HTTPClientProvider.RequestWithHeaders(next, "GET", headers, nil, nil)

		if err != nil {
			return nil, err
		}

		var nextMergeRequestResponses []MergeRequestData
		if resStatus == http.StatusOK {
			err = jsoniter.Unmarshal(resBody, &nextMergeRequestResponses)
			if err != nil {
				return nil, err
			}
			mergeRequestResponses = append(mergeRequestResponses, nextMergeRequestResponses...)
		}

		linkData = resHeaders["Link"]
		next = getNextLink(linkData)

		if next == "" {
			break
		}
	}

	for _, merge_request := range mergeRequestResponses {
		var raw MergeRequestRaw
		raw.Data = merge_request
		raw.Data.Type = "merge_request"
		raw.MetadataUpdatedOn = merge_request.UpdatedAt
		raw.MetadataTimestamp = time.Now()
		raw.Timestamp = timeLib.ConvertTimeToFloat(raw.MetadataTimestamp)
		raw.BackendVersion = f.BackendVersion
		raw.BackendName = f.DSName
		raw.Project = f.Project
		raw.ProjectSlug = f.ProjectSlug
		raw.Repo = f.Repo

		mergeRequestURL := fmt.Sprintf("%s/projects/merge_request/%s", GITLAB_API_BASE, projectID)
		uuid, err := uuid.Generate(mergeRequestURL, strconv.Itoa(merge_request.MergeRequestID))
		if err != nil {
			return nil, err
		}
		raw.UUID = uuid

		rawAry = append(rawAry, raw)
	}

	return rawAry, nil
}

func (f *Fetcher) FetchIssues(projectID string, lastDate time.Time) ([]IssueRaw, error) {
	var (
		rawAry         = make([]IssueRaw, 0)
		issueResponses []IssueData
	)

	lastDateISO := lastDate.Format(time.RFC3339)
	gitlabURL := fmt.Sprintf("%s/%s/projects/%s/issues?per_page=100&updated_after=%s", GITLAB_API_BASE, GITLAB_API_VERSION, projectID, lastDateISO)

	headers := map[string]string{
		"PRIVATE-TOKEN": f.Token,
	}
	resStatus, resBody, resHeaders, err := f.HTTPClientProvider.RequestWithHeaders(gitlabURL, "GET", headers, nil, nil)
	if err != nil {
		return nil, err
	}

	if resStatus == http.StatusOK {
		err = jsoniter.Unmarshal(resBody, &issueResponses)
		if err != nil {
			return nil, err
		}
	}

	linkData := resHeaders["Link"]
	next := getNextLink(linkData)

	for next != "" {
		resStatus, resBody, resHeaders, err = f.HTTPClientProvider.RequestWithHeaders(next, "GET", headers, nil, nil)

		if err != nil {
			return nil, err
		}

		var nextIssueResponses []IssueData
		if resStatus == http.StatusOK {
			err = jsoniter.Unmarshal(resBody, &nextIssueResponses)
			if err != nil {
				return nil, err
			}
			issueResponses = append(issueResponses, nextIssueResponses...)
		}

		linkData = resHeaders["Link"]
		next = getNextLink(linkData)

		if next == "" {
			break
		}
	}

	for _, issue := range issueResponses {
		var raw IssueRaw
		raw.Data = issue
		raw.MetadataUpdatedOn = issue.UpdatedAt
		raw.MetadataTimestamp = time.Now()
		raw.Timestamp = timeLib.ConvertTimeToFloat(raw.MetadataTimestamp)
		raw.BackendVersion = f.BackendVersion
		raw.BackendName = f.DSName
		raw.Project = f.Project
		raw.ProjectSlug = f.ProjectSlug
		raw.Repo = f.Repo

		issueURL := fmt.Sprintf("%s/projects/issue/%s", GITLAB_API_BASE, projectID)
		uuid, err := uuid.Generate(issueURL, strconv.Itoa(issue.IssueID))
		if err != nil {
			return nil, err
		}

		raw.UUID = uuid
		rawAry = append(rawAry, raw)
	}

	return rawAry, nil
}

func getNextLink(link []string) (next string) {
	linkString := link[0]
	allLinks := strings.Split(linkString, ",")

	for _, i := range allLinks {
		linkAry := strings.Split(strings.TrimSpace(i), " ")
		desc := strings.TrimSpace(linkAry[1])

		if desc == "rel=\"next\"" {
			next = strings.Trim(linkAry[0], "<>;")
		}
	}
	return
}

func (f *Fetcher) getProjectID(repo string) (projectID string, err error) {
	u, err := url.Parse(repo)
	if err != nil {
		return "", err
	}

	encodedPath := url.QueryEscape(strings.TrimLeft(u.Path, "/"))
	projectURL := fmt.Sprintf("%s/%s/projects/%s", GITLAB_API_BASE, GITLAB_API_VERSION, encodedPath)

	headers := map[string]string{
		"PRIVATE-TOKEN": f.Token,
	}

	resStatus, resBody, err := f.HTTPClientProvider.Request(projectURL, "GET", headers, nil, nil)
	if err != nil {
		return "", err
	}

	var projectResponse Project
	if resStatus == http.StatusOK {
		err = jsoniter.Unmarshal(resBody, &projectResponse)
		if err != nil {
			return "", err
		}
	}

	return strconv.Itoa(projectResponse.ID), nil
}

func (f *Fetcher) GetLastFetchDate(index string) time.Time {
	lastDate, err := f.ElasticSearchProvider.GetStat(index, "metadata__updated_on", "max", nil, nil)
	if err != nil {
		return DefaultDateTime
	}

	return lastDate
}
