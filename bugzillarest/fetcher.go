package bugzillarest

import (
	"fmt"
	timeLib "github.com/LF-Engineering/dev-analytics-libraries/time"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
)

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// Fetcher ...
type Fetcher struct {
	dSName                string
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	Endpoint              string
	BackendName           string
}

// Params required parameters for bugzilla fetcher
type Params struct {
	Name           string
	Endpoint       string
	FromDate       time.Time
	Order          string
	Project        string
	BackendVersion string
	BackendName    string
}

// NewFetcher initiates a new bugZillaRest fetcher
func NewFetcher(params Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
		Endpoint:              params.Endpoint,
		dSName:                BugzillaRest,
	}
}

// FetchAll fetches all bugs
func (f *Fetcher) FetchAll(origin string, date string, limit string, offset string, now time.Time) ([]Raw, *time.Time, error) {

	url := fmt.Sprintf("%s", origin)
	bugsURL:= fmt.Sprintf("%srest/bug?include_fields=_extra,_default&last_change_time=%s&limit=%s&offset=%s&", url, date, limit, offset)

	// fetch all bugs from a specific date
	_, res, err := f.HTTPClientProvider.Request(bugsURL, "GET", nil, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	var result FetchedBugs
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, nil, err
	}

	data := make([]Raw, 0)
	var lastDate time.Time
	if len(result.Bugs) != 0 {
		lastDate = result.Bugs[0].LastChangeTime
	}
	for _, bug := range result.Bugs {
		if bug.LastChangeTime.After(lastDate) {
			lastDate = bug.LastChangeTime
		}
		bugRaw, err := f.FetchItem(url, bug.ID, bug, now)
		if err != nil {
			return nil, nil, err
		}
		data = append(data, *bugRaw)
	}

	return data, &lastDate, nil
}

// FetchItem fetches bug item
func (f *Fetcher) FetchItem(origin string, bugID int, fetchedBug BugData, now time.Time) (*Raw, error) {

	url := fmt.Sprintf("%srest/bug", origin)

	// fetch bug comments
	comments, err := f.fetchComments(url, bugID)
	if err != nil {
		return nil, err
	}

	// fetch bug history
	history, err := f.fetchHistory(url, bugID)
	if err != nil {
		return nil, err
	}

	// fetch bug attachments
	attachments, err := f.fetchAttachments(url, bugID)
	if err != nil {
		return nil, err
	}

	var bugRaw Raw

	// generate UUID
	uid, err := uuid.Generate(url, strconv.Itoa(bugID))
	if err != nil {
		return nil, err
	}

	bugRaw.UUID = uid
	bugRaw.Data.Comments = comments
	bugRaw.Data.History = &history
	bugRaw.Data.Attachments = attachments

	bugRaw.Data.ID = fetchedBug.ID
	bugRaw.Data.Resolution = fetchedBug.Resolution
	bugRaw.Data.Priority = fetchedBug.Priority
	bugRaw.Data.Keywords = fetchedBug.Keywords
	bugRaw.Data.DependsOn = fetchedBug.DependsOn
	bugRaw.Data.Alias = fetchedBug.Alias
	bugRaw.Data.IsCcAccessible = fetchedBug.IsCcAccessible
	bugRaw.Data.SeeAlso = fetchedBug.SeeAlso
	bugRaw.Data.LastChangeTime = fetchedBug.LastChangeTime
	bugRaw.Data.CreatorDetail = fetchedBug.CreatorDetail
	bugRaw.Data.Blocks = fetchedBug.Blocks
	bugRaw.Data.TargetMilestone = fetchedBug.TargetMilestone
	bugRaw.Data.Deadline = fetchedBug.Deadline
	bugRaw.Data.IsOpen = fetchedBug.IsOpen
	bugRaw.Data.RemainingTime = fetchedBug.RemainingTime
	bugRaw.Data.Flags = fetchedBug.Flags
	bugRaw.Data.Groups = fetchedBug.Groups
	bugRaw.Data.Component = fetchedBug.Component
	bugRaw.Data.Platform = fetchedBug.Platform
	bugRaw.Data.EstimatedTime = fetchedBug.EstimatedTime
	bugRaw.Data.OpSys = fetchedBug.OpSys
	bugRaw.Data.Severity = fetchedBug.Severity
	bugRaw.Data.URL = fetchedBug.URL
	bugRaw.Data.IsConfirmed = fetchedBug.IsConfirmed
	bugRaw.Data.IsCreatorAccessible = fetchedBug.IsCreatorAccessible
	bugRaw.Data.ActualTime = fetchedBug.ActualTime
	bugRaw.Data.DupeOf = fetchedBug.DupeOf
	bugRaw.Data.Tags = fetchedBug.Tags
	bugRaw.Data.CreationTime = fetchedBug.CreationTime
	bugRaw.Data.Whiteboard = fetchedBug.Whiteboard
	bugRaw.Data.Status = fetchedBug.Status
	bugRaw.Data.Summary = fetchedBug.Summary
	bugRaw.Data.Classification = fetchedBug.Classification
	bugRaw.Data.QaContact = fetchedBug.QaContact
	bugRaw.Data.Product = fetchedBug.Product
	bugRaw.Data.ID = fetchedBug.ID
	bugRaw.Data.Creator = fetchedBug.Creator
	bugRaw.Data.Version = fetchedBug.Version
	bugRaw.Data.Duplicates = fetchedBug.Duplicates

	bugRaw.MetadataUpdatedOn = fetchedBug.LastChangeTime
	bugRaw.ClassifiedFieldsFiltered = nil
	bugRaw.UpdatedOn = timeLib.ConvertTimeToFloat(fetchedBug.LastChangeTime)
	bugRaw.Category = Category

	bugRaw.BackendName = f.dSName
	bugRaw.BackendVersion = f.BackendVersion
	bugRaw.Origin = origin
	bugRaw.Tag = origin
	bugRaw.Data.Cc = fetchedBug.Cc
	bugRaw.Data.CcDetail = fetchedBug.CcDetail
	bugRaw.Data.AssignedTo = fetchedBug.AssignedTo
	bugRaw.Data.AssignedToDetail = fetchedBug.AssignedToDetail

	bugRaw.MetadataTimestamp = now.UTC()
	bugRaw.Timestamp = timeLib.ConvertTimeToFloat(bugRaw.MetadataTimestamp)
	return &bugRaw, nil
}

func (f *Fetcher) fetchComments(url string, id int) (Comments, error) {
	commentsURL := fmt.Sprintf("%s/%v/%s", url, id, "comment")
	_, res, err := f.HTTPClientProvider.Request(commentsURL, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	result := map[string]map[string]map[string]Comments{}

	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}
	comments := result["bugs"][strconv.Itoa(id)]["comments"]

	return comments, nil
}

func (f *Fetcher) fetchHistory(url string, id int) ([]History, error) {

	historyURL := fmt.Sprintf("%s/%v/%s", url, id, "history")
	_, res, err := f.HTTPClientProvider.Request(historyURL, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	var result HistoryRes
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	return result.Bugs[0].History, nil
}

func (f *Fetcher) fetchAttachments(url string, id int) ([]Attachment, error) {

	attachmentURL := fmt.Sprintf("%s/%v/%s", url, id, "attachment")
	_, res, err := f.HTTPClientProvider.Request(attachmentURL, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	var result AttachmentRes
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	return result.Bugs[strconv.Itoa(id)], nil
}

// Query query saved raw data from ES
func (f *Fetcher) Query(index string, query map[string]interface{}) (*RawHits, error) {

	var hits RawHits

	err := f.ElasticSearchProvider.Get(index, query, &hits)
	if err != nil {
		return nil, err
	}

	return &hits, err
}
