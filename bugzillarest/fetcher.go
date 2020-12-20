package bugzillarest

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
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

type AttachmentRes struct {
	Bugs map[string][]Attachment `json:"bugs"`
}

type HistoryRes struct {
	Bugs []HistoryBug
}

type HistoryBug struct {
	ID      int
	History []History
	Alias   []string
}

type History struct {
	Changes []Change
	Who     string
	When    time.Time
}

type Change struct {
	Added        string
	Removed      string
	FieldName    string
	AttachmentID *string
}

type Attachment struct {
	Data           string
	Size           int
	CreationTime   time.Time
	LastChangeTime time.Time
	ID             int
	BugID          int
	FileName       string
	Summary        string
	ContentType    string
	IsPrivate      bool
	IsObsolete     bool
	IsPatch        bool
	Creator        string
	Flags          []string
}

type BugzillaRestRaw struct {
	Data                     BugData `json:"data"`
	UUID                     string `json:"uuid"`
	MetadataUpdatedOn        time.Time `json:"metadata__updated_on"`
	ClassifiedFieldsFiltered *string `json:"classified_fields_filtered"`
	UpdatedOn                float64 `json:"updated_on"`
	BackendName              string `json:"backend_name"`
	Category                 string `json:"category"`
	Origin                   string `json:"origin"`
	BackendVersion           string `json:"backend_version"`
	Tag                      string `json:"tag"`
	Timestamp                float64 `json:"timestamp"`
	MetadataTimestamp        time.Time `json:"metadata__timestamp"`
}

type FetchedBugs struct {
	Bugs []BugData
}

type BugData struct {
	History             *[]History `json:"history"`
	Resolution          string `json:"resolution"`
	Priority            string `json:"priority"`
	Keywords            []string `json:"keywords"`
	DependsOn           []string `json:"depends_on"`
	Alias               []string `json:"alias"`
	IsCcAccessible      bool `json:"is_cc_accessible"`
	Duplicates          []int `json:"duplicates"`
	SeeAlso             []string `json:"see_also"`
	LastChangeTime      time.Time `json:"last_change_time"`
	CreatorDetail       *PersonDetail `json:"creator_detail"`
	Blocks              []int `json:"blocks"`
	TargetMilestone     string `json:"target_milestone"`
	Deadline            *string `json:"deadline"`
	IsOpen              bool `json:"is_open"`
	RemainingTime       int `json:"remaining_time"`
	Flags               []string `json:"flags"`
	Groups              []string `json:"groups"`
	Component           string `json:"component"`
	Platform            string `json:"platform"`
	Comments            Comments `json:"comments"`
	EstimatedTime       int `json:"estimated_time"`
	OpSys               string `json:"op_sys"`
	Severity            string `json:"severity"`
	Url                 string `json:"url"`
	Cc                  []string `json:"cc"`
	IsConfirmed         bool `json:"is_confirmed"`
	IsCreatorAccessible bool `json:"is_creator_accessible"`
	ActualTime          int `json:"actual_time"`
	AssignedTo          string `json:"assigned_to"`
	DupeOf              *string`json:"dupe_of"`
	Attachments         []Attachment `json:"attachments"`
	Tags                []string `json:"tags"`
	CreationTime        time.Time`json:"creation_time"`
	Whiteboard          string `json:"whiteboard"`
	CcDetail            []PersonDetail `json:"cc_detail"`
	Status              string
	Summary             string
	Classification      string
	QaContact           string
	Product             string `json:"product"`
	ID                  int `json:"id"`
	Creator             string `json:"creator"`
	Version             string `json:"version"`
	AssignedToDetail    *PersonDetail `json:"assigned_to_detail"`
}

type PersonDetail struct {
	Name     string `json:"name"`
	RealName string `json:"real_name"`
	ID       int    `json:"id"`
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
func (f *Fetcher) FetchAll(origin string, date string, limit string, offset string, now time.Time) ([]BugzillaRestRaw, error) {

	url := fmt.Sprintf("%s", origin)
	d := fmt.Sprintf("%srest/bug?include_fields=_extra,_default&last_change_time=%s&limit=%s&offset=%s&", url, date, limit, offset)

	// fetch all bugs from a specific date
	_, res, err := f.HTTPClientProvider.Request(d, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	var result FetchedBugs
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	data := make([]BugzillaRestRaw, 0)
	for _, bug := range result.Bugs {
		bugRaw, err := f.FetchItem(url, bug.ID, bug, now)
		if err != nil {
			return nil, err
		}
		data = append(data, *bugRaw)
	}

	return data, nil
}

// FetchItem fetches bug item
func (f *Fetcher) FetchItem(origin string, bugId int, fetchedBug BugData, now time.Time) (*BugzillaRestRaw, error) {

	url := fmt.Sprintf("%srest/bug", origin)

	// fetch bug comments
	comments, err := f.fetchComments(url, bugId)
	if err != nil {
		return nil, err
	}

	// fetch bug history
	history, err := f.fetchHistory(url, bugId)
	if err != nil {
		return nil, err
	}

	// fetch bug attachments
	attachments, err := f.fetchAttachments(url, bugId)
	if err != nil {
		return nil, err
	}

	var bugRaw BugzillaRestRaw

	// generate UUID
	uid, err := uuid.Generate(url, strconv.Itoa(bugId))
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
	bugRaw.Data.Url = fetchedBug.Url
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
	bugRaw.UpdatedOn = utils.ConvertTimeToFloat(fetchedBug.LastChangeTime)
	bugRaw.Category = "bug"

	// todo : BackendName, BackendVersion will be a param
	bugRaw.BackendName = "bugzillarest"
	bugRaw.BackendVersion = "0.0.1"
	bugRaw.Origin = origin
	bugRaw.Tag = origin
	bugRaw.Data.Cc = fetchedBug.Cc
	bugRaw.Data.CcDetail = fetchedBug.CcDetail
	bugRaw.Data.AssignedTo = fetchedBug.AssignedTo
	bugRaw.Data.AssignedToDetail = fetchedBug.AssignedToDetail

	bugRaw.MetadataTimestamp = now.UTC()
	bugRaw.Timestamp = utils.ConvertTimeToFloat(bugRaw.MetadataTimestamp)
fmt.Println("=====xx")
	fmt.Println(bugRaw.Timestamp)
	return &bugRaw, nil
}

func (f *Fetcher) fetchComments(url string, id int) (Comments, error) {
	commentsUrl := fmt.Sprintf("%s/%v/%s", url, id, "comment")
	fmt.Println("fetch")
	fmt.Println(commentsUrl)
	_, res, err := f.HTTPClientProvider.Request(commentsUrl, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	result := map[string]map[string]map[string]Comments{}

	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}
	comments := result["bugs"][strconv.Itoa(id)]["comments"]
	fmt.Println(comments[0].Creator)

	return comments, nil
}

func (f *Fetcher) fetchHistory(url string, id int) ([]History, error) {

	historyUrl := fmt.Sprintf("%s/%v/%s", url, id, "history")
	_, res, err := f.HTTPClientProvider.Request(historyUrl, "GET", nil, nil, nil)
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

	attachmentUrl := fmt.Sprintf("%s/%v/%s", url, id, "attachment")
	_, res, err := f.HTTPClientProvider.Request(attachmentUrl, "GET", nil, nil, nil)
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
