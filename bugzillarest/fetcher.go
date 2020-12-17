package bugzillarest

import (
	"fmt"
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
	Bugs map[string][]Attachment
}

type HistoryRes struct {
	Bugs []HistoryBug
}

type HistoryBug struct {
	ID      int
	History []History
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
	Data BugData
	UUID string
}

type FetchedBugs struct {
	Bugs []BugData
}

type BugData struct {
	History             []History
	Resolution          string
	Priority            string
	Keywords            []string
	DependsOn           []string
	Alias               []string
	IsCcAccessible      bool
	Duplicates          []int
	SeeAlso             []string
	LastChangeTime      time.Time
	CreatorDetail       PersonDetail
	Blocks              []int
	TargetMilestone     string
	Deadline            *string
	IsOpen              bool
	RemainingTime       int
	Flags               []string
	Groups              []string
	Component           string
	Platform            string
	Comments            Comments
	EstimatedTime       int
	OpSys               string
	Severity            string
	Url                 string
	Cc                  []string
	IsConfirmed         bool
	IsCreatorAccessible bool
	ActualTime          int
	AssignedTo          string
	DupeOf              *string
	Attachments         []Attachment
	Tags                []string
	CreationTime        time.Time
	Whiteboard          string
	CcDetail            PersonDetail
	Status              string
	Summary             string
	Classification      string
	QaContact           string
	Product             string
	ID                  int
	Creator             string
	Version             string
	AssignedToDetail    PersonDetail

	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	UpdatedOn int
	BackendName string
	Category string
	Origin string
	BackendVersion string
	Tag string
	TimeStamp string
	MetadataTimestamp	time.Time `json:"metadata__timestamp"`

}

type PersonDetail struct {
	Name     string
	RealName string
	ID       int
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
func (f *Fetcher) FetchAll(url string, date string, limit string, offset string) ([]BugzillaRestRaw,error) {

	d := fmt.Sprintf("%s?include_fields=_extra,_default&last_change_time=%s&limit=%s&offset=%s&",url, date, limit, offset)

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

	fmt.Println("bugs ===")
	fmt.Println(len(result.Bugs))

	data := make([]BugzillaRestRaw, 0)
	for _,bug := range result.Bugs{
		bugRaw,err := f.FetchItem(url , bug.ID , bug )
		if err != nil {
			return nil, err
		}
		data = append(data, *bugRaw)
		fmt.Println("bug no :")
		fmt.Println(bug.ID)
	}

	return data, nil
}

// FetchItem fetches bug item
func (f *Fetcher) FetchItem(url string, bugId int, fetchedBug BugData) (*BugzillaRestRaw, error) {

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
	bugRaw.Data.History = history
	bugRaw.Data.Attachments = attachments

	//fetchedBug := result

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

	bugRaw.Data.MetadataUpdatedOn = fetchedBug.LastChangeTime


	return &bugRaw, nil
}

func (f *Fetcher) fetchComments(url string, id int) (Comments, error) {
	commentsUrl := fmt.Sprintf("%s/%v/%s", url, id, "comment")
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
