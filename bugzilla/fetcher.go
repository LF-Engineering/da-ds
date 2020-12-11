package bugzilla

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LF-Engineering/da-ds/utils"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
)

// Fetcher contains Bugzilla fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	HttpClientProvider    HttpClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	Endpoint              string
}

// HttpClientProvider used in connecting to remote http server
type HttpClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
	RequestCSV(url string) ([][]string, error)
}

// ESClientProvider used in connecting to ES server
type ESClientProvider interface {
	Add(index string, documentID string, body []byte) ([]byte, error)
	CreateIndex(index string, body []byte) ([]byte, error)
	DeleteIndex(index string, ignoreUnavailable bool) ([]byte, error)
	Bulk(body []byte) ([]byte, error)
	Get(index string, query map[string]interface{}, result interface{}) (err error)
	GetStat(index string, field string, aggType string, mustConditions []map[string]interface{}, mustNotConditions []map[string]interface{}) (result time.Time, err error)
	BulkInsert(data []*utils.BulkData) ([]byte, error)
}

// Params required parameters for bugzilla fetcher
type Params struct {
	Name           string
	Endpoint       string
	FromDate       time.Time
	Order          string
	Project        string
	Component      string
	Category       string
	BackendVersion string
}

// NewFetcher initiates a new bugZilla fetcher
func NewFetcher(params *Params, httpClientProvider HttpClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		DSName:                Bugzilla,
		HttpClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
		Endpoint:              params.Endpoint,
	}
}

// FetchItems ...
func (f *Fetcher) FetchItem(fromDate time.Time, limit int, now time.Time) ([]*BugRaw, error) {
	bugList, err := f.fetchBugList(fromDate, limit)
	if err != nil {
		return nil, err
	}

	bugs := make([]*BugRaw, 0)
	for _, bug := range bugList {
		raw := &BugRaw{}
		raw.BackendVersion = f.BackendVersion
		raw.BackendName = strings.Title(f.DSName)

		detail, err := f.fetchDetails(bug.ID)
		if err != nil {
			return nil, err
		}

		// generate UUID
		uid, err := uuid.Generate(f.Endpoint, strconv.Itoa(bug.ID))
		if err != nil {
			return nil, err
		}
		raw.UUID = uid

		raw.Origin = f.Endpoint
		raw.Tag = f.Endpoint
		raw.Product = bug.Product
		raw.BugID = bug.ID
		raw.Product = bug.Product
		raw.Component = bug.Component
		raw.Assignee.Name = bug.AssignedTo.Name
		raw.Assignee.Email = bug.AssignedTo.Email
		raw.ShortDescription = bug.ShortDescription

		deltaTS, err := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(detail.Bug.DeltaTS, " +0000"))
		if err != nil {
			return nil, err
		}
		raw.DeltaTs = deltaTS

		t, err := time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(detail.Bug.CreationTS, " +0000"))
		if err != nil {
			return nil, err
		}
		raw.CreationTS = t

		raw.Priority = detail.Bug.Priority
		raw.BugStatus = bug.BugStatus
		raw.Severity = detail.Bug.Severity
		raw.OpSys = detail.Bug.OpSys
		raw.RepPlatform = detail.Bug.RepPlatform
		raw.StatusWhiteboard = detail.Bug.StatusWhiteboard
		raw.Resolution = detail.Bug.Resolution
		raw.Reporter = detail.Bug.Reporter
		raw.AssignedTo = detail.Bug.AssignedTo
		raw.Summary = detail.Bug.Summary
		raw.LongDesc = detail.Bug.LongDesc

		count, err := f.fetchActivitiesCount(bug.ID)
		if err != nil {
			return nil, err
		}
		raw.ActivityCount = count

		raw.MetadataUpdatedOn = now
		raw.MetadataTimestamp = now
		raw.Timestamp = utils.ConvertTimeToFloat(now)
		raw.Category = Category

		t, err = time.Parse("2006-01-02 15:04:05", strings.TrimSuffix(bug.ChangedAt, " +0000"))
		if err != nil {
			return nil, err
		}
		raw.ChangedAt = t

		bugs = append(bugs, raw)
	}

	return bugs, nil
}

func (f *Fetcher) fetchBugList(fromDate time.Time, limit int) ([]*BugResponse, error) {
	url := fmt.Sprintf("%s/buglist.cgi?chfieldfrom=%s&ctype=csv&limit=%v&order=changeddate", f.Endpoint, fromDate.Format("2006-01-02+15:04:05"), limit)

	bugs, err := f.HttpClientProvider.RequestCSV(url)
	if err != nil {
		return nil, err
	}

	var bugsRes []*BugResponse
	for i, b := range bugs {
		// skip the header
		if i == 0 {
			continue
		}

		bugID, err := strconv.Atoi(b[0])
		if err != nil {
			continue
		}

		bugsRes = append(bugsRes, &BugResponse{
			ID:               bugID,
			Product:          b[1],
			Component:        b[2],
			AssignedTo:       &AssigneeResponse{Name: b[3]},
			ShortDescription: b[6],
			BugStatus:        b[4],
			ChangedAt:        b[7],
		})
	}

	return bugsRes, nil
}

func (f *Fetcher) fetchDetails(bugID int) (*BugDetailResponse, error) {
	url := fmt.Sprintf("%s/show_bug.cgi?id=%v&ctype=xml&excludefield=attachmentdata", f.Endpoint, bugID)
	status, res, err := f.HttpClientProvider.Request(url, "GET", nil, nil, nil)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("status error: %v", status)
	}

	result := &BugDetailResponse{}
	if err := xml.Unmarshal(res, result); err != nil {
		return nil, err
	}

	return result, nil
}

func (f *Fetcher) fetchActivitiesCount(bugID int) (int, error) {
	url := fmt.Sprintf("%s/show_activity.cgi?id=%v", f.Endpoint, bugID)
	status, res, err := f.HttpClientProvider.Request(url, "GET", nil, nil, nil)
	if err != nil {
		return 0, err
	}

	if status != http.StatusOK {
		return 0, fmt.Errorf("status error: %v", status)
	}

	return GetActivityLen("#bugzilla-body tr", res)
}

// HandleMapping updates bugzilla raw mapping
func (f *Fetcher) HandleMapping(index string) error {
	_, err := f.ElasticSearchProvider.CreateIndex(index, BugzillaRawMapping)
	return err
}
