package bugzilla

import (
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/LF-Engineering/da-ds/utils"
	"strconv"
	"strings"
	"time"
)

// Fetcher contains datasource fetch logic
type Fetcher struct {
	DSName                string // Datasource will be used as key for ES
	HttpClientProvider    HttpClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	Endpoint              string
}

// HttpClientProvider used in connecting to remote http server
type HttpClientProvider interface {
	RequestCSV(url string) ([][]string, error)
}

// ESClientProvider used in connecting to ES Client server
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
	MaxBugs        int
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

func (f *Fetcher) fetchBugList(fromDate time.Time, limit int) ([]*BugResponse, error) {
	url := fmt.Sprintf("%s/buglist.cgi?chfieldfrom=%s&ctype=csv&limit=%v&order=changeddate", f.Endpoint, "2020-01-01%12:00:00", limit)

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
		chDate, err := time.Parse("2006-01-02 15:04:05", b[7])
		if err != nil {
			continue
		}

		bugsRes = append(bugsRes, &BugResponse{
			ID:               bugID,
			Product:          b[1],
			Component:        b[2],
			AssignedTo:       b[3],
			Status:           b[4],
			Resolution:       b[5],
			ShortDescription: b[6],
			ChangedDate:      chDate,
		})
	}

	return bugsRes, nil
}

// FetchItems ...
func (f *Fetcher) FetchItem(fromDate time.Time, limit int) (*time.Time, error) {
	bugList, err := f.fetchBugList(fromDate, limit)
	if err != nil {
		return nil, err
	}

	for _, bug := range bugList {
		raw := &BugRaw{}
		//raw.Data = repoRes
		raw.BackendName = strings.Title(f.DSName)
		raw.BackendVersion = f.BackendVersion
		raw.Category = Category
		raw.ClassifiedFieldsFiltered = nil
		now := time.Now().UTC()
		raw.Timestamp = now.UnixNano()
		raw.MetadataTimestamp = now
		raw.Origin = f.Endpoint
		raw.SearchFields = &SearchFields{Component: bug.Component, Product: bug.Product, ItemID: strconv.Itoa(bug.ID)}
		raw.Tag = f.Endpoint
		// todo: get it from details
		/*lastUpdated := raw.Data.LastUpdated
		raw.UpdatedOn = lastUpdated.UnixNano()
		raw.MetadataUpdatedOn = lastUpdated*/

		// generate UUID
		ctx := &dads.Ctx{}
		uid := dads.UUIDNonEmpty(ctx, raw.Origin, fmt.Sprintf("%v", raw.Data.FetchedOn))
		raw.UUID = uid
	}

	return raw, nil

	return nil, nil
}
