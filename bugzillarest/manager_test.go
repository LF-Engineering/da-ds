package bugzillarest

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/elastic"

	jsoniter "github.com/json-iterator/go"

	"github.com/stretchr/testify/mock"

	"github.com/LF-Engineering/da-ds/bugzillarest/mocks"
	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	// Arrange
	origin := "https://bugs.dpdk.org/"
	httpClientMock := &mocks.HTTPClientProvider{}
	fakeBugsHTTPRes, err := fakeBugsHTTPResult()
	if err != nil {
		t.Error(err)
	}

	httpClientMock.On("Request",
		fmt.Sprintf("%srest/bug?include_fields=_extra,_default&last_change_time=1970-01-01T00:00:00&limit=1000&offset=0&order=%s&", origin, "changeddate%20ASC"),
		"GET",
		map[string]string(nil),
		[]byte(nil),
		map[string]string(nil)).Return(200, fakeBugsHTTPRes, nil)

	fakeCommentsHTTPRes := fakeCommentsHTTPResult()
	httpClientMock.On("Request",
		mock.Anything,
		"GET",
		map[string]string{"X-Item": "comment"},
		[]byte(nil),
		map[string]string(nil)).Return(200, fakeCommentsHTTPRes, nil)

	fakeHistoryRes := fakeHistoryHTTPResult()
	httpClientMock.On("Request",
		mock.Anything,
		"GET",
		map[string]string{"X-Item": "history"},
		[]byte(nil),
		map[string]string(nil)).Return(200, fakeHistoryRes, nil)

	fakeAttachRes := fakeAttachmentHTTPResult()
	httpClientMock.On("Request",
		mock.Anything,
		"GET",
		map[string]string{"X-Item": "attachment"},
		[]byte(nil),
		map[string]string(nil)).Return(200, fakeAttachRes, nil)

	esClientMock := &mocks.ESClientProvider{}
	val := &TopHits{Hits: Hits{Hits: []NestedHits(nil)}}
	lastFetchQ := map[string]interface{}{"query": map[string]interface{}{"term": map[string]interface{}{"id": map[string]string{"value": "fetch"}}}}
	esClientMock.On("Get", "sds-data-plane-development-kit-dpdk-bugzillarest-last-action-date-cache", lastFetchQ, val).Run(func(args mock.Arguments) {

	}).Return(nil)
	lastEnrichQ := map[string]interface{}{"query": map[string]interface{}{"term": map[string]interface{}{"id": map[string]string{"value": "enrich"}}}}
	esClientMock.On("Get", "sds-data-plane-development-kit-dpdk-bugzillarest-last-action-date-cache", lastEnrichQ, val).Run(func(args mock.Arguments) {

	}).Return(nil)

	rawQuery := map[string]interface{}{"from": 0, "query": map[string]interface{}{"bool": map[string]interface{}{"must": map[string]interface{}{"range": map[string]interface{}{"data.last_change_time": map[string]interface{}{"gte": "1970-01-01T00:00:00Z"}}}}}, "size": 1000, "sort": []map[string]interface{}{{"data.last_change_time": map[string]string{"order": "asc"}}}}
	rawVal := &RawHits{Hits: NHits{Hits: []NestedRawHits(nil)}}

	fakeMapping := `{"mappings":
{"properties":
{
  "metadata__updated_on":{"type":"date"},
  "metadata__timestamp":{"type":"date"},
  "metadata__enriched_on":{"type":"date"},
  "metadata__backend_name":{"type":"date"},
  "creation_date":{"type":"date"},
  "creation_ts":{"type":"date"},
  "delta_ts":{"type":"date"},
  "main_description":{"type":"text","index":true},
  "main_description_analyzed":{"type":"text","index":true},
  "uuid":{"type":"keyword"},
  "creator_detail_id":{"type":"keyword"},
  "creator_detail_uuid":{"type":"keyword"},
  "author_id":{"type":"keyword"},
  "author_uuid":{"type":"keyword"},
  "assigned_to_detail_id":{"type":"keyword"},
  "assigned_to_detail_uuid":{"type":"keyword"},
  "assigned_to_id":{"type":"keyword"},
  "assigned_to_uuid":{"type":"keyword"},
  "priority":{"type":"keyword"},
  "severity":{"type":"keyword"},
  "status":{"type":"keyword"},
  "project":{"type":"keyword"},
  "product":{"type":"keyword"},
  "origin":{"type":"keyword"},
  "metadata__backend_version":{"type":"keyword"},
  "id": {"type":"keyword"}
}}}`

	esClientMock.On("CreateIndex", "sds-data-plane-development-kit-dpdk-bugzillarest", []byte(fakeMapping)).Run(func(args mock.Arguments) {

	}).Return(nil, nil)

	esClientMock.On("DelayOfCreateIndex", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {

	}).Return(nil)
	savedRawCount := 0
	savedEnrichedCount := 0
	esClientMock.On("BulkInsert", mock.Anything).Run(func(args mock.Arguments) {
		bulkInserted := args[0].([]elastic.BulkData)
		isFetching := strings.HasSuffix(bulkInserted[0].IndexName, "-raw")
		if isFetching {
			savedRawCount = len(bulkInserted)
			if len(bulkInserted) > 0 {
				lines := make([]interface{}, 0)
				for _, item := range bulkInserted {
					lines = append(lines, item.Data)
				}

				file, _ := jsoniter.MarshalIndent(lines[:len(bulkInserted)-2], "", " ")
				_ = ioutil.WriteFile("fetched.json", file, 0644)
			}
		} else {
			if len(bulkInserted) > 0 {
				savedEnrichedCount = len(bulkInserted)
				enrichFile, _ := jsoniter.MarshalIndent(bulkInserted, "", " ")
				_ = ioutil.WriteFile("enriched.json", enrichFile, 0644)
			}
		}

	}).Return(nil, nil)

	esClientMock.On("Get", "sds-data-plane-development-kit-dpdk-bugzillarest-raw", rawQuery, rawVal).Run(func(args mock.Arguments) {
		fetchFile, _ := ioutil.ReadFile("fetched.json")

		var hits RawHits
		var rawData []Raw
		err = jsoniter.Unmarshal(fetchFile, &rawData)
		if len(rawData) > 0 {
			for _, raw := range rawData {
				hits.Hits.Hits = append(hits.Hits.Hits, NestedRawHits{ID: "xxx", Source: raw})
			}
		}
		reflect.ValueOf(args.Get(2)).Elem().Set(reflect.ValueOf(hits))

	}).Return(nil)

	params := &MgrParams{
		EndPoint:               origin,
		ShConnStr:              "",
		FetcherBackendVersion:  "",
		EnricherBackendVersion: "",
		Fetch:                  true,
		Enrich:                 true,
		ESUrl:                  "",
		EsUser:                 "",
		EsPassword:             "",
		FromDate:               nil,
		Project:                "",
		EsIndex:                "sds-data-plane-development-kit-dpdk-bugzillarest",
		FetchSize:              1000,
		EnrichSize:             1000,
		Retries:                uint(3),
		Delay:                  time.Second * 2,
		GapURL:                 "",
		ESClientProvider:       esClientMock,
	}

	fetcher := NewFetcher(&FetcherParams{Endpoint: params.EndPoint, BackendVersion: params.FetcherBackendVersion}, httpClientMock, esClientMock)
	params.Fetcher = fetcher

	affiliationsClientMock := &mocks.AffiliationClient{}
	affiliationsClientMock.On("GetIdentityByUser", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {

	}).Return(nil, fmt.Errorf("error adding affilliation"))
	affiliationsClientMock.On("AddIdentity", mock.Anything).Run(func(args mock.Arguments) {

	}).Return(false)
	enricher := NewEnricher(&EnricherParams{BackendVersion: params.EnricherBackendVersion, Project: params.Project}, affiliationsClientMock)

	params.Enricher = enricher

	auth0ClientMock := &mocks.Auth0ClientProvider{}
	params.Auth0ClientProvider = auth0ClientMock
	mgr, err := NewManager(params)
	if err != nil {
		fmt.Println(err)
		t.Error(err)
	}

	// Act
	err = mgr.Sync()

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 5001, savedRawCount)
	assert.Equal(t, 5000, savedEnrichedCount)

}

func fakeBugsHTTPResult() ([]byte, error) {
	bugs := &FetchedBugs{}
	bugs.Bugs = make([]BugData, 0)

	for i := 0; i < 5000; i++ {
		lastChangeDate, _ := time.Parse(time.RFC3339, "2018-10-25T12:57:20Z")
		creationTime, _ := time.Parse(time.RFC3339, "2018-10-25T12:57:20Z")
		bug := BugData{
			SeeAlso:             []string{},
			Creator:             fmt.Sprintf("solal.pirelli %v", i),
			Alias:               []string{},
			Classification:      "Unclassified",
			LastChangeTime:      lastChangeDate.Add(time.Second * time.Duration(i+1)),
			AssignedToDetail:    &PersonDetail{ID: 26 + i, Name: fmt.Sprintf("anatoly.burakov %v", i), RealName: fmt.Sprintf("Anatoly Burakov %v", i)},
			Product:             "DPDK",
			QaContact:           "",
			ID:                  20 + i,
			Groups:              []string{},
			Status:              "RESOLVED",
			Platform:            "All",
			Keywords:            []string{},
			Severity:            "normal",
			IsCreatorAccessible: true,
			Component:           "core",
			CreationTime:        creationTime,
			Blocks:              []int{},
			Cc:                  []string{fmt.Sprintf("ajit.khaparde %v", i), fmt.Sprintf("anatoly.burakov %v", i)},
			CcDetail:            []PersonDetail{{ID: 114 + i, Name: fmt.Sprintf("ajit.khaparde %v", i), RealName: fmt.Sprintf("Ajit Khaparde %v", i)}},
			Version:             "unspecified",
			OpSys:               "Linux",
			IsCcAccessible:      true,
			Whiteboard:          "",
			Deadline:            nil,
			Resolution:          "FIXED",
			Flags:               []string{},
			Priority:            "Normal",
			IsOpen:              false,
			CreatorDetail:       &PersonDetail{ID: 114 + i, Name: fmt.Sprintf("ajit.khaparde %v", i), RealName: fmt.Sprintf("Ajit Khaparde %v", i)},
			TargetMilestone:     "---",
			Summary:             "Undefined behavior caused by NUMA function in eal_memory",
			DependsOn:           []int{},
			AssignedTo:          fmt.Sprintf("anatoly.burakov %v", i),
			DupeOf:              nil,
			IsConfirmed:         true,
			URL:                 "",
		}

		bugs.Bugs = append(bugs.Bugs, bug)

	}

	return jsoniter.Marshal(bugs)
}

func fakeCommentsHTTPResult() []byte {
	commByte := `{
    "bugs": {
        "20": {
            "comments": [
                {
                    "id": 2536,
                    "tags": [],
                    "text": "",
					"time": "2020-07-20T13:17:11Z",
                    "creator": "kevuzaj",
                    "is_markdown": false,
                    "is_private": false,
                    "creation_time": "2020-07-20T13:17:11Z",
                    "count": 0,
                    "bug_id": 20,
                    "attachment_id": null
                },
                {
                    "id": 2538,
                    "tags": [],
                    "text": "Yeah, this seems like something we could implement. I'll start looking into this when I have the time. Thanks",
                    "is_private": false,
                    "is_markdown": false,
                    "time": "2020-07-20T21:07:21Z",
                    "creator": "blo",
                    "count": 1,
                    "creation_time": "2020-07-20T21:07:21Z",
                    "bug_id": 20,
                    "attachment_id": null
                },
                {
                    "is_markdown": false,
                    "is_private": false,
                    "time": "2020-07-30T13:29:50Z",
                    "creator": "lylavoie",
                    "text": "",
					"tags": [],
                    "id": 2580,
                    "attachment_id": null,
                    "bug_id": 20,
                    "count": 2,
                    "creation_time": "2020-07-30T13:29:50Z"
                }
            ]
        }
    }
}`

	commRes := []byte(commByte)

	return commRes
}

func fakeHistoryHTTPResult() []byte {
	hisJs := `{
    "bugs": [
        {
            "history": [
                {
                    "when": "2020-07-20T21:07:21Z",
                    "changes": [
                        {
                            "removed": "UNCONFIRMED",
                            "added": "CONFIRMED",
                            "field_name": "status"
                        },
                        {
                            "added": "blo",
                            "field_name": "cc",
                            "removed": ""
                        },
                        {
                            "added": "1",
                            "field_name": "is_confirmed",
                            "removed": "0"
                        }
                    ],
                    "who": "blo"
                },
                {
                    "when": "2020-07-30T13:29:50Z",
                    "changes": [
                        {
                            "removed": "Intel Lab",
                            "field_name": "component",
                            "added": "job scripts"
                        },
                        {
                            "removed": "",
                            "added": "dpdklab, lylavoie",
                            "field_name": "cc"
                        }
                    ],
                    "who": "lylavoie"
                }
            ],
            "id": 20,
            "alias": []
        }
    ]
}`

	return []byte(hisJs)
}

func fakeAttachmentHTTPResult() []byte {
	attaSt := `{
    "bugs": {
        "20": []
    }
}`
	return []byte(attaSt)
}
