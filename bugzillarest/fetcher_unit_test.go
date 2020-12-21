package bugzillarest

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/bugzillarest/mocks"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"strconv"
	"testing"
	"time"
)

func TestFetchAll(t *testing.T) {
	url := "https://bugs.dpdk.org/"
	limit := "1"
	offset := "3"
	id := 511
	from, err := time.Parse("2006-01-02 15:04:05", "2020-12-10 03:00:00")
	if err != nil {
		fmt.Println(err)
	}
	date := from.Format("2006-01-02T15:04:05")

	bugsUrl := fmt.Sprintf("%srest/bug?include_fields=_extra,_default&last_change_time=%s&limit=%s&offset=%s&", url, date, limit, offset)

	httpClientProviderMock := &mocks.HTTPClientProvider{}
	eSClientProvider := &mocks.ESClientProvider{}
	type test struct {
		name     string
		expected string
	}

	tddtest := test{
		name: "testTdd",
		expected: `[{
          "category" : "bug",
          "tag" : "https://bugs.dpdk.org/",
          "origin" : "https://bugs.dpdk.org/",
          "classified_fields_filtered" : null,
          "metadata__updated_on" : "2020-12-17T14:07:28Z",
          "updated_on" : 1.608214048e+09,
          "backend_name" : "bugzillarest",
          "metadata__timestamp" : "2020-12-20T18:32:58.68373Z",
          "timestamp" : 1.60848917868373e+09,
          "data" : {
            "status" : "CONFIRMED",
            "severity" : "enhancement",
            "creation_time" : "2020-07-20T13:17:11Z",
            "product" : "lab",
            "op_sys" : "All",
            "classification" : "Unclassified",
            "duplicates" : [ ],
            "remaining_time" : 0,
            "assigned_to" : "ci",
            "component" : "job scripts",
            "id" : 511,
            "assigned_to_detail" : {
              "real_name" : "",
              "id" : 149,
              "name" : "ci"
            },
            "deadline" : null,
            "estimated_time" : 0,
            "qa_contact" : "",
            "see_also" : [ ],
            "whiteboard" : "",
            "cc_detail" : [
              {
                "real_name" : "Brandon Lo",
                "id" : 376,
                "name" : "blo"
              },
              {
                "real_name" : "IOL-UNH",
                "id" : 138,
                "name" : "dpdklab"
              },
              {
                "real_name" : "Lincoln Lavoie",
                "id" : 199,
                "name" : "lylavoie"
              }
            ],
            "attachments" : [ ],
            "blocks" : [ ],
            "resolution" : "",
            "actual_time" : 0,
            "is_confirmed" : true,
            "cc" : [
              "blo",
              "dpdklab",
              "lylavoie"
            ],
            "is_cc_accessible" : true,
            "flags" : [ ],
            "last_change_time" : "2020-12-17T14:07:28Z",
            "dupe_of" : null,
            "priority" : "Normal",
            "target_milestone" : "---",
            "is_creator_accessible" : true,
            "tags" : [ ],
            "creator_detail" : {
              "real_name" : "Kevin Traynor",
              "id" : 134,
              "name" : "kevuzaj"
            },
            "alias" : [ ],
            "keywords" : [ ],
            "is_open" : true,
            "summary" : "Add check if performance tests are needed",
            "platform" : "All",
            "creator" : "kevuzaj",
            "history" : [
              {
                "changes" : [
                  {
                    "removed" : "UNCONFIRMED",
                    "added" : "CONFIRMED",
                    "field_name" : "status"
                  },
                  {
                    "removed" : "",
                    "added" : "blo",
                    "field_name" : "cc"
                  },
                  {
                    "removed" : "0",
                    "added" : "1",
                    "field_name" : "is_confirmed"
                  }
                ],
                "who" : "blo",
                "when" : "2020-07-20T21:07:21Z"
              },
              {
                "who" : "lylavoie",
                "when" : "2020-07-30T13:29:50Z",
                "changes" : [
                  {
                    "removed" : "Intel Lab",
                    "added" : "job scripts",
                    "field_name" : "component"
                  },
                  {
                    "removed" : "",
                    "added" : "dpdklab, lylavoie",
                    "field_name" : "cc"
                  }
                ]
              }
            ],
            "depends_on" : [ ],
            "groups" : [ ],
            "version" : "unspecified",
            "comments" : [
              {
                "is_markdown" : false,
                "time" : "2020-07-20T13:17:11Z",
                "creation_time" : "2020-07-20T13:17:11Z",
                "is_private" : false,
                "count" : 0,
                "text" : "",
                "creator" : "kevuzaj",
                "bug_id" : 511,
                "id" : 2536,
                "attachment_id" : null,
                "tags" : [ ]
              },
              {
                "is_markdown" : false,
                "time" : "2020-07-20T21:07:21Z",
                "creation_time" : "2020-07-20T21:07:21Z",
                "is_private" : false,
                "count" : 1,
                "text" : "Yeah, this seems like something we could implement. I'll start looking into this when I have the time. Thanks",
                "creator" : "blo",
                "bug_id" : 511,
                "id" : 2538,
                "attachment_id" : null,
                "tags" : [ ]
              },
              {
                "is_markdown" : false,
                "attachment_id" : null,
                "is_private" : false,
                "creation_time" : "2020-07-30T13:29:50Z",
                "count" : 2,
                "text" : "",
                "tags" : [ ],
                "id" : 2580,
                "time" : "2020-07-30T13:29:50Z",
                "bug_id" : 511,
                "creator" : "lylavoie"
              }
            ],
            "url" : ""
          },
          "version" : "0.17.0",
          "backend_version" : "0.0.1",
          "uuid" : "dcfbadc47f39165ca0d56605c7a28363bb0ffc6e"
        }]`}

	bugsDa := `{
    "bugs":[
        {
            "flags": [],
            "severity": "enhancement",
            "classification": "Unclassified",
            "is_confirmed": true,
            "summary": "Add check if performance tests are needed",
            "assigned_to": "ci",
            "last_change_time": "2020-12-17T14:07:28Z",
            "qa_contact": "",
            "see_also": [],
            "cc": [
                "blo",
                "dpdklab",
                "lylavoie"
            ],
            "duplicates": [],
            "is_open": true,
            "dupe_of": null,
            "target_milestone": "---",
            "creator_detail": {
                "id": 134,
                "real_name": "Kevin Traynor",
                "name": "kevuzaj"
            },
            "deadline": null,
            "blocks": [],
            "url": "",
            "creation_time": "2020-07-20T13:17:11Z",
            "alias": [],
            "platform": "All",
            "whiteboard": "",
            "product": "lab",
            "component": "job scripts",
            "depends_on": [],
            "assigned_to_detail": {
			  "real_name" : "",
              "id" : 149,
              "name" : "ci"
            },
            "op_sys": "All",
            "keywords": [],
            "groups": [],
            "version": "unspecified",
            "creator": "kevuzaj",
            "is_cc_accessible": true,
            "priority": "Normal",
            "id": 511,
            "resolution": "",
            "is_creator_accessible": true,
            "tags": [],
            "cc_detail": [
                {
                    "id": 376,
                    "real_name": "Brandon Lo",
                    "name": "blo"
                },
                {
                    "name": "dpdklab",
                    "id": 138,
                    "real_name": "IOL-UNH"
                },
                {
                    "real_name": "Lincoln Lavoie",
                    "id": 199,
                    "name": "lylavoie"
                }
            ],
            "status": "CONFIRMED"
        }
    ]}`

	expecRaw, err := toBugzillarestRaw(tddtest.expected)
	if err != nil {
		t.Error(err)
	}


	bugRes := []byte(bugsDa)

	var expectedRaw FetchedBugs

	err = jsoniter.Unmarshal(bugRes,expectedRaw)

	httpClientProviderMock.On("Request", bugsUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(200, bugRes, nil)

	err = jsoniter.Unmarshal(bugRes, &expectedRaw)
	if err != nil {
		t.Error(err)
	}

	// arrange comments request and result
	commUrl := fmt.Sprintf("%srest/bug/%v/%s", url, id, "comment")

	commByte := `{
    "bugs": {
        "511": {
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
                    "bug_id": 511,
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
                    "bug_id": 511,
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
                    "bug_id": 511,
                    "count": 2,
                    "creation_time": "2020-07-30T13:29:50Z"
                }
            ]
        }
    }
}`

	commentResult := map[string]map[string]map[string]Comments{}

	commRes := []byte(commByte)
	err = jsoniter.Unmarshal(commRes, &commentResult)
	if err != nil {
		t.Error(err)
	}

	httpClientProviderMock.On("Request", commUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(
		200, commRes, nil)


	// arrange history
	historyUrl := fmt.Sprintf("%srest/bug/%v/%s", url, id, "history")

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
            "id": 511,
            "alias": []
        }
    ]
}`

	hisByte := []byte(hisJs)

	var hisResult HistoryRes

	err = jsoniter.Unmarshal(hisByte, &hisResult)
	if err != nil {
		t.Error(err)
	}

	httpClientProviderMock.On("Request", historyUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(
		200, hisByte, nil)


	// arrange attachments
	attachmentUrl := fmt.Sprintf("%srest/bug/%v/%s", url, id, "attachment")

	attaSt := `{
    "bugs": {
        "511": []
    }
}`
	attaByte := []byte(attaSt)

	var attachmentResult AttachmentRes
	err = jsoniter.Unmarshal(attaByte, &attachmentResult)
	if err != nil {
		t.Error(err)
	}
	httpClientProviderMock.On("Request", attachmentUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(
		200, attaByte, nil)

	params := &Params{
		Endpoint:   "https://bugs.dpdk.org/",
		BackendVersion: "0.0.1",
	}
	srv := NewFetcher( *params, httpClientProviderMock, eSClientProvider)
	var bugs []BugzillaRestRaw
	bugs,_, err = srv.FetchAll(url, date, limit, offset, expecRaw[0].MetadataTimestamp)
	if err != nil {
		t.Error(err)
	}

	origin := fmt.Sprintf("%srest/bug", url)

	// generate UUID
	uid, err := uuid.Generate(origin, strconv.Itoa(id))
	if err != nil {
		t.Error(err)
	}
	expecRaw[0].UUID = uid

	expect := expecRaw[0].Data
	act := bugs[0].Data
	assert.NoError(t, err)
	assert.Equal(t, expecRaw[0].MetadataTimestamp, bugs[0].MetadataTimestamp)
	assert.Equal(t, expecRaw[0], bugs[0] )
	assert.Equal(t, *expecRaw[0].Data.History , *bugs[0].Data.History )
	assert.Equal(t, expecRaw[0].Data.Comments , bugs[0].Data.Comments )
	assert.Equal(t, expecRaw[0].Data.Attachments , bugs[0].Data.Attachments )
	assert.Equal(t, expecRaw[0].Data.LastChangeTime, bugs[0].Data.LastChangeTime)
	assert.Equal(t, expect.Cc, act.Cc)
	assert.Equal(t, expecRaw[0].Timestamp, bugs[0].Timestamp)
	assert.Equal(t, expecRaw[0].MetadataUpdatedOn,bugs[0].MetadataUpdatedOn)

}

func toBugzillarestRaw(b string) ([]BugzillaRestRaw, error) {
	expectedRaw := make([]BugzillaRestRaw, 0)
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}
