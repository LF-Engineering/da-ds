package bugzillarest

import (
	"fmt"
	"github.com/LF-Engineering/da-ds/bugzillarest/mocks"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestFetchAll(t *testing.T) {
	url := "https://bugs.dpdk.org/rest/bug"
	limit := "1"
	offset := "3"
	id := 601
	from, err := time.Parse("2006-01-02 15:04:05", "2020-12-10 03:00:00")
	if err != nil {
		fmt.Println(err)
	}
	date := from.Format("2006-01-02T15:04:05")

	bugsUrl := fmt.Sprintf("%s?include_fields=_extra,_default&last_change_time=%s&limit=%s&offset=%s&", url, date, limit, offset)

	httpClientProviderMock := &mocks.HTTPClientProvider{}

	type test struct {
		name     string
		expected string
	}

	tddtest := test{
		name: "testTdd",
		expected: `[{
          "category" : "bug",
          "search_fields" : {
            "item_id" : "511",
            "product" : "lab",
            "component" : "job scripts"
          },
          "tag" : "https://bugs.dpdk.org/",
          "origin" : "https://bugs.dpdk.org/",
          "classified_fields_filtered" : null,
          "metadata__updated_on" : "2020-07-30T13:29:50+00:00",
          "updated_on" : 1596115790,
          "backend_name" : "BugzillaREST",
          "metadata__timestamp" : "2020-07-30T18:30:11.666836+00:00",
          "timestamp" : 1.596133811666836E9,
          "data" : {
            "status" : "CONFIRMED",
            "severity" : "enhancement",
            "creation_time" : "2020-07-20T13:17:11Z",
            "product" : "lab",
            "op_sys" : "All",
            "classification" : "Unclassified",
            "duplicates" : [ ],
            "remaining_time" : 0,
            "assigned_to" : "ci@dpdk.org",
            "component" : "job scripts",
            "id" : 511,
            "assigned_to_detail" : {
              "real_name" : "",
              "id" : 149,
              "name" : "ci@dpdk.org"
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
                "name" : "blo@iol.unh.edu"
              },
              {
                "real_name" : "IOL-UNH",
                "id" : 138,
                "name" : "dpdklab@iol.unh.edu"
              },
              {
                "real_name" : "Lincoln Lavoie",
                "id" : 199,
                "name" : "lylavoie@iol.unh.edu"
              }
            ],
            "attachments" : [ ],
            "blocks" : [ ],
            "resolution" : "",
            "actual_time" : 0,
            "is_confirmed" : true,
            "cc" : [
              "blo@iol.unh.edu",
              "dpdklab@iol.unh.edu",
              "lylavoie@iol.unh.edu"
            ],
            "is_cc_accessible" : true,
            "flags" : [ ],
            "last_change_time" : "2020-07-30T13:29:50Z",
            "dupe_of" : null,
            "priority" : "Normal",
            "target_milestone" : "---",
            "is_creator_accessible" : true,
            "tags" : [ ],
            "creator_detail" : {
              "real_name" : "Kevin Traynor",
              "id" : 134,
              "name" : "kevuzaj@gmail.com"
            },
            "alias" : [ ],
            "keywords" : [ ],
            "is_open" : true,
            "summary" : "Add check if performance tests are needed",
            "platform" : "All",
            "creator" : "kevuzaj@gmail.com",
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
                    "added" : "blo@iol.unh.edu",
                    "field_name" : "cc"
                  },
                  {
                    "removed" : "0",
                    "added" : "1",
                    "field_name" : "is_confirmed"
                  }
                ],
                "who" : "blo@iol.unh.edu",
                "when" : "2020-07-20T21:07:21Z"
              },
              {
                "who" : "lylavoie@iol.unh.edu",
                "when" : "2020-07-30T13:29:50Z",
                "changes" : [
                  {
                    "removed" : "Intel Lab",
                    "added" : "job scripts",
                    "field_name" : "component"
                  },
                  {
                    "removed" : "",
                    "added" : "dpdklab@iol.unh.edu, lylavoie@iol.unh.edu",
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
                "text" : """I submitted a patch for DPDK Unit Test [1], that only changed one file in app/test/.

I noticed that UNH is running performance testing on these patches, when really it is not required. It may also run performance testing when there is a change to a .rst only etc.

Sometimes it may be difficult to know if something will impact performance, but for these very obvious cases at least maybe some rule could be added so that performance testing is not run. 

Of course, it is not an issue in receiving the mails with the results, but perhaps it would free up some lab resources, hence the suggestion.

[1]
http://patchwork.dpdk.org/patch/74486/""",
                "creator" : "kevuzaj@gmail.com",
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
                "creator" : "blo@iol.unh.edu",
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
                "text" : """Idea: Develop a script to determine the "category" of the patch, which can be used by the infrastructure to appropriately test the path.  

Categories could include:
1. Documentation - would be tested for syntax, etc
2. Tooling Changes - scripts, dev tools
3. Base Code - changes to DPDK "main" - requires compile / performance
4. Unit Tests -""",
                "tags" : [ ],
                "id" : 2580,
                "time" : "2020-07-30T13:29:50Z",
                "bug_id" : 511,
                "creator" : "lylavoie@iol.unh.edu"
              }
            ],
            "url" : ""
          },
          "perceval_version" : "0.17.0",
          "backend_version" : "0.10.0",
          "uuid" : "dcfbadc47f39165ca0d56605c7a28363bb0ffc6e"
        }]`}
	fmt.Println(tddtest.name)
	bugsDa := `[
        {
            "flags": [],
            "severity": "enhancement",
            "classification": "Unclassified",
            "is_confirmed": true,
            "summary": "Add check if performance tests are needed",
            "assigned_to": "thomas",
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
                "name": "thomas",
                "real_name": "Thomas Monjalon",
                "id": 2
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
    ]`
	expectedRaw, err := toBugzillarestRaw(bugsDa)
	if err != nil {
		t.Error(err)
	}

	data, err := jsoniter.Marshal(expectedRaw)
	if err != nil {
		t.Error(err)
	}

	httpClientProviderMock.On("Request", bugsUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(200, data, nil)

	var result FetchedBugs
	err = jsoniter.Unmarshal(data, &result.Bugs)
	if err != nil {
		t.Error(err)
	}

	// arrange comments request and result
	commentsUrl := fmt.Sprintf("%s/%v/%s", url, id, "comment")

	commByte := `{
    "bugs": {
        "511": {
            "comments": [
                {
                    "id": 2536,
                    "tags": [],
                    "text": "I submitted a patch for DPDK Unit Test [1], that only changed one file in app/test/.\n\nI noticed that UNH is running performance testing on these patches, when really it is not required. It may also run performance testing when there is a change to a .rst only etc.\n\nSometimes it may be difficult to know if something will impact performance, but for these very obvious cases at least maybe some rule could be added so that performance testing is not run. \n\nOf course, it is not an issue in receiving the mails with the results, but perhaps it would free up some lab resources, hence the suggestion.\n\n[1]\nhttp://patchwork.dpdk.org/patch/74486/",
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
                    "text": "Idea: Develop a script to determine the \"category\" of the patch, which can be used by the infrastructure to appropriately test the path.  \n\nCategories could include:\n1. Documentation - would be tested for syntax, etc\n2. Tooling Changes - scripts, dev tools\n3. Base Code - changes to DPDK \"main\" - requires compile / performance\n4. Unit Tests -",
                    "tags": [],
                    "id": 2580,
                    "attachment_id": null,
                    "bug_id": 511,
                    "count": 2,
                    "creation_time": "2020-07-30T13:29:50Z"
                },
                {
                    "text": "https://mails.dpdk.org/archives/ci/2020-December/000902.html",
                    "tags": [],
                    "id": 3020,
                    "creator": "lylavoie",
                    "time": "2020-12-17T14:07:03Z",
                    "is_markdown": false,
                    "is_private": false,
                    "bug_id": 511,
                    "creation_time": "2020-12-17T14:07:03Z",
                    "count": 3,
                    "attachment_id": null
                }
            ]
        }
    }
}`

	//commRes, err := jsoniter.Marshal(commByte)
	//if err != nil {
	//	t.Error(err)
	//}

	commentResult := map[string]map[string]map[string]Comments{}

	commRes := []byte(commByte)
	err = jsoniter.Unmarshal(commRes, &commentResult )
	if err != nil {
		t.Error(err)
	}

	httpClientProviderMock.On("Request", commentsUrl, "GET",
		mock.Anything, mock.Anything, mock.Anything).Return(
			200, commRes, nil)


	//comments := commentResult["bugs"][strconv.Itoa(id)]["comments"]

	// arrange history
	historyUrl := fmt.Sprintf("%s/%v/%s", url, id, "history")

	hisJs := `
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
                            "added": "blo@iol.unh.edu",
                            "field_name": "cc",
                            "removed": ""
                        },
                        {
                            "added": "1",
                            "field_name": "is_confirmed",
                            "removed": "0"
                        }
                    ],
                    "who": "blo@iol.unh.edu"
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
                            "added": "dpdklab@iol.unh.edu, lylavoie@iol.unh.edu",
                            "field_name": "cc"
                        }
                    ],
                    "who": "lylavoie@iol.unh.edu"
                },
                {
                    "when": "2020-12-17T14:07:28Z",
                    "changes": [
                        {
                            "removed": "ci@dpdk.org",
                            "field_name": "assigned_to",
                            "added": "thomas@monjalon.net"
                        }
                    ],
                    "who": "lylavoie@iol.unh.edu"
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



	//history := hisResult.Bugs[0].History

	// arrange attachments
	attachmentUrl := fmt.Sprintf("%s/%v/%s", url, id, "attachment")

	attaSt := `{
    "bugs": {
        "511": []
    },
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

	//attachment := attachmentResult.Bugs[strconv.Itoa(id)]

	srv := NewFetcher(httpClientProviderMock)
	var bugs []BugzillaRestRaw
	bugs, err = srv.FetchAll(url, date, limit, offset)
	fmt.Println(bugs)

	assert.NoError(t, err)

}

func toBugzillarestRaw(b string) ([]BugzillaRestRaw, error) {
	expectedRaw := make([]BugzillaRestRaw, 0)
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}
