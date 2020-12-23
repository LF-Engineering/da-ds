package bugzillarest

import (
	"testing"
	"time"

	"github.com/LF-Engineering/da-ds/affiliation"
	"github.com/LF-Engineering/da-ds/bugzilla/mocks"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestEnrichItem(t *testing.T) {

	type test struct {
		name      string
		fetchData string
		expected  string
	}

	testDbd := test{
		"DbdTest",
		`{
          "metadata__updated_on" : "2017-09-14T09:46:38Z",
          "classified_fields_filtered" : null,
          "updated_on" : 1505382398,
          "category" : "bug",
          "backend_name" : "BugzillarestEnrich",
          "data" : {
            "history" : [
              {
                "changes" : [
                  {
                    "field_name" : "status",
                    "removed" : "UNCONFIRMED",
                    "added" : "CONFIRMED"
                  },
                  {
                    "field_name" : "assigned_to",
                    "added" : "qian.q.xu@intel.com",
                    "removed" : "dev@dpdk.org"
                  },
                  {
                    "field_name" : "is_confirmed",
                    "added" : "1",
                    "removed" : "0"
                  }
                ],
                "who" : "qian.q.xu@intel.com",
                "when" : "2017-09-14T09:42:45Z"
              },
              {
                "who" : "qian.q.xu@intel.com",
                "changes" : [
                  {
                    "field_name" : "status",
                    "added" : "RESOLVED",
                    "removed" : "CONFIRMED"
                  },
                  {
                    "field_name" : "resolution",
                    "removed" : "",
                    "added" : "FIXED"
                  }
                ],
                "when" : "2017-09-14T09:44:33Z"
              },
              {
                "changes" : [
                  {
                    "field_name" : "status",
                    "added" : "UNCONFIRMED",
                    "removed" : "RESOLVED"
                  },
                  {
                    "field_name" : "resolution",
                    "removed" : "FIXED",
                    "added" : ""
                  },
                  {
                    "field_name" : "is_confirmed",
                    "removed" : "1",
                    "added" : "0"
                  }
                ],
                "who" : "qian.q.xu@intel.com",
                "when" : "2017-09-14T09:44:55Z"
              },
              {
                "changes" : [
                  {
                    "field_name" : "status",
                    "removed" : "UNCONFIRMED",
                    "added" : "RESOLVED"
                  },
                  {
                    "field_name" : "resolution",
                    "added" : "INVALID",
                    "removed" : ""
                  }
                ],
                "who" : "qian.q.xu@intel.com",
                "when" : "2017-09-14T09:46:38Z"
              }
            ],
            "resolution" : "INVALID",
            "is_open" : false,
            "keywords" : [ ],
            "depends_on" : [ ],
            "alias" : [ ],
            "is_cc_accessible" : true,
            "duplicates" : [ ],
            "see_also" : [ ],
            "last_change_time" : "2017-09-14T09:46:38Z",
            "creator_detail" : {
              "name" : "qian.q.xu@intel.com",
              "real_name" : "Qian",
              "id" : 5
            },
            "blocks" : [ ],
            "url" : "",
            "deadline" : null,
            "priority" : "Normal",
            "remaining_time" : 0,
            "flags" : [ ],
            "groups" : [ ],
            "component" : "doc",
            "platform" : "All",
            "comments" : [
              {
                "count" : 0,
                "time" : "2017-09-14T09:41:31Z",
                "is_private" : false,
                "attachment_id" : null,
                "is_markdown" : false,
                "bug_id" : 3,
                "id" : 6,
                "tags" : [ ],
                "creation_time" : "2017-09-14T09:41:31Z",
                "creator" : "qian.q.xu@intel.com",
                "text" : "xxx"
              },
              {
                "count" : 1,
                "time" : "2017-09-14T09:44:33Z",
                "is_private" : false,
                "attachment_id" : null,
                "is_markdown" : false,
                "bug_id" : 3,
                "id" : 7,
                "tags" : [ ],
                "creation_time" : "2017-09-14T09:44:33Z",
                "creator" : "qian.q.xu@intel.com",
                "text" : "ROOT CAUSE ..."
              },
              {
                "count" : 2,
                "time" : "2017-09-14T09:45:45Z",
                "is_private" : false,
                "attachment_id" : null,
                "is_markdown" : false,
                "bug_id" : 3,
                "id" : 8,
                "tags" : [ ],
                "text" : "NOT INVALID",
                "creator" : "qian.q.xu@intel.com",
                "creation_time" : "2017-09-14T09:45:45Z"
              },
              {
                "time" : "2017-09-14T09:46:38Z",
                "count" : 3,
                "is_private" : false,
                "attachment_id" : null,
                "is_markdown" : false,
                "text" : "NOT INVALID",
                "bug_id" : 3,
                "id" : 9,
                "creation_time" : "2017-09-14T09:46:38Z",
                "creator" : "qian.q.xu@intel.com",
                "tags" : [ ]
              }
            ],
            "id" : 3,
            "op_sys" : "All",
            "severity" : "normal",
            "target_milestone" : "17.11",
            "cc" : [ ],
            "is_confirmed" : false,
            "summary" : "Test bug",
            "actual_time" : 0,
            "assigned_to" : "qian.q.xu",
            "dupe_of" : null,
            "attachments" : [ ],
            "tags" : [ ],
            "creation_time" : "2017-09-14T09:41:31Z",
            "whiteboard" : "",
            "cc_detail" : [ ],
            "status" : "RESOLVED",
            "is_creator_accessible" : true,
            "classification" : "Unclassified",
            "qa_contact" : "",
            "product" : "DPDK",
            "estimated_time" : 0,
            "creator" : "qian.q.xu@intel.com",
            "version" : "unspecified",
            "assigned_to_detail" : {
              "name" : "qian.q.xu",
              "real_name" : "Qian",
              "id" : 5
            }
          },
          "origin" : "https://bugs.dpdk.org/",
          "backend_version" : "0.10.0",
          "tag" : "https://bugs.dpdk.org/",
          "timestamp" : 1.593498534892008E9,
          "perceval_version" : "0.15.0",
          "uuid" : "9821f832cd97ddc9d735844a98667d2d2954a867",
          "metadata__timestamp" : "2020-06-30T06:28:54.892008Z",
          "search_fields" : {
            "item_id" : "3",
            "product" : "DPDK",
            "component" : "doc"
          }
        }`,
		`{
"uuid" : "9821f832cd97ddc9d735844a98667d2d2954a867",
          "assigned_to_org_name" : "Unknown",
          "is_bugzillarest_bugrest" : 1,
          "creator_detail_domain" : "",
          "creation_ts" : "2017-09-14T09:41:31",
          "status" : "RESOLVED",
          "delta_ts" : "2017-09-14T09:46:38Z",
          "main_description" : "Test bug",
          "metadata__backend_name" : "BugzillarestEnrich",
          "author_user_name" : "",
          "number_of_comments" : 0,
          "author_bot" : false,
          "author_org_name" : "Unknown",
          "timeopen_days" : 0,
          "creator_detail_user_name" : "",
          "assigned_to_detail_multi_org_names" : [
            "Unknown"
          ],
          "main_description_analyzed" : "Test bug",
          "creator_detail_org_name" : "Unknown",
          "assigned_to_detail_domain" : "",
          "origin" : "https://bugs.dpdk.org/",
          "metadata__backend_version" : "0.18",
          "creator_detail_name" : "Qian",
          "component" : "doc",
          "summary" : "Test bug",
          "assigned_to_detail_gender_acc" : 0,
          "repository_labels" : null,
          "creator_detail_bot" : false,
          "assigned_to_detail_id" : "756be8209f265138d271a6223fa0d85085e308db",
          "summary_analyzed" : "Test bug",
          "author_name" : "Qian",
          "metadata__updated_on" : "2017-09-14T09:46:38Z",
          "author_multi_org_names" : [
            "Unknown"
          ],
          "creator_detail_uuid" : "756be8209f265138d271a6223fa0d85085e308db",
          "author_gender" : "Unknown",
          "metadata__filter_raw" : null,
          "offset" : null,
          "assigned_to_detail_gender" : "Unknown",
          "author_gender_acc" : 0,
          "assigned_to" : "Qian",
          "assigned_to_detail_uuid" : "756be8209f265138d271a6223fa0d85085e308db",
          "creation_date" : "2017-09-14T09:41:31Z",
          "creator_detail_gender_acc" : 0,
          "assigned_to_detail_name" : "Qian",
          "author_domain" : "",
          "creator_detail_id" : "756be8209f265138d271a6223fa0d85085e308db",
          "time_to_last_update_days" : 0,
          "metadata__enriched_on" : "2020-07-22T07:49:33.800387Z",
          "project_ts" : 1.595404173800387e+09,
          "project" : "dpdk-common",
          "creator_detail_multi_org_names" : [
            "Unknown"
          ],
          "assigned_to_detail_org_name" : "Unknown",
          "metadata__timestamp" : "2020-06-30T06:28:54.892008Z",
          "tag" : "https://bugs.dpdk.org/",
          "changes" : 10,
          "author_uuid" : "756be8209f265138d271a6223fa0d85085e308db",
          "assigned_to_uuid" : "756be8209f265138d271a6223fa0d85085e308db",
          "comments" : 4,
          "author_id" : "756be8209f265138d271a6223fa0d85085e308db",
          "product" : "DPDK",
          "creator" : "Qian",
          "assigned_to_detail_user_name" : "",
          "changed_date" : "2017-09-14T09:46:38Z",
          "url" : "https://bugs.dpdk.org/show_bug.cgi?id=3",
          "is_open" : false,
          "id" : 3,
          "creator_detail_gender" : "Unknown",
          "assigned_to_detail_bot" : false
        }`,
	}

	t.Run(testDbd.name, func(t *testing.T) {
		raw, err := toBugRaw(testDbd.fetchData)
		if err != nil {
			t.Error(err)
		}

		expectedEnrich, err := toBugEnrich(testDbd.expected)
		if err != nil {
			t.Error(err)
		}

		identityProviderMock := &mocks.IdentityProvider{}
		unknown := "Unknown"
		zero := 0
		dd := "MontaVista Software, LLC"

		fakeAff1 := &affiliation.Identity{ID: "756be8209f265138d271a6223fa0d85085e308db",
			UUID: "756be8209f265138d271a6223fa0d85085e308db", Name: "Qian", IsBot: false,
			Domain: "", OrgName: nil, Username: "", GenderACC: &zero,
			MultiOrgNames: []string{unknown}, Gender: &unknown,
		}

		fakeAff2 := &affiliation.Identity{ID: "a89364af9818412b8c59193ca83b30dd67b20e35",
			UUID: "5d408e590365763c3927084d746071fa84dc8e52", Name: "akuster", IsBot: false,
			Domain: "gmail.com", OrgName: &dd, Username: "", GenderACC: &zero,
			MultiOrgNames: []string{"MontaVista Software, LLC"}, Gender: &unknown,
		}
		//rmultiorg1 := []string{"MontaVista Software, LLC"}
		rmultiorg2 := []string{unknown}
		identityProviderMock.On("GetIdentity", "username", "Qian").Return(fakeAff1, nil)
		identityProviderMock.On("GetIdentity", "username", "akuster808").Return(fakeAff2, nil)

		d, err := time.Parse(time.RFC3339, "2017-09-14T09:46:38Z")
		identityProviderMock.On("GetOrganizations", "756be8209f265138d271a6223fa0d85085e308db", d).Return(rmultiorg2, nil)
		identityProviderMock.On("GetOrganizations", "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056", d).Return(rmultiorg2, nil)

		// Act
		srv := NewEnricher(identityProviderMock, "0.18", "dpdk-common")

		enrich, err := srv.EnrichItem(raw, expectedEnrich.MetadataEnrichedOn.UTC())
		if err != nil {
			t.Error(err)
		}

		// Assert
		assert.Equal(t, *expectedEnrich, *enrich)
		assert.Equal(t, expectedEnrich.UUID, enrich.UUID)
		assert.Equal(t, expectedEnrich.CreationTs, enrich.CreationTs)
		assert.Equal(t, expectedEnrich.AssignedToDetailMultiOrgName, enrich.AssignedToDetailMultiOrgName)
		assert.Equal(t, expectedEnrich.CreatorDetailOrgName, enrich.CreatorDetailOrgName)
	})

}

func toBugEnrich(b string) (*BugRestEnrich, error) {
	expectedEnrich := &BugRestEnrich{}
	err := jsoniter.Unmarshal([]byte(b), expectedEnrich)
	if err != nil {
		return nil, err
	}

	return expectedEnrich, err
}

func toBugRaw(b string) (Raw, error) {
	expectedRaw := Raw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}
