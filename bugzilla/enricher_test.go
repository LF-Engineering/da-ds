package bugzilla

import (
	"testing"

	"github.com/LF-Engineering/da-ds/bugzilla/mocks"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestEnrichItem(t *testing.T) {

	type test struct {
		name      string
		fetchData string
		expected  string
	}

	testYocto := test{
		"YoctoTest",
		`{
		  "backend_version" : "0.0.1",
          "backend_name" : "Bugzilla",
          "uuid" : "5d61b34bcdf735a83d8b1c6762890b79f053c491",
          "bug_id" : 14136,
          "origin" : "https://bugzilla.yoctoproject.org",
          "tag" : "https://bugzilla.yoctoproject.org",
          "product" : "OE-Core",
          "component" : "oe-core other",
          "Assignee" : {
            "name" : "akuster",
            "username" : "akuster808"
          },
          "short_description" : "If u-boot defconfig is incomplete, 'bitbake u-boot -c configure' hangs and eats all memory",
          "bug_status" : "ACCEPTED",
          "metadata__updated_on" : "2020-12-07T14:38:23.895437Z",
          "metadata__timestamp" : "2020-12-06T17:16:36.178198Z",
          "timestamp" : 1.607351903895437E9,
          "category" : "bug",
          "creation_ts" : "2020-11-03T05:31:00Z",
          "priority" : "Medium+",
          "severity" : "normal",
          "op_sys" : "Multiple",
          "changed_at" : "2020-12-07T09:24:51Z",
          "activity_count" : 13,
          "delta_ts" : "2020-11-13T05:31:00Z",
          "keywords" : null,
          "rep_platform" : "PC",
          "status_whiteboard" : "",
          "resolution" : "",
          "reporter" : {"name":"vvavrychuk", "username":"vvavrychuk"},
          "assigned_to" : "akuster808",
          "summary" : ""
        }`,
		`{
          "metadata__backend_name" : "BugzillaEnrich",
          "metadata__backend_version" : "0.18",
          "labels" : null,
		  "bug_id" : 14136,
          "priority" : "Medium+",
		  "category":"bug",
          "changes" : 13,
          "metadata__timestamp" : "2020-12-06T17:16:36.178198Z",
          "assigned" : "akuster808",
		  "reporter_name":"vvavrychuk",
		  "author_name":"vvavrychuk",
          "tag" : "https://bugzilla.yoctoproject.org",
          "product" : "OE-Core",
          "resolution_days" : 10.00,
          "project_ts" : 1607275057,
          "creation_date" : "2020-11-03T05:31:00Z",
          "metadata__updated_on" : "2020-12-07T14:38:23.895437Z",
          "metadata__version" : "0.80.0",
          "severity" : "normal",
          "metadata__enriched_on" : "2020-12-06T17:16:36.178198Z",
          "project" : "yocto",
          "changed_date" : "2020-12-07T09:24:51Z",
          "metadata__filter_raw" : null,
          "origin" : "https://bugzilla.yoctoproject.org",
          "op_sys" : "Multiple",
          "platform" : "PC",
          "uuid" : "5d61b34bcdf735a83d8b1c6762890b79f053c491",
          "timeopen_days" : 0,
          "main_description" : "If u-boot defconfig is incomplete, 'bitbake u-boot -c configure' hangs and eats all memory",
          "main_description_analyzed" : "If u-boot defconfig is incomplete, 'bitbake u-boot -c configure' hangs and eats all memory",
		  "is_bugzilla_bug" : 1,
          "component" : "oe-core other",
          "url" : "https://bugzilla.yoctoproject.org/show_bug.cgi?id=14136",
          "creation_date" : "2020-11-03T05:31:00Z",
          "delta_ts" : "2020-11-13T05:31:00Z",
          "status" : "ACCEPTED",
          "comments" : 0,
         "assigned_to_uuid" : "5d408e590365763c3927084d746071fa84dc8e52",
          "reporter_multi_org_names" : [
            "Unknown"
          ],
          "assigned_to_name" : "akuster",
          "author_domain" : "gmail.com",
          "author_org_name" : "Unknown",
          "reporter_domain" : "gmail.com",
          "reporter_uuid" : "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056",
          "assigned_to_bot" : false,
          "reporter_name" : "Vasyl",
          "author_id" : "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056",
          "assigned_to_user_name" : "",
          "reporter_org_name" : "Unknown",
          "author_uuid" : "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056",
          "assigned_to_gender" : "Unknown",
          "reporter_gender_acc" : 0,
          "assigned_to_gender_acc" : 0,
          "author_user_name" : "",
          "assigned_to_multi_org_names" : [
            "MontaVista Software, LLC"
          ],
          "assigned_to_id" : "a89364af9818412b8c59193ca83b30dd67b20e35",
          "author_name" : "Vasyl",
          "assigned_to_domain" : "gmail.com",
          "author_gender_acc" : 0,
          "author_bot" : false,
          "reporter_bot" : false,
          "reporter_id" : "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056",
          "reporter_gender" : "Unknown",
          "author_multi_org_names" : [
            "Unknown"
          ],
          "assigned_to_org_name" : "MontaVista Software, LLC",
          "author_gender" : "Unknown",
          "reporter_user_name" : ""

        }
`,
	}

	t.Run(testYocto.name, func(t *testing.T) {
		raw, err := toBugRaw(testYocto.fetchData)
		if err != nil {
			t.Error(err)
		}

		expectedEnrich, err := toBugEnrich(testYocto.expected)
		if err != nil {
			t.Error(err)
		}

		affProviderMock := &mocks.Affiliation{}
		unknown := "Unknown"
		zero := int64(0)

		aff1UUID := "50ffba4dfbedc6dc4390fc8bde7aeec0a7191056"
		fakeAff1 := &libAffiliations.AffIdentity{ID: &aff1UUID,
			UUID: &aff1UUID, Name: "Vasyl", IsBot: &zero,
			Domain: "gmail.com", OrgName: nil, Username: "", GenderACC: &zero,
			MultiOrgNames: []string{}, Gender: &unknown,
		}

		dd := "MontaVista Software, LLC"
		aff2UUID := "a89364af9818412b8c59193ca83b30dd67b20e35"
		aff2ID := "5d408e590365763c3927084d746071fa84dc8e52"
		fakeAff2 := &libAffiliations.AffIdentity{ID: &aff2UUID,
			UUID: &aff2ID, Name: "akuster", IsBot: &zero,
			Domain: "gmail.com", OrgName: &dd, Username: "", GenderACC: &zero,
			MultiOrgNames: []string{"MontaVista Software, LLC"}, Gender: &unknown,
		}
		affProviderMock.On("GetIdentityByUser", "username", "vvavrychuk").Return(fakeAff1, nil)
		affProviderMock.On("GetIdentityByUser", "username", "akuster808").Return(fakeAff2, nil)

		// Act
		srv := NewEnricher("0.18", "yocto", affProviderMock)

		enrich, err := srv.EnrichItem(raw, expectedEnrich.MetadataEnrichedOn.UTC())
		if err != nil {
			t.Error(err)
		}

		// Assert
		assert.Equal(t, *expectedEnrich, *enrich)
		assert.Equal(t, expectedEnrich.UUID, enrich.UUID)
		assert.Equal(t, expectedEnrich.MetadataBackendName, enrich.MetadataBackendName)
		assert.Equal(t, expectedEnrich.AssignedToMultiOrgName, enrich.AssignedToMultiOrgName)

	})

}

func toBugEnrich(b string) (*BugEnrich, error) {
	expectedEnrich := &BugEnrich{}
	err := jsoniter.Unmarshal([]byte(b), expectedEnrich)
	if err != nil {
		return nil, err
	}

	return expectedEnrich, err
}

func toBugRaw(b string) (BugRaw, error) {
	expectedRaw := BugRaw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}
