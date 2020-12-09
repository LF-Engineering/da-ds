package bugzilla

import (
	"fmt"
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
            "name" : "akuster808",
            "email" : ""
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
          "reporter" : "vvavrychuk",
          "assigned_to" : "akuster808",
          "summary" : ""
        }`,
		`{
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
"comments" : 0
        }
`,
	}

	t.Run(testYocto.name, func(tt *testing.T) {
		expectedRaw, err := toBugRaw(testYocto.fetchData)
		if err != nil {
			t.Error(err)
		}

		expectedEnrich, err := toBugEnrich(testYocto.expected)
		if err != nil {
			t.Error(err)
		}

		identityProviderMock := &mocks.IdentityProvider{}
		fakeAff := &affiliation.Identity{ID: "1", UUID: "", Name: "Ayman"}
		identityProviderMock.On("GetIdentity", "email", "ayman@mail.com").Return(fakeAff, nil)
		// Act

		srv := NewEnricher(identityProviderMock)

		enrich, er := srv.EnrichItem(expectedRaw, expectedEnrich.MetadataUpdatedOn)
		if er != nil {
			tt.Error(er)
		}
		fmt.Println("enriched:==== ")
		fmt.Println(enrich.DeltaTs.Format(time.RFC3339Nano))
		fmt.Println(expectedEnrich.DeltaTs)

		fmt.Println(enrich)

		assert.Equal(tt, expectedEnrich, *enrich)

	})

}

func toBugEnrich(b string) (*EnrichedItem, error) {
	expectedEnrich := &EnrichedItem{}
	err := jsoniter.Unmarshal([]byte(b), expectedEnrich)
	if err != nil {
		fmt.Println("errrrrrr")
		fmt.Println(err.Error())
		return nil, err
	}

	fmt.Println("222222")
	fmt.Println(expectedEnrich.DeltaTs)
	return expectedEnrich, err
}

func toBugRaw(b string) (BugRaw, error) {
	expectedRaw := BugRaw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}
