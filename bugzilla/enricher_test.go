package bugzilla

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"testing"
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
          "metadata__timestamp" : "2020-12-07T14:38:23.895437Z",
          "timestamp" : 1.607351903895437E9,
          "category" : "bug",
          "creation_ts" : "2020-11-27T02:48:00Z",
          "priority" : "Medium+",
          "severity" : "normal",
          "op_sys" : "Multiple",
          "changed_at" : "2020-12-07 08:18:54",
          "activity_count" : 5,
          "delta_ts" : "0001-01-01T00:00:00Z",
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
          "changes" : 13,
          "metadata__timestamp" : "2020-12-06T17:16:36.178198Z",
          "assigned" : "akuster",
          "tag" : "https://bugzilla.yoctoproject.org",
          "product" : "OE-Core",
          "main_description_analyzed" : "If u-boot defconfig is incomplete, 'bitbake u-boot -c configure' hangs and eats all memory",
          "resolution_days" : 6.78,
          "project_ts" : 1607275057,
          "creation_date" : "2020-11-27T02:48:00Z",
          "metadata__updated_on" : "2020-12-03T21:26:39Z",
          "metadata__version" : "0.80.0",
          "metadata__backend_name" : "BugzillaEnrich",
          "severity" : "normal",
          "metadata__enriched_on" : "2020-12-06T17:16:39.517209",
          "project" : "yocto",
          "changeddate_date" : "2020-12-03T21:26:39+00:00",
          "metadata__filter_raw" : null,
          "origin" : "https://bugzilla.yoctoproject.org",
          "op_sys" : "Multiple",
          "platform" : "PC",
          "uuid" : "5d61b34bcdf735a83d8b1c6762890b79f053c491",
          "timeopen_days" : 9.6,
          "main_description" : "If u-boot defconfig is incomplete, 'bitbake u-boot -c configure' hangs and eats all memory",
          "is_bugzilla_bug" : 1,
          "component" : "oe-core other",
          "url" : "https://bugzilla.yoctoproject.org/show_bug.cgi?id=14136",
          "creation_date" : "2020-11-27T02:48:00",
          "delta_ts" : "2020-12-03T21:26:39",
          "status" : "ACCEPTED",
        }`,
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


		// Act
		enrich, er := EnrichItem(expectedRaw, expectedEnrich.MetadataUpdatedOn)
		if er != nil {
			tt.Error(er)
		}
		fmt.Println("enriched:==== ")
		fmt.Println(enrich)

		assert.Equal(tt, expectedEnrich, *enrich)

	})

}

func toBugEnrich(b string) (EnItem, error) {
	expectedEnrich := EnItem{}
	err := jsoniter.Unmarshal([]byte(b), &expectedEnrich)
	return expectedEnrich, err
}

func toBugRaw(b string) (BugRaw, error) {
	expectedRaw := BugRaw{}
	err := jsoniter.Unmarshal([]byte(b), &expectedRaw)
	return expectedRaw, err
}