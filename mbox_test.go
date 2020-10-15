package dads

import (
	"testing"

	lib "github.com/LF-Engineering/da-ds"
)

func TestParseMBoxDate(t *testing.T) {
	var testCases = []struct {
		input         string
		expectedStr   string
		expectedValid bool
	}{
		{input: "Mon, 30  Lut 2019  15:15:39 +0000", expectedStr: "", expectedValid: false},
		{input: "Mon, 30  Sep 2019  15:15:39 +0000", expectedStr: "2019-09-30T15:15:39Z", expectedValid: true},
		{input: "TUE, 1 oCt 2019   15:15:39 -1200", expectedStr: "2019-10-01T15:15:39Z", expectedValid: true},
		{input: "23 Dec  2013 14:51:30 gmt", expectedStr: "2013-12-23T14:51:30Z", expectedValid: true},
		{input: "> Tue, 02 Jul 2013 02:28:30 GMT", expectedStr: "2013-07-02T02:28:30Z", expectedValid: true},
		{input: "2017-04-03 09:52:03 -0700", expectedStr: "2017-04-03T09:52:03Z", expectedValid: true},
		{input: "2017-11-19 09:52:03 -1000", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true},
		{input: ">>\t Wed,  29  Jan \t 2003 16:55\t +0000 (Pacific Standard Time)", expectedStr: "2003-01-29T16:55:00Z", expectedValid: true},
		{input: "Wed Nov  6 09:24:41 2019", expectedStr: "2019-11-06T09:24:41Z", expectedValid: true},
		{input: "> Wed Nov 06 09:24:41 19", expectedStr: "2019-11-06T09:24:41Z", expectedValid: true},
		{input: "Wed Nov 06 09:24 19", expectedStr: "2019-11-06T09:24:00Z", expectedValid: true},
		{input: "30 Sep 19\t15:15", expectedStr: "2019-09-30T15:15:00Z", expectedValid: true},
		{input: "2017-11-19T09:52:03", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true},
		{input: "2017-11-19T09:52:03Z", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true},
		{input: "2017-11-19\t09:52:03Z", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true},
	}
	// Execute test cases
	for index, test := range testCases {
		gotDt, gotValid := lib.ParseMBoxDate(test.input)
		if gotValid != test.expectedValid {
			t.Errorf("test number %d, expected '%s' validation result %v, got %v", index+1, test.input, test.expectedValid, gotValid)
		} else {
			gotStr := ""
			if gotValid {
				gotStr = lib.ToYMDTHMSZDate(gotDt)
			}
			if gotStr != test.expectedStr {
				t.Errorf("test number %d, expected '%s' to parse to '%s', got '%s'", index+1, test.input, test.expectedStr, gotStr)
			}
		}
	}
}
