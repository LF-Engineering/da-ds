package dads

import (
	"testing"
)

func TestParseDateWithTz(t *testing.T) {
	var testCases = []struct {
		input         string
		expectedStr   string
		expectedTz    float64
		expectedValid bool
	}{
		{input: "Mon, 30  Lut 2019  15:15:39 +0000", expectedStr: "", expectedValid: false, expectedTz: 0.0},
		{input: "Mon, 30  Sep 2019  15:15:39 +0300", expectedStr: "2019-09-30T12:15:39Z", expectedValid: true, expectedTz: 3.0},
		{input: "TUE, 1 oCt 2019   15:15:39 -1200", expectedStr: "2019-10-02T03:15:39Z", expectedValid: true, expectedTz: -12.0},
		{input: "TUE, 1 oCt 2019   15:15:39 -1200", expectedStr: "2019-10-02T03:15:39Z", expectedValid: true, expectedTz: -12.0},
		{input: "23 Dec  2013 14:51:30 gmt", expectedStr: "2013-12-23T14:51:30Z", expectedValid: true, expectedTz: 0.0},
		{input: "> Tue, 02 Jul 2013 02:28:30 GMT", expectedStr: "2013-07-02T02:28:30Z", expectedValid: true, expectedTz: 0.0},
		{input: "2017-04-03 09:52:03 -0700", expectedStr: "2017-04-03T16:52:03Z", expectedValid: true, expectedTz: -7.0},
		{input: "2017-11-19 09:52:03 -1000", expectedStr: "2017-11-19T19:52:03Z", expectedValid: true, expectedTz: -10.0},
		{input: ">>\t Wed,  29  Jan \t 2003 16:55\t +0200 (Pacific Standard Time)", expectedStr: "2003-01-29T14:55:00Z", expectedValid: true, expectedTz: 2.0},
		{input: "Wed Nov  6 09:24:41 2019", expectedStr: "2019-11-06T09:24:41Z", expectedValid: true, expectedTz: 0.0},
		{input: "> Wed Nov 06 09:24:41 19", expectedStr: "2019-11-06T09:24:41Z", expectedValid: true, expectedTz: 0.0},
		{input: "Wed Nov 06 09:24 19", expectedStr: "2019-11-06T09:24:00Z", expectedValid: true, expectedTz: 0.0},
		{input: "30 Sep 19\t15:15", expectedStr: "2019-09-30T15:15:00Z", expectedValid: true, expectedTz: 0.0},
		{input: "2017-11-19T09:52:03", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true, expectedTz: 0.0},
		{input: "2017-11-19T09:52:03Z", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true, expectedTz: 0.0},
		{input: "2017-11-19\t09:52:03Z", expectedStr: "2017-11-19T09:52:03Z", expectedValid: true, expectedTz: 0.0},
		{input: "Fri, 12 February 2016 14:53:49 +0900", expectedStr: "2016-02-12T05:53:49Z", expectedValid: true, expectedTz: 9.0},
		{input: "Fri, 12 February 2016 14:53:49 +0430", expectedStr: "2016-02-12T10:23:49Z", expectedValid: true, expectedTz: 4.5},
		{input: "Fri, 12 February 2016 14:53:49 +0430", expectedStr: "2016-02-12T10:23:49Z", expectedValid: true, expectedTz: 4.5},
		{input: "Wed Dec 5 06:04:38 2018 -1000", expectedStr: "2018-12-05T16:04:38Z", expectedValid: true, expectedTz: -10.0},
		{input: "Fri, 12 February 2016 14:53:49 +1130", expectedStr: "2016-02-12T03:23:49Z", expectedValid: true, expectedTz: 11.5},
		{input: "Fri, 12 February 2016 14:53:49 +1200", expectedStr: "2016-02-12T02:53:49Z", expectedValid: true, expectedTz: 12.0},
		{input: "Fri, 12 February 2016 14:53:49 -0600", expectedStr: "2016-02-12T20:53:49Z", expectedValid: true, expectedTz: -6.0},
		{input: "Fri, 12 February 2016 14:53:49 -0030", expectedStr: "2016-02-12T15:23:49Z", expectedValid: true, expectedTz: -0.5},
		{input: "Fri, 12 February 2016 14:53:49 -1030", expectedStr: "2016-02-13T01:23:49Z", expectedValid: true, expectedTz: -10.5},
		{input: "Fri, 12 February 2016 14:53:49 -1200", expectedStr: "2016-02-13T02:53:49Z", expectedValid: true, expectedTz: -12.0},
		{input: "2013-07-02 02:28:30 +0000 UTC", expectedStr: "2013-07-02T02:28:30Z", expectedValid: true, expectedTz: 0.0},
	}
	// Execute test cases
	for index, test := range testCases {
		gotDt, _, gotTz, gotValid := ParseDateWithTz(test.input)
		if gotValid != test.expectedValid {
			t.Errorf("test number %d, expected '%s' validation result %v, got %v", index+1, test.input, test.expectedValid, gotValid)
		} else {
			gotStr := ""
			if gotValid {
				gotStr = ToYMDTHMSZDate(gotDt)
			}
			if gotStr != test.expectedStr {
				t.Errorf("test number %d, expected '%s' to parse to '%s'/%.1f, got '%s'/%.1f", index+1, test.input, test.expectedStr, test.expectedTz, gotStr, gotTz)
			}
			if gotTz != test.expectedTz {
				t.Errorf("test number %d, expected '%s' to parse to '%s'/%.1f, got '%s'/%.1f", index+1, test.input, test.expectedStr, test.expectedTz, gotStr, gotTz)
			}
		}
	}
}
