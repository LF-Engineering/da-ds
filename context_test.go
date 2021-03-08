package dads

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	testlib "github.com/LF-Engineering/da-ds/test"
)

// Copies Ctx structure
func copyContext(in *Ctx) *Ctx {
	out := Ctx{
		DS:                in.DS,
		DSPrefix:          in.DSPrefix,
		Debug:             in.Debug,
		DebugSQL:          in.DebugSQL,
		Retry:             in.Retry,
		ST:                in.ST,
		NCPUs:             in.NCPUs,
		NCPUsScale:        in.NCPUsScale,
		Enrich:            in.Enrich,
		RawIndex:          in.RawIndex,
		RichIndex:         in.RichIndex,
		Tag:               in.Tag,
		ESURL:             in.ESURL,
		AffiliationAPIURL: in.AffiliationAPIURL,
		ESBulkSize:        in.ESBulkSize,
		ESScrollSize:      in.ESScrollSize,
		ESScrollWait:      in.ESScrollWait,
		DBHost:            in.DBHost,
		DBName:            in.DBName,
		DBUser:            in.DBUser,
		DBPass:            in.DBPass,
		DBPort:            in.DBPort,
		DBOpts:            in.DBOpts,
		DBConn:            in.DBConn,
		DBBulkSize:        in.DBBulkSize,
		NoRaw:             in.NoRaw,
		NoIdentities:      in.NoIdentities,
		NoCache:           in.NoCache,
		NoAffiliation:     in.NoAffiliation,
		DryRun:            in.DryRun,
		RefreshAffs:       in.RefreshAffs,
		OnlyIdentities:    in.OnlyIdentities,
		ForceFull:         in.ForceFull,
		LegacyUUID:        in.LegacyUUID,
		AllowFail:         in.AllowFail,
		Project:           in.Project,
		ProjectSlug:       in.ProjectSlug,
		Category:          in.Category,
		DateFrom:          in.DateFrom,
		DateTo:            in.DateTo,
		OffsetFrom:        in.OffsetFrom,
		OffsetTo:          in.OffsetTo,
		ESScrollWaitSecs:  in.ESScrollWaitSecs,
	}
	return &out
}

// Dynamically sets Ctx fields (uses map of field names into their new values)
func dynamicSetFields(t *testing.T, ctx *Ctx, fields map[string]interface{}) *Ctx {
	// Prepare mapping field name -> index
	valueOf := reflect.Indirect(reflect.ValueOf(*ctx))
	nFields := valueOf.Type().NumField()
	namesToIndex := make(map[string]int)
	for i := 0; i < nFields; i++ {
		namesToIndex[valueOf.Type().Field(i).Name] = i
	}

	// Iterate map of interface{} and set values
	elem := reflect.ValueOf(ctx).Elem()
	for fieldName, fieldValue := range fields {
		// Check if structure actually  contains this field
		fieldIndex, ok := namesToIndex[fieldName]
		if !ok {
			t.Errorf("context has no field: \"%s\"", fieldName)
			return ctx
		}
		field := elem.Field(fieldIndex)
		fieldKind := field.Kind()
		// Switch type that comes from interface
		switch interfaceValue := fieldValue.(type) {
		case int:
			// Check if types match
			if fieldKind != reflect.Int {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetInt(int64(interfaceValue))
		case float64:
			// Check if types match
			if fieldKind != reflect.Float64 {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetFloat(float64(interfaceValue))
		case bool:
			// Check if types match
			if fieldKind != reflect.Bool {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetBool(interfaceValue)
		case string:
			// Check if types match
			if fieldKind != reflect.String {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.SetString(interfaceValue)
		case time.Time:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(time.Now()) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case *time.Time:
			// Check if types match
			fieldType := field.Type()
			now := time.Now()
			if fieldType != reflect.TypeOf(&now) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case time.Duration:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(time.Now().Sub(time.Now())) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []int:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]int{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []int64:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]int64{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case []string:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf([]string{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case map[string]bool:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(map[string]bool{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case map[string]map[bool]struct{}:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(map[string]map[bool]struct{}{}) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		case *regexp.Regexp:
			// Check if types match
			fieldType := field.Type()
			if fieldType != reflect.TypeOf(regexp.MustCompile("a")) {
				t.Errorf("trying to set value %v, type %T for field \"%s\", type %v", interfaceValue, interfaceValue, fieldName, fieldKind)
				return ctx
			}
			field.Set(reflect.ValueOf(fieldValue))
		default:
			// Unknown type provided
			t.Errorf("unknown type %T for field \"%s\"", interfaceValue, fieldName)
		}
	}

	// Return dynamically updated structure
	return ctx
}

func TestInit(t *testing.T) {
	// This is the expected default struct state
	defaultContext := Ctx{
		DS:                "ds",
		DSPrefix:          "DA_DS_",
		Debug:             0,
		DebugSQL:          0,
		Retry:             5,
		ST:                false,
		NCPUs:             0,
		NCPUsScale:        1.0,
		Enrich:            false,
		RawIndex:          "",
		Tag:               "",
		RichIndex:         "",
		ESURL:             "",
		AffiliationAPIURL: "",
		ESBulkSize:        1000,
		ESScrollSize:      1000,
		ESScrollWait:      "10m",
		DBHost:            "",
		DBName:            "",
		DBUser:            "",
		DBPass:            "",
		DBPort:            "",
		DBOpts:            "",
		DBConn:            "",
		DBBulkSize:        1000,
		NoRaw:             false,
		NoIdentities:      false,
		NoCache:           false,
		NoAffiliation:     false,
		DryRun:            false,
		RefreshAffs:       false,
		OnlyIdentities:    false,
		ForceFull:         false,
		LegacyUUID:        false,
		AllowFail:         0,
		Project:           "",
		ProjectSlug:       "",
		Category:          "",
		DateFrom:          nil,
		DateTo:            nil,
		OffsetFrom:        -1.0,
		OffsetTo:          -1.0,
		ESScrollWaitSecs:  600.0,
	}

	// Set fake data source name to "ds" which will create prefix "DA_DS_"
	FatalOnError(os.Setenv("DA_DS", "ds"))

	// Test cases
	dtF := testlib.YMDHMS(2020, 9, 28, 9, 12, 17)
	dtT := testlib.YMDHMS(2021, 1, 1, 0, 0, 0)
	var testCases = []struct {
		name            string
		environment     map[string]string
		expectedContext *Ctx
	}{
		{
			"Default values",
			map[string]string{},
			&defaultContext,
		},
		{
			"Setting debug levels and retry",
			map[string]string{
				"DA_DS_DEBUG":     "2",
				"DA_DS_DEBUG_SQL": "1",
				"DA_DS_RETRY":     "3",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"Debug":    2,
					"DebugSQL": 1,
					"Retry":    3,
				},
			),
		},
		{
			"Setting negative debug level",
			map[string]string{"DA_DS_DEBUG": "-1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"Debug": -1},
			),
		},
		{
			"Setting ST (singlethreading) and NCPUs",
			map[string]string{"DA_DS_ST": "1", "DA_DS_NCPUS": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": true, "NCPUs": 1},
			),
		},
		{
			"Setting NCPUs to 2",
			map[string]string{"DA_DS_NCPUS": "2"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": false, "NCPUs": 2},
			),
		},
		{
			"Setting NCPUs to 1 should also set ST mode",
			map[string]string{"DA_DS_NCPUS": "1"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": true, "NCPUs": 1},
			),
		},
		{
			"Setting NCPUs Scale to 1.5",
			map[string]string{"DA_DS_NCPUS_SCALE": "1.5"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"ST": false, "NCPUsScale": 1.5},
			),
		},
		{
			"Setting enrich flag",
			map[string]string{"DA_DS_ENRICH": "y"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"Enrich": true},
			),
		},
		{
			"Setting raw & rich index and tag",
			map[string]string{
				"DA_DS_RAW_INDEX":  "ds-raw",
				"DA_DS_RICH_INDEX": "ds-rich",
				"DA_DS_TAG":        "tag",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"RawIndex":  "ds-raw",
					"RichIndex": "ds-rich",
					"Tag":       "tag",
				},
			),
		},
		{
			"Setting ES params",
			map[string]string{
				"DA_DS_ES_URL":         "elastic.co",
				"DA_DS_ES_BULK_SIZE":   "500",
				"DA_DS_ES_SCROLL_SIZE": "600",
				"DA_DS_ES_SCROLL_WAIT": "30m",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"ESURL":            "elastic.co",
					"ESBulkSize":       500,
					"ESScrollSize":     600,
					"ESScrollWait":     "30m",
					"ESScrollWaitSecs": 1800.0,
				},
			),
		},
		{
			"Setting affiliation DB params",
			map[string]string{
				"DA_DS_DB_HOST":      "h",
				"DA_DS_DB_NAME":      "n",
				"DA_DS_DB_USER":      "u",
				"DA_DS_DB_PASS":      "p",
				"DA_DS_DB_PORT":      "o",
				"DA_DS_DB_OPTS":      "a=1&b=2",
				"DA_DS_DB_CONN":      "c",
				"DA_DS_DB_BULK_SIZE": "500",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"DBHost":     "h",
					"DBName":     "n",
					"DBUser":     "u",
					"DBPass":     "p",
					"DBPort":     "o",
					"DBOpts":     "a=1&b=2",
					"DBConn":     "c",
					"DBBulkSize": 500,
				},
			),
		},
		{
			"Setting re affiliate params",
			map[string]string{
				"DA_DS_NO_RAW":          "1",
				"DA_DS_REFRESH_AFFS":    "y",
				"DA_DS_ONLY_IDENTITIES": "x",
				"DA_DS_FORCE_FULL":      "t",
				"DA_DS_NO_IDENTITIES":   "+",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"NoRaw":          true,
					"RefreshAffs":    true,
					"OnlyIdentities": true,
					"ForceFull":      true,
					"NoIdentities":   true,
				},
			),
		},
		{
			"Setting Affiliation API params",
			map[string]string{
				"DA_DS_AFFILIATION_API_URL": "my.url",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"AffiliationAPIURL": "my.url",
				},
			),
		},
		{
			"Setting legacy UUID mode",
			map[string]string{
				"DA_DS_LEGACY_UUID": "1",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"LegacyUUID": true,
				},
			),
		},
		{
			"Setting allow fail mode",
			map[string]string{
				"DA_DS_ALLOW_FAIL": "1",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"AllowFail": 1,
				},
			),
		},
		{
			"Setting no-cache/dry-run params",
			map[string]string{
				"DA_DS_NO_CACHE":       "1",
				"DA_DS_DRY_RUN":        "1",
				"DA_DS_NO_AFFILIATION": "1",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"NoCache":       true,
					"DryRun":        true,
					"NoAffiliation": true,
				},
			),
		},
		{
			"Setting project, project slug, category",
			map[string]string{
				"DA_DS_PROJECT":      "ONAP",
				"DA_DS_PROJECT_SLUG": "lfn/onap",
				"DA_DS_CATEGORY":     "issue",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"Project":     "ONAP",
					"ProjectSlug": "lfn/onap",
					"Category":    "issue",
				},
			),
		},
		{
			"Setting legacy project slug",
			map[string]string{
				"PROJECT_SLUG": "lfn/onap",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"ProjectSlug": "lfn/onap",
				},
			),
		},
		{
			"Setting date range",
			map[string]string{
				"DA_DS_DATE_FROM": "2020-09-28 09:12:17",
				"DA_DS_DATE_TO":   "2021",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"DateFrom": &dtF,
					"DateTo":   &dtT,
				},
			),
		},
		{
			"Setting offset range",
			map[string]string{
				"DA_DS_OFFSET_FROM": "100",
				"DA_DS_OFFSET_TO":   "200.5",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"OffsetFrom": 100.0,
					"OffsetTo":   200.5,
				},
			),
		},
		{
			"Setting parameters to falsey values",
			map[string]string{
				"DA_DS_DEBUG":           "false",
				"DA_DS_DEBUG_SQL":       "f",
				"DA_DS_RETRY":           "FALSE",
				"DA_DS_NCPUS":           "F",
				"DA_DS_NCPUS_SCALE":     "fAlSE",
				"DA_DS_ST":              "0",
				"DA_DS_ENRICH":          "  faLSE ",
				"DA_DS_NO_RAW":          "0.00 ",
				"DA_DS_NO_IDENTITIES":   "  F",
				"DA_DS_NO_CACHE":        "False ",
				"DA_DS_NO_AFFILIATION":  " \t No  \t ",
				"DA_DS_DRY_RUN":         "0.0",
				"DA_DS_REFRESH_AFFS":    " 0. ",
				"DA_DS_ONLY_IDENTITIES": " .0",
				"DA_DS_FORCE_FULL":      "",
				"DA_DS_LEGACY_UUID":     "  ",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"Debug":          0,
					"DebugSQL":       0,
					"Retry":          5,
					"NCPUs":          0,
					"NCPUsScale":     1.0,
					"ST":             false,
					"Enrich":         false,
					"NoRaw":          false,
					"NoIdentities":   false,
					"NoCache":        false,
					"NoAffiliation":  false,
					"DryRun":         false,
					"RefreshAffs":    false,
					"OnlyIdentities": false,
					"ForceFull":      false,
					"LegacyUUID":     false,
				},
			),
		},
	}

	// Execute test cases
	for index, test := range testCases {
		var gotContext Ctx

		// Remember initial environment
		currEnv := make(map[string]string)
		for key := range test.environment {
			currEnv[key] = os.Getenv(key)
		}

		// Set new environment
		for key, value := range test.environment {
			err := os.Setenv(key, value)
			if err != nil {
				t.Errorf(err.Error())
			}
		}

		// Initialize context while new environment is set
		gotContext.Init()
		// FIXME: this is a hack that should be removed, once BugZilla variable is only initialized in DS=bugzilla mode.
		gotContext.BugZilla = nil
		gotContext.PiperMail = nil
		gotContext.GoogleGroups = nil

		// Restore original environment
		for key := range test.environment {
			err := os.Setenv(key, currEnv[key])
			if err != nil {
				t.Errorf(err.Error())
			}
		}

		// Check if we got expected context
		got := fmt.Sprintf("%+v", gotContext)
		expected := fmt.Sprintf("%+v", *test.expectedContext)
		if got != expected {
			t.Errorf(
				"Test case number %d \"%s\"\nExpected:\n%+v\nGot:\n%+v\n",
				index+1, test.name, expected, got,
			)
		}
	}
}
