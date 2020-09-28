package dads

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"

	lib "github.com/LF-Engineering/da-ds"
)

// Copies Ctx structure
func copyContext(in *lib.Ctx) *lib.Ctx {
	out := lib.Ctx{
		DS:         in.DS,
		DSPrefix:   in.DSPrefix,
		Debug:      in.Debug,
		ST:         in.ST,
		NCPUs:      in.NCPUs,
		NCPUsScale: in.NCPUsScale,
		Enrich:     in.Enrich,
	}
	return &out
}

// Dynamically sets Ctx fields (uses map of field names into their new values)
func dynamicSetFields(t *testing.T, ctx *lib.Ctx, fields map[string]interface{}) *lib.Ctx {
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
	defaultContext := lib.Ctx{
		DS:           "ds",
		DSPrefix:     "DA_DS_",
		Debug:        0,
		ST:           false,
		NCPUs:        0,
		NCPUsScale:   1.0,
		Enrich:       false,
		RawIndex:     "",
		RichIndex:    "",
		ESURL:        "",
		ESBulkSize:   0,
		ESScrollSize: 0,
		ESScrollWait: "",
		DBHost:       "",
		DBName:       "",
		DBUser:       "",
		DBPass:       "",
		NoRaw:        false,
		RefreshAffs:  false,
		ForceFull:    false,
	}

	// Set fake data source name to "ds" which will create prefix "DA_DS_"
	lib.FatalOnError(os.Setenv("DA_DS", "ds"))

	// Test cases
	var testCases = []struct {
		name            string
		environment     map[string]string
		expectedContext *lib.Ctx
	}{
		{
			"Default values",
			map[string]string{},
			&defaultContext,
		},
		{
			"Setting debug level",
			map[string]string{"DA_DS_DEBUG": "2"},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{"Debug": 2},
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
			"Setting raw & rich index names",
			map[string]string{
				"DA_DS_RAW_INDEX":  "ds-raw",
				"DA_DS_RICH_INDEX": "ds-rich",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"RawIndex":  "ds-raw",
					"RichIndex": "ds-rich",
				},
			),
		},
		{
			"Setting ES params",
			map[string]string{
				"DA_DS_ES_URL":         "elastic.co",
				"DA_DS_ES_BULK_SIZE":   "500",
				"DA_DS_ES_SCROLL_SIZE": "600",
				"DA_DS_ES_SCROLL_WAIT": "10m",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"ESURL":        "elastic.co",
					"ESBulkSize":   500,
					"ESScrollSize": 600,
					"ESScrollWait": "10m",
				},
			),
		},
		{
			"Setting affiliation DB params",
			map[string]string{
				"DA_DS_DB_HOST": "h",
				"DA_DS_DB_NAME": "n",
				"DA_DS_DB_USER": "u",
				"DA_DS_DB_PASS": "p",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"DBHost": "h",
					"DBName": "n",
					"DBUser": "u",
					"DBPass": "p",
				},
			),
		},
		{
			"Setting re affiliate params",
			map[string]string{
				"DA_DS_NO_RAW":       "1",
				"DA_DS_REFRESH_AFFS": "y",
				"DA_DS_FORCE_FULL":   "t",
			},
			dynamicSetFields(
				t,
				copyContext(&defaultContext),
				map[string]interface{}{
					"NoRaw":       true,
					"RefreshAffs": true,
					"ForceFull":   true,
				},
			),
		},
	}

	// Execute test cases
	for index, test := range testCases {
		var gotContext lib.Ctx

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
