package uuid

import (
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

func TestToUnicode(t *testing.T) {
	/*"""Check unicode casting with several cases"""*/

	result, _ := ToUnicode("abcdefghijk")
	assert.Equal(t, result, "abcdefghijk")

	result, _ = ToUnicode("")
	assert.Equal(t, result, "")

	result, _ = ToUnicode("1234")
	assert.Equal(t, result, "1234")

	result, _ = ToUnicode("1234.4321")
	assert.Equal(t, result, "1234.4321")
}

func TestUnaccent(t *testing.T) {
	/*"""Check unicode casting removing accents"""*/

	result, _ := ToUnicode("Tomáš Čechvala")
	assert.Equal(t, result, "Tomas Cechvala")

	result, _ = ToUnicode("Santiago Dueñas")
	assert.Equal(t, result, "Santiago Duenas")

	result, _ = ToUnicode("1234")
	assert.Equal(t, result, "1234")
}

func TestGenerate(t *testing.T) {
	type testData struct {
		args   []string
		result string
	}
	// Arrange
	tests := []testData{
		{[]string{" abc ", "123"}, "18ecd81c8bb792b5c23142c89aa60d0fb2442863"},
		{[]string{"scm", "Mishal\\udcc5 Pytasz"}, "789a5559fc22f398b7e18d97601c027811773121"},
		{[]string{"1483228800.0"}, "e4c0899ba951ed06781c30eab386e4e2a9cc9f60"},
	}

	for _, test := range tests {
		// Act
		id, err := Generate(test.args...)

		// Assert
		assert.Equal(t, test.result, id)
		assert.NoError(t, err)
	}
}

func TestLegacyUUID(t *testing.T) {

	uid := "6d1d2134e4c26e5631b86f13cb79253ff2b4208a"
	origin := "https://hub.docker.com/hyperledger/explorer-db"

	ctx := &dads.Ctx{}
	ctx.LegacyUUID = true

	f := 1.605627512585879e9
	legacyUUID := dads.UUIDNonEmpty(ctx, origin, fmt.Sprintf("%f", f))

	assert.Equal(t, uid, legacyUUID, "legacy UUID is not correct")
}

func TestUUID(t *testing.T) {

	uid := "6d1d2134e4c26e5631b86f13cb79253ff2b4208a"
	origin := "https://hub.docker.com/hyperledger/explorer-db"

	f := 1.605627512585879e9

	newUUID, err := Generate(origin, fmt.Sprintf("%f", f))
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uid, newUUID, "new UUID is not correct")
}

func TestUUID2(t *testing.T) {
	/*""
	"Check whether the function returns the expected UUID"
	""*/

	result, _ := Generate("1", "2", "3", "4")
	assert.Equal(t, result, "e7b71c81f5a0723e2237f157dba81777ce7c6c21")

	result, _ = Generate("http://example.com/", "1234567")
	assert.Equal(t, result, "47509b2f0d4ffc513ca9230838a69aa841d7f055")
}

func TestEmptyValue(t *testing.T) {
	//"""Check whether a UUID cannot be generated when a given value is not a str"""

	_, err := Generate("1", "", "2", "3")
	fmt.Println(err)
	if err == nil {
		assert.Error(t, errors.New("error accepts empty"))
	}
	_, err = Generate("", "1", "2", "3")
	if err == nil {
		assert.Error(t, errors.New("error accepts empty"))
	}
	_, err = Generate("1", "2", "3", "")
	if err == nil {
		assert.Error(t, errors.New("error accepts empty"))
	}

}

func TestUnicode(t *testing.T) {
	s := []byte("\xf1")
	fmt.Println(s)
	r := utf16.Decode([]uint16{uint16(s[0])})
	fmt.Printf("%c\n", r[0])
}

func TestUnicode2(t *testing.T) {
	ss := []byte("scm::John Ca\xf1as:jcanas")

	output := ""
	for _, r := range ss {
		if !unicode.IsUpper(rune(r)) && unicode.IsPrint(rune(r)) && unicode.IsGraphic(rune(r)) && !unicode.IsSymbol(rune(r)) {
			output += string(r)
		} else {
			fmt.Printf("%v\n", r)

			u := utf16.Decode([]uint16{uint16(r)})
			newR := fmt.Sprintf("%c", u[0])
			output += newR
			fmt.Println("surrogate:", newR)
		}
	}
	fmt.Println(output)
}

func TestUnicode3(t *testing.T) {
	ss := "Max Müster"
	st := ""
	for len(ss) > 0 {
		r, size := utf8.DecodeRuneInString(ss)
		if unicode.IsSymbol(r) {
			st += string(rune(ss[0]))
		} else {
			st += string(r)
		}
		ss = ss[size:]
	}

	fmt.Printf("%s", strings.ToLower(st))
}

func TestUnicode4(t *testing.T) {
	ss := "John Ca\xf1as"
	st := ""
	for len(ss) > 0 {
		r, size := utf8.DecodeRuneInString(ss)
		if unicode.IsSymbol(r) {
			st += string(rune(ss[0]))
		} else {
			st += string(r)
		}
		ss = ss[size:]
	}

	fmt.Printf("%s", strings.ToLower(st))
}

func TestUUID3(t *testing.T) {
	//""
	//"Check whether the function returns the expected UUID"
	//""

	result, _ := GenerateIdentity("scm", "jsmith@example.com", "John Smith", "jsmith")
	assert.Equal(t, "a9b403e150dd4af8953a52a4bb841051e4b705d9", result)

	result, _ = GenerateIdentity("scm", "jsmith@example.com", "", "")
	assert.Equal(t, "3f0eb1c38060ce3bc6cb1676c8b9660e99354291", result)

	result, _ = GenerateIdentity("scm", "", "John Smith", "jsmith")
	assert.Equal(t, "a4b4591c3a2171710c157d7c278ea3cc03becf81", result)

	result, _ = GenerateIdentity("scm", "", "John Smith", "")
	assert.Equal(t, "76e3624e24aacae178d05352ad9a871dfaf81c13", result)

	result, _ = GenerateIdentity("scm", "", "", "jsmith")
	assert.Equal(t, "6e7ce2426673f8a23a72a343b1382dda84c0078b", result)

	result, err := GenerateIdentity("scm", "", "John Ca\xf1as", "jcanas")
	if err != nil {
		fmt.Println(err)
	}
	assert.Equal(t, "c88e126749ff006eb1eea25e4bb4c1c125185ed2", result)

	result, _ = GenerateIdentity("scm", "", "Max Müster", "mmuester")
	assert.Equal(t, "9a0498297d9f0b7e4baf3e6b3740d22d2257367c", result)
}

func Test_case_insensitive(t *testing.T) {
	//"""Check if same values in lower or upper case produce the same UUID"""

	uuid_a, _ := GenerateIdentity("scm", "jsmith@example.com",
		"John Smith", "jsmith")
	uuid_b, _ := GenerateIdentity("SCM", "jsmith@example.com",
		"John Smith", "jsmith")

	assert.Equal(t, uuid_a, uuid_b)

	uuid_c, _ := GenerateIdentity("scm", "jsmith@example.com",
		"john smith", "jsmith")

	assert.Equal(t, uuid_c, uuid_a)

	uuid_d, _ := GenerateIdentity("scm", "jsmith@example.com",
		"John Smith", "JSmith")

	assert.Equal(t, uuid_d, uuid_a)

	uuid_e, _ := GenerateIdentity("scm", "JSMITH@example.com",
		"John Smith", "jsmith")

	assert.Equal(t, uuid_e, uuid_a)
}

func Test_case_unaccent_name(t *testing.T) {
	//""
	//"Check if same values accent or unaccent produce the same UUID"
	//""

	accent_result, _ := GenerateIdentity("scm", "", "Max Müster", "mmuester")
	unaccent_result, _ := GenerateIdentity("scm", "", "Max Muster", "mmuester")
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, "9a0498297d9f0b7e4baf3e6b3740d22d2257367c")

	accent_result, _ = GenerateIdentity("scm", "", "Santiago Dueñas", "")
	unaccent_result, _ = GenerateIdentity("scm", "", "Santiago Duenas", "")
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, "0f1dd18839007ee8a11d02572ca0a0f4eedaf2cd")

	accent_result, _ = GenerateIdentity("scm", "", "Tomáš Čechvala", "")
	partial_accent_result, _ := GenerateIdentity("scm", "", "Tomáš Cechvala", "")
	unaccent_result, _ = GenerateIdentity("scm", "", "Tomas Cechvala", "")
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, partial_accent_result)
}

// In go the invalid unicode character raises an error and this behavior cannot be changed to ignore the error
// So, instead the invalid character is escaped to allow code to compile.
func Test_surrogate_escape(t *testing.T) {
	//"""Check if no errors are raised for invalid UTF-8 chars"""

	result, _ := GenerateIdentity("scm", "", "Mishal\\udcc5 Pytasz", "")
	assert.Equal(t, "625166bdc2c4f1a207d39eb8d25315010babd73b", result)

}

func Test_empty_source(t *testing.T) {
	//"""Check whether uuid cannot be obtained giving a empty source"""

	_, err := GenerateIdentity("", "", "Mishal\\udcc5 Pytasz", "")
	fmt.Println(err)
	assert.Error(t, err)
}

func TestSpecialCases(t *testing.T) {
	type item struct {
		name  string
		input []string
	}
	testCases := []item{
		{
			"test1",
			[]string{"a", "§"},
		},
		{
			"test2",
			[]string{"ds", "ę", "ąć∂į", "東京都"},
		},

		{
			"test3",
			[]string{"ds", "ü", "", ""},
		},
		{
			"test4",
			[]string{"ds", "", "§", ""},
		},
		{
			"test5",
			[]string{"ds", "", "", "東京都"},
		},
		{
			"test6",
			[]string{"A", "ą", "c", "ę"},
		},
		{
			"test7",
			[]string{"A", "ą", "ć", "ę"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(tt *testing.T) {
			ctx := &dads.Ctx{}
			ctx.LegacyUUID = true

			legacyUUID := dads.UUIDNonEmpty(ctx, testCase.input...)

			uid, _ := Generate(testCase.input...)
			fmt.Println(uid)
			assert.Equal(tt, legacyUUID, uid)
		})
	}
}

func Test_empty_data(t *testing.T) {
	//"""Check whether uuid cannot be obtained when identity data is empty"""

	_, err := GenerateIdentity("scm", "", "", "")
	fmt.Println(err)
	assert.Error(t, err)
}

func TestUUIDRealCases(t *testing.T) {
	type item struct {
		name   string
		result string
		input  []string
	}

	items := []item{
		{
			"hyperledger-explorer-db",
			"c935dee3207c6a4812d108e7e07929b96eb1b7b6",
			[]string{
				"https://hub.docker.com/hyperledger/explorer-db",
				fmt.Sprintf("%f", 1.605760867976719e9),
			},
		},
		{
			"aries-cloudagent",
			"534546c1d28fd3f8d91ead0de950a05881e68644",
			[]string{
				"https://hub.docker.com/hyperledger/aries-cloudagent",
				fmt.Sprintf("%f", 1.605760484366669e9),
			},
		},
		{
			"envoy",
			"0440406863089e2be2e0821e2c6f0c5337b55d81",
			[]string{
				"https://hub.docker.com/envoyproxy/ratelimit",
				fmt.Sprintf("%f", 1.605748149761947e9),
			},
		},
		{
			"fluentd-kubernetes-daemonse",
			"e4fdc078fb37b8263a7bdfe7acf4d3e0e2ff1ae1",
			[]string{
				"https://hub.docker.com/fluent/fluentd-kubernetes-daemonset",
				fmt.Sprintf("%f", 1.605748553332666e9),
			},
		},
		{
			"statsd-exporter-linux-armv7",
			"f202fe3ae3caaee3c0ad1a35aa7354efc7cb7608",
			[]string{
				"https://hub.docker.com/prom/statsd-exporter-linux-armv7",
				fmt.Sprintf("%f", 1.605754799573303e9),
			},
		},
		{
			"docker-sys-mgmt-agent-go-arm64",
			"87b8aeec37013f2a21b2e449356b4312a031ac95",
			[]string{
				"https://hub.docker.com/edgexfoundry/docker-sys-mgmt-agent-go-arm64",
				fmt.Sprintf("%f", 1.605767089298282e9),
			},
		},
		{
			"eve",
			"6e7b28becd42d4b6e6040ce8de4dcde0c1378dee",
			[]string{
				"https://hub.docker.com/lfedge/eve",
				fmt.Sprintf("%f", 1.605494468827583e9),
			},
		},
		{
			"iop-hive",
			"d6dfb2c2155021aee796bdb47b810c50a349d3d5",
			[]string{
				"https://hub.docker.com/prestodb/iop-hive",
				fmt.Sprintf("%f", 1.605783119151603e9),
			},
		},
		{
			"sdnc-dmaap-listener-image",
			"a9990c3adff4d4f84d5820eb76dbe1527239d001",
			[]string{
				"https://hub.docker.com/onap/sdnc-dmaap-listener-image",
				fmt.Sprintf("%f", 1.605770511212728e9),
			},
		},
		{
			"iop4.2-hive",
			"b46690b52cf91c72db16f180d242748a94c165ea",
			[]string{
				"https://hub.docker.com/prestodb/iop4.2-hive",
				strconv.FormatFloat(1.60578312324116e9, 'f', -1, 64),
			},
		},
		{
			"onap",
			"5cdae4ab8020f7ffa1a2a8980f0aff5af511997a",
			[]string{
				"https://hub.docker.com/onap/workflow-init",
				strconv.FormatFloat(1.605770653737379e9, 'f', -1, 64),
			},
		},
		{
			"opnfv",
			"f0fa950a40bc6abc5755b3558ef53a6a19f5a933",
			[]string{
				"https://hub.docker.com/opnfv/yardstick_aarch64",
				strconv.FormatFloat(1.605771532273906e9, 'f', -1, 64),
			},
		},
		{
			"prometheus",
			"f202fe3ae3caaee3c0ad1a35aa7354efc7cb7608",
			[]string{
				"https://hub.docker.com/prom/statsd-exporter-linux-armv7",
				strconv.FormatFloat(1.605754799573303e9, 'f', -1, 64),
			},
		},
	}

	for _, i := range items {
		t.Run(i.name, func(t *testing.T) {
			result, _ := Generate(i.input...)
			assert.Equal(t, i.result, result)
		})
	}

}
