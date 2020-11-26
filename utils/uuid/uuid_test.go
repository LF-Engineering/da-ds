package uuid

import (
	"errors"
	"fmt"
	dads "github.com/LF-Engineering/da-ds"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type input struct {
	source   *string
	email    *string
	name     *string
	username *string
}

func TestToUnicode(t *testing.T) {
	t.Run("Basic Test", testToUnicode)
	t.Run("Unaccented Test", testUnaccent)
}

func TestGenerate(t *testing.T) {
	t.Run("Basic Test", testGenerate)
	t.Run("Generate vs Legacy UUID Test", testLegacyUUID)
	t.Run("UUID2 Test", testUUID2)
	t.Run("Empty value Test", testEmptyValue)
	t.Run("Special Cases Test", testSpecialCases)
	t.Run("Real Cases Test", testUUIDRealCases)
}

func TestGenerateIdentity(t *testing.T) {
	t.Run("Basic Test", testUUID3)
	t.Run("Case Insensitive Test", testCaseInsensitive)
	t.Run("Case Unaccent Test", testCaseUnaccentName)
	t.Run("Empty Source Test", testEmptySource)
	t.Run("Empty Data Test", testNoneOrEmptyData)
}

func testToUnicode(t *testing.T) {
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

func testUnaccent(t *testing.T) {
	/*"""Check unicode casting removing accents"""*/

	result, _ := ToUnicode("Tomáš Čechvala")
	assert.Equal(t, result, "Tomas Cechvala")

	result, _ = ToUnicode("Santiago Dueñas")
	assert.Equal(t, result, "Santiago Duenas")

	result, _ = ToUnicode("1234")
	assert.Equal(t, result, "1234")
}

func testGenerate(t *testing.T) {
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

func testLegacyUUID(t *testing.T) {
	uid := "6d1d2134e4c26e5631b86f13cb79253ff2b4208a"
	origin := "https://hub.docker.com/hyperledger/explorer-db"

	ctx := &dads.Ctx{}
	ctx.LegacyUUID = true

	f := 1.605627512585879e9
	legacyUUID := dads.UUIDNonEmpty(ctx, origin, fmt.Sprintf("%f", f))

	assert.Equal(t, uid, legacyUUID, "legacy UUID is not correct")

	newUUID, err := Generate(origin, fmt.Sprintf("%f", f))
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, uid, newUUID, "new UUID is not correct")
}

func testUUID2(t *testing.T) {
	/*""
	"Check whether the function returns the expected UUID"
	""*/

	result, _ := Generate("1", "2", "3", "4")
	assert.Equal(t, result, "e7b71c81f5a0723e2237f157dba81777ce7c6c21")

	result, _ = Generate("http://example.com/", "1234567")
	assert.Equal(t, result, "47509b2f0d4ffc513ca9230838a69aa841d7f055")
}

func testEmptyValue(t *testing.T) {
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

func testUUID3(t *testing.T) {
	//""
	//"Check whether the function returns the expected UUID"
	//""

	type test struct {
		name     string
		input    input
		expected string
	}

	testInput := [][]string{
		{"scm", "jsmith@example.com", "John Smith", "jsmith"},
		{"scm", "jsmith@example.com", "", ""},
		{"scm", "", "John Smith", "jsmith"},
		{"scm", "", "John Smith", ""},
		{"scm", "", "", "jsmith"},
		{"scm", "", "John Ca\xf1as", "jcanas"},
		{"scm", "", "Max Müster", "mmuester"},
	}

	tests := []test{
		{
			"test1",
			input{&testInput[0][0], &testInput[0][1], &testInput[0][2], &testInput[0][3]},
			"a9b403e150dd4af8953a52a4bb841051e4b705d9",
		},
		{
			"test2",
			input{&testInput[1][0], &testInput[1][1], &testInput[1][2], &testInput[1][3]},
			"3f0eb1c38060ce3bc6cb1676c8b9660e99354291",
		},
		{
			"test3",
			input{&testInput[2][0], &testInput[2][1], &testInput[2][2], &testInput[2][3]},
			"a4b4591c3a2171710c157d7c278ea3cc03becf81",
		},
		{
			"test4",
			input{&testInput[3][0], &testInput[3][1], &testInput[3][2], &testInput[3][3]},
			"76e3624e24aacae178d05352ad9a871dfaf81c13",
		},
		{
			"test5",
			input{&testInput[4][0], &testInput[4][1], &testInput[4][2], &testInput[4][3]},
			"6e7ce2426673f8a23a72a343b1382dda84c0078b",
		},
		{
			"test6",
			input{&testInput[5][0], &testInput[5][1], &testInput[5][2], &testInput[5][3]},
			"c88e126749ff006eb1eea25e4bb4c1c125185ed2",
		},
		{
			"test7",
			input{&testInput[6][0], &testInput[6][1], &testInput[6][2], &testInput[6][3]},
			"9a0498297d9f0b7e4baf3e6b3740d22d2257367c",
		},
	}

	for _, te := range tests {
		t.Run(te.name, func(tt *testing.T) {
			result, _ := GenerateIdentity(te.input.source, te.input.email, te.input.name, te.input.username)
			assert.Equal(t, te.expected, result)
		})
	}
}

func testCaseInsensitive(t *testing.T) {
	//"""Check if same values in lower or upper case produce the same UUID"""

	inpStr := []string{"scm", "jsmith@example.com",
		"John Smith", "jsmith"}
	inp := input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	uuid_a, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	inpStr = []string{"SCM", "jsmith@example.com",
		"John Smith", "jsmith"}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	uuid_b, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)

	assert.Equal(t, uuid_a, uuid_b)

	inpStr = []string{"scm", "jsmith@example.com",
		"john smith", "jsmith"}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	uuid_c, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)

	assert.Equal(t, uuid_c, uuid_a)

	inpStr = []string{"scm", "jsmith@example.com",
		"John Smith", "JSmith"}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	uuid_d, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)

	assert.Equal(t, uuid_d, uuid_a)

	inpStr = []string{"scm", "JSMITH@example.com",
		"John Smith", "jsmith"}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	uuid_e, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)

	assert.Equal(t, uuid_e, uuid_a)
}

func testCaseUnaccentName(t *testing.T) {
	//""
	//"Check if same values accent or unaccent produce the same UUID"
	//""

	inpStr := []string{"scm", "", "Max Müster", "mmuester"}
	inp := input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	accent_result, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	inpStr = []string{"scm", "", "Max Muster", "mmuester"}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	unaccent_result, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, "9a0498297d9f0b7e4baf3e6b3740d22d2257367c")

	inpStr = []string{"scm", "", "Santiago Dueñas", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	accent_result, _ = GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	inpStr = []string{"scm", "", "Santiago Duenas", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	unaccent_result, _ = GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, "0f1dd18839007ee8a11d02572ca0a0f4eedaf2cd")

	inpStr = []string{"scm", "", "Tomáš Čechvala", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	accent_result, _ = GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	inpStr = []string{"scm", "", "Tomáš Cechvala", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	partial_accent_result, _ := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	inpStr = []string{"scm", "", "Tomas Cechvala", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	unaccent_result, _ = GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	assert.Equal(t, accent_result, unaccent_result)
	assert.Equal(t, accent_result, partial_accent_result)
}

//// In go the invalid unicode character raises an error and this behavior cannot be changed to ignore the error
//// So, instead the invalid character is escaped to allow code to compile.
//func Test_surrogate_escape(t *testing.T) {
//	//"""Check if no errors are raised for invalid UTF-8 chars"""
//
//	result, _ := GenerateIdentity("scm", "", "Mishal\\udcc5 Pytasz", "")
//	assert.Equal(t, "625166bdc2c4f1a207d39eb8d25315010babd73b", result)
//
//}

func testEmptySource(t *testing.T) {
	//"""Check whether uuid cannot be obtained giving a empty source"""

	inpStr := []string{"", "", "Mishal\\udcc5 Pytasz", ""}
	inp := input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	_, err := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	fmt.Println(err)
	assert.Error(t, err)
}

func testSpecialCases(t *testing.T) {
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

func testNoneOrEmptyData(t *testing.T) {
	//"""Check whether uuid cannot be obtained when identity data is empty"""

	inpStr := []string{"scm", ""}
	inp := input{&inpStr[0], nil, &inpStr[1], nil}
	_, err := GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	fmt.Println(err)
	assert.Error(t, err)

	inpStr = []string{"scm", "", "", ""}
	inp = input{&inpStr[0], &inpStr[1], &inpStr[2], &inpStr[3]}
	_, err = GenerateIdentity(inp.source, inp.email, inp.name, inp.username)
	fmt.Println(err)
	assert.Error(t, err)
}

func testUUIDRealCases(t *testing.T) {
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
