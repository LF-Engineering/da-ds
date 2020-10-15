package dads

import (
	"os"
	"testing"

	lib "github.com/LF-Engineering/da-ds"
)

func TestGetThreadsNum(t *testing.T) {
	// Environment context parse
	var ctx lib.Ctx
	lib.FatalOnError(os.Setenv("DA_DS", "ds"))
	ctx.Init()

	// Get actual number of threads available
	nThreads := lib.GetThreadsNum(&ctx)

	// Set context's ST/NCPUs manually (don't need to repeat tests from context_test.go)
	var testCases = []struct {
		ST         bool
		NCPUs      int
		NCPUsScale float64
		expected   int
	}{
		{ST: false, NCPUs: 0, NCPUsScale: 1.0, expected: nThreads},
		{ST: false, NCPUs: 1, NCPUsScale: 1.0, expected: 1},
		{ST: false, NCPUs: -1, NCPUsScale: 1.0, expected: nThreads},
		{ST: false, NCPUs: 2, NCPUsScale: 1.0, expected: 2},
		{ST: true, NCPUs: 0, NCPUsScale: 1.0, expected: 1},
		{ST: true, NCPUs: 1, NCPUsScale: 1.0, expected: 1},
		{ST: true, NCPUs: -1, NCPUsScale: 1.0, expected: 1},
		{ST: true, NCPUs: 2, NCPUsScale: 1.0, expected: 2},
		{ST: true, NCPUs: nThreads + 1, NCPUsScale: 1.0, expected: nThreads},
		{ST: false, NCPUs: 0, NCPUsScale: 2.0, expected: nThreads * 2},
		{ST: false, NCPUs: 1, NCPUsScale: 2.0, expected: 1},
		{ST: false, NCPUs: -1, NCPUsScale: 2.0, expected: nThreads * 2},
		{ST: false, NCPUs: 2, NCPUsScale: 2.0, expected: 2},
		{ST: true, NCPUs: 0, NCPUsScale: 2.0, expected: 1},
		{ST: true, NCPUs: 1, NCPUsScale: 2.0, expected: 1},
		{ST: true, NCPUs: -1, NCPUsScale: 2.0, expected: 1},
		{ST: true, NCPUs: 2, NCPUsScale: 2.0, expected: 2},
		{ST: true, NCPUs: nThreads + 1, NCPUsScale: 2.0, expected: nThreads + 1},
	}
	// Execute test cases
	for index, test := range testCases {
		ctx.ST = test.ST
		ctx.NCPUs = test.NCPUs
		ctx.NCPUsScale = test.NCPUsScale
		expected := test.expected
		got := lib.GetThreadsNum(&ctx)
		if got != expected {
			t.Errorf(
				"test number %d, expected to return %d threads, got %d (default is %d on this machine)",
				index+1, expected, got, nThreads,
			)
		}
		lib.ResetThreadsNum(&ctx)
	}
}
