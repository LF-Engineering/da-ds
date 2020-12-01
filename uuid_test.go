package dads

import (
	"os"
	"testing"
)

func TestUUIDNonEmpty(t *testing.T) {
	// UUIDNonEmpty - at least one argument, arguments cannot be empty
	var testCases = []struct {
		args []string
	}{
		{args: []string{" "}},
		{args: []string{"Nil"}},
		{args: []string{"<Nil>"}},
		{args: []string{"nil"}},
		{args: []string{"<nil>"}},
		{args: []string{" ", " "}},
		{args: []string{"a", "b"}},
		//{args: []string{"a", "§"}},
		//{args: []string{"ds", "ę", "ąć∂į", "東京都"}},
	}
	FatalOnError(os.Setenv("DA_DS", "ds"))
	var ctx Ctx
	ctx.Init()
	for index, test := range testCases {
		ctx.LegacyUUID = false
		uuidGo := UUIDNonEmpty(&ctx, test.args...)
		ResetUUIDCache()
		ctx.LegacyUUID = true
		uuidPy := UUIDNonEmpty(&ctx, test.args...)
		if uuidGo != uuidPy {
			t.Errorf("uuid non-empty test number %d, %+v gives %s using go code and %s using py code", index+1, test.args, uuidGo, uuidPy)
		}
	}
}

func TestUUIDAffs(t *testing.T) {
	// UUIDAffs - 4 arguments (identity): source, email, name, username
	// first cannot be empty and then at least one of the remainign must be non-empty
	var testCases = []struct {
		args [4]string
	}{
		{args: [4]string{" ", "", "", "a"}},
		{args: [4]string{" ", "", "b", ""}},
		{args: [4]string{" ", "c", "", ""}},
		{args: [4]string{" ", "a", "b", "c"}},
		{args: [4]string{"git", "", "", "<nil>"}},
		{args: [4]string{"git", "<nil>", "", "<nil>"}},
		{args: [4]string{"git", "<nil>", "<nil>", "<nil>"}},
		{args: [4]string{"git", "", "", "None"}},
		{args: [4]string{"git", "None", "", "None"}},
		{args: [4]string{"git", "None", "None", "None"}},
		{args: [4]string{" ", "", "", " "}},
		//{args: [4]string{"ds", "ü", "", ""}},
		//{args: [4]string{"ds", "", "§", ""}},
		//{args: [4]string{"ds", "", "", "東京都"}},
		//{args: [4]string{"A", "ą", "c", "ę"}},
		//{args: [4]string{"A", "ą", "ć", "ę"}},
	}
	FatalOnError(os.Setenv("DA_DS", "ds"))
	var ctx Ctx
	ctx.Init()
	for index, test := range testCases {
		ctx.LegacyUUID = false
		uuidGo := UUIDAffs(&ctx, test.args[0], test.args[1], test.args[2], test.args[3])
		ResetUUIDCache()
		ctx.LegacyUUID = true
		uuidPy := UUIDAffs(&ctx, test.args[0], test.args[1], test.args[2], test.args[3])
		if uuidGo != uuidPy {
			t.Errorf("uuid affs test number %d, %+v gives %s using go code and %s using py code", index+1, test.args, uuidGo, uuidPy)
		}
	}
}
