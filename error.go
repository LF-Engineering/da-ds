package dads

import (
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

// FatalOnError displays error message (if error present) and exits program
func FatalOnError(err error) string {
	if err != nil {
		tm := time.Now()
		msg := fmt.Sprintf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		Printf("%s", msg)
		fmt.Fprintf(os.Stderr, "%s", msg)
		panic("stacktrace")
	}
	return OK
}

// Fatalf - it will call FatalOnError using fmt.Errorf with args provided
func Fatalf(f string, a ...interface{}) {
	FatalOnError(fmt.Errorf(f, a...))
}
