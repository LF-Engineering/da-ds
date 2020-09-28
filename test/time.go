package test

import (
	"fmt"
	"os"
	"time"
)

// YMDHMS - return time defined by args
func YMDHMS(in ...int) time.Time {
	m := 1
	d := 1
	h := 0
	mi := 0
	s := 0
	l := len(in)
	if l >= 2 {
		m = in[1]
	}
	if l >= 3 {
		d = in[2]
	}
	if l >= 4 {
		h = in[3]
	}
	if l >= 5 {
		mi = in[4]
	}
	if l >= 6 {
		s = in[5]
	}
	t := time.Date(
		in[0],
		time.Month(m),
		d,
		h,
		mi,
		s,
		0,
		time.UTC,
	)
	if t.Year() != in[0] || t.Month() != time.Month(m) || t.Day() != d || t.Hour() != h || t.Minute() != mi || t.Second() != s {
		fmt.Printf("Expected to set date from %v, got %v\n", in, t)
		os.Exit(1)
	}
	return t
}
