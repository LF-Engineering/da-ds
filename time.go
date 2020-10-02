package dads

import (
	"fmt"
	"strings"
	"time"
)

// ProgressInfo display info about progress: i/n if current time >= last + period
// If displayed info, update last
func ProgressInfo(i, n int, start time.Time, last *time.Time, period time.Duration, msg string) {
	now := time.Now()
	if last.Add(period).Before(now) {
		perc := 0.0
		if n > 0 {
			perc = (float64(i) * 100.0) / float64(n)
		}
		eta := start
		if i > 0 && n > 0 {
			etaNs := float64(now.Sub(start).Nanoseconds()) * (float64(n) / float64(i))
			etaDuration := time.Duration(etaNs) * time.Nanosecond
			eta = start.Add(etaDuration)
			if msg != "" {
				Printf("%d/%d (%.3f%%), ETA: %v: %s\n", i, n, perc, eta, msg)
			} else {
				Printf("%d/%d (%.3f%%), ETA: %v\n", i, n, perc, eta)
			}
		} else {
			Printf("%s\n", msg)
		}
		*last = now
	}
}

// ToYMDDate - return time formatted as YYYYMMDD
func ToYMDDate(dt time.Time) string {
	return fmt.Sprintf("%04d%02d%02d", dt.Year(), dt.Month(), dt.Day())
}

// ToYMDHMSDate - return time formatted as YYYY-MM-DD HH:MI:SS
func ToYMDHMSDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

// ToESDate - return time formatted as YYYY-MM-DD HH:MI:SS YYYY-MM-DDTHH:MI:SS.uuuuuu+00:00
func ToESDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%06.0f+00:00", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), float64(dt.Nanosecond())/1.0e3)
}

// TimeParseAny - attempts to parse time from string YYYY-MM-DD HH:MI:SS
// Skipping parts from right until only YYYY id left
func TimeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02 15",
		"2006-01-02",
		"2006-01",
		"2006",
	}
	for _, format := range formats {
		t, e := time.Parse(format, dtStr)
		if e == nil {
			return t, e
		}
	}
	e := fmt.Errorf("Error:\nCannot parse date: '%v'\n", dtStr)
	return time.Now(), e
}

// TimeParseES - parse datetime in ElasticSearch output format
func TimeParseES(dtStr string) (time.Time, error) {
	ary := strings.Split(dtStr, "+")
	return time.Parse("2006-01-02T15:04:05.000", ary[0])
}
