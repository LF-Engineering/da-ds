package dads

import (
	"fmt"
	"strconv"
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

// ToESDate - return time formatted as YYYY-MM-DDTHH:MI:SS.uuuuuu+00:00
func ToESDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%06.0f+00:00", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), float64(dt.Nanosecond())/1.0e3)
}

// ToYMDTHMSZDate - return time formatted as YYYY-MM-DDTHH:MI:SSZ
func ToYMDTHMSZDate(dt time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

// TimeParseAny - attempts to parse time from string YYYY-MM-DD HH:MI:SS
// Skipping parts from right until only YYYY id left
func TimeParseAny(dtStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05.000",
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

// TimeParseESSec - parse datetime in ElasticSearch output format
func TimeParseESSec(dtStr string) (time.Time, error) {
	ary := strings.Split(dtStr, "+")
	ary2 := strings.Split(ary[0], ".")
	return time.Parse("2006-01-02T15:04:05", ary2[0])
}

// TimeParseES - parse datetime in ElasticSearch output format
func TimeParseES(dtStr string) (time.Time, error) {
	dtStr = strings.TrimSpace(strings.Replace(dtStr, "Z", "", -1))
	ary := strings.Split(dtStr, "+")
	ary2 := strings.Split(ary[0], ".")
	var s string
	if len(ary2) == 1 {
		s = ary2[0] + ".000"
	} else {
		if len(ary2[1]) > 3 {
			ary2[1] = ary2[1][:3]
		}
		s = strings.Join(ary2, ".")
	}
	return time.Parse("2006-01-02T15:04:05.000", s)
}

// TimeParseInterfaceString - parse interface{} -> string -> time.Time
func TimeParseInterfaceString(date interface{}) (dt time.Time, err error) {
	sDate, ok := date.(string)
	if !ok {
		err = fmt.Errorf("%+v %T is not a string", date, date)
		return
	}
	dt, err = TimeParseES(sDate)
	return
}

// ParseDateWithTz - try to parse mbox date
func ParseDateWithTz(indt string) (dt, dtInTz time.Time, off float64, valid bool) {
	defer func() {
		if !valid {
			return
		}
		dtInTz = dt
		ary := strings.Split(indt, "+0")
		if len(ary) > 1 {
			last := ary[len(ary)-1]
			if TZOffsetRE.MatchString(last) {
				digs := TZOffsetRE.ReplaceAllString(last, `$1`)
				offH, _ := strconv.Atoi(digs[:1])
				offM, _ := strconv.Atoi(digs[1:])
				off = float64(offH) + float64(offM)/60.0
				dt = dt.Add(time.Minute * time.Duration(off*-60))
				return
			}
		}
		ary = strings.Split(indt, "+1")
		if len(ary) > 1 {
			last := ary[len(ary)-1]
			if TZOffsetRE.MatchString(last) {
				digs := TZOffsetRE.ReplaceAllString(last, `$1`)
				offH, _ := strconv.Atoi(digs[:1])
				offM, _ := strconv.Atoi(digs[1:])
				off = float64(10+offH) + float64(offM)/60.0
				dt = dt.Add(time.Minute * time.Duration(off*-60))
				return
			}
		}
		ary = strings.Split(indt, "-0")
		if len(ary) > 1 {
			last := ary[len(ary)-1]
			if TZOffsetRE.MatchString(last) {
				digs := TZOffsetRE.ReplaceAllString(last, `$1`)
				offH, _ := strconv.Atoi(digs[:1])
				offM, _ := strconv.Atoi(digs[1:])
				off = -(float64(offH) + float64(offM)/60.0)
				dt = dt.Add(time.Minute * time.Duration(off*-60))
				return
			}
		}
		ary = strings.Split(indt, "-1")
		if len(ary) > 1 {
			last := ary[len(ary)-1]
			if TZOffsetRE.MatchString(last) {
				digs := TZOffsetRE.ReplaceAllString(last, `$1`)
				offH, _ := strconv.Atoi(digs[:1])
				offM, _ := strconv.Atoi(digs[1:])
				off = -(float64(10+offH) + float64(offM)/60.0)
				dt = dt.Add(time.Minute * time.Duration(off*-60))
				return
			}
		}
	}()
	sdt := indt
	// https://www.broobles.com/eml2mbox/mbox.html
	// but the real world is not that simple
	for _, r := range []string{">", ",", ")", "("} {
		sdt = strings.Replace(sdt, r, "", -1)
	}
	for _, split := range []string{"+0", "+1", "."} {
		ary := strings.Split(sdt, split)
		sdt = ary[0]
	}
	for _, split := range []string{"-0", "-1"} {
		ary := strings.Split(sdt, split)
		lAry := len(ary)
		if lAry > 1 {
			_, err := strconv.Atoi(ary[lAry-1])
			if err == nil {
				sdt = strings.Join(ary[:lAry-1], split)
			}
		}
	}
	sdt = SpacesRE.ReplaceAllString(sdt, " ")
	sdt = strings.ToLower(strings.TrimSpace(sdt))
	ary := strings.Split(sdt, " ")
	day := ary[0]
	if len(day) > 3 {
		day = day[:3]
	}
	_, ok := LowerDayNames[day]
	if ok {
		sdt = strings.Join(ary[1:], " ")
	}
	sdt = strings.TrimSpace(sdt)
	for lm, m := range LowerFullMonthNames {
		sdt = strings.Replace(sdt, lm, m, -1)
	}
	for lm, m := range LowerMonthNames {
		sdt = strings.Replace(sdt, lm, m, -1)
	}
	ary = strings.Split(sdt, " ")
	if len(ary) > 4 {
		sdt = strings.Join(ary[:4], " ")
	}
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02t15:04:05",
		"2006-01-02 15:04:05z",
		"2006-01-02t15:04:05z",
		"2 Jan 2006 15:04:05",
		"02 Jan 2006 15:04:05",
		"2 Jan 06 15:04:05",
		"02 Jan 06 15:04:05",
		"2 Jan 2006 15:04",
		"02 Jan 2006 15:04",
		"2 Jan 06 15:04",
		"02 Jan 06 15:04",
		"Jan 2 15:04:05 2006",
		"Jan 02 15:04:05 2006",
		"Jan 2 15:04:05 06",
		"Jan 02 15:04:05 06",
		"Jan 2 15:04 2006",
		"Jan 02 15:04 2006",
		"Jan 2 15:04 06",
		"Jan 02 15:04 06",
	}
	var (
		err  error
		errs []error
	)
	for _, format := range formats {
		dt, err = time.Parse(format, sdt)
		if err == nil {
			// Printf("Parsed %v\n", dt)
			valid = true
			return
		}
		errs = append(errs, err)
	}
	Printf("ParseDateWithTz: errors: %+v\n", errs)
	Printf("ParseDateWithTz: '%s' -> '%s', day: %s\n", indt, sdt, day)
	return
}
