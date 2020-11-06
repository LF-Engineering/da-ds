package dads

import (
	"bytes"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// MBoxDropXFields - drop fields starting with X- - to avoid ES 1000 fields limit
	MBoxDropXFields = true
)

var (
	// LowerDayNames - downcased 3 letter US day names
	LowerDayNames = map[string]struct{}{
		"mon": {},
		"tue": {},
		"wed": {},
		"thu": {},
		"fri": {},
		"sat": {},
		"sun": {},
	}
	// LowerMonthNames - map lower month names
	LowerMonthNames = map[string]string{
		"jan": "Jan",
		"feb": "Feb",
		"mar": "Mar",
		"apr": "Apr",
		"may": "May",
		"jun": "Jun",
		"jul": "Jul",
		"aug": "Aug",
		"sep": "Sep",
		"oct": "Oct",
		"nov": "Nov",
		"dec": "Dec",
	}
	// LowerFullMonthNames - map lower month names (full)
	LowerFullMonthNames = map[string]string{
		"january":   "Jan",
		"february":  "Feb",
		"march":     "Mar",
		"april":     "Apr",
		"may":       "May",
		"june":      "Jun",
		"july":      "Jul",
		"august":    "Aug",
		"september": "Sep",
		"october":   "Oct",
		"november":  "Nov",
		"decdember": "Dec",
	}
	// SpacesRE - match 1 or more space characters
	SpacesRE = regexp.MustCompile(`\s+`)
	// TZOffsetRE - time zone offset that comes after +0... +1... -0... -1...
	// Can be 3 disgits or 3 digits then whitespace and then anything
	TZOffsetRE = regexp.MustCompile(`^(\d{3})(\s+.*$|$)`)
)

// ParseMBoxMsg - parse a raw MBox message into object to be inserte dinto raw ES
func ParseMBoxMsg(ctx *Ctx, groupName string, msg []byte) (item map[string]interface{}, valid, warn bool) {
	item = make(map[string]interface{})
	raw := make(map[string][][]byte)
	defer func() {
		item["MBox-Valid"] = valid
		item["MBox-Warn"] = warn
	}()
	item["MBox-Bytes-Length"] = len(msg)
	item["MBox-Group-Name"] = groupName
	dumpMBox := func() {
		fn := groupName + "_" + strconv.Itoa(len(msg)) + ".mbox"
		_ = ioutil.WriteFile(fn, msg, 0644)
	}
	addRaw := func(k string, v []byte, replace int) {
		// replace: 0-add new item, 1-replace current, 2-replace all
		if len(raw) >= GroupsioMaxMessageProperties {
			return
		}
		a, ok := raw[k]
		if ok {
			switch replace {
			case 0:
				raw[k] = append(a, v)
			case 1:
				l := len(a)
				raw[k][l-1] = v
			case 2:
				raw[k] = [][]byte{v}
			default:
				Printf("addRaw called with an unsupported replace mode(%s,%d)\n", groupName, len(msg))
			}
			return
		}
		raw[k] = [][]byte{v}
	}
	getRaw := func(k string) (v []byte, ok bool) {
		a, ok := raw[k]
		if !ok {
			return
		}
		v = a[len(a)-1]
		return
	}
	mustGetRaw := func(k string) (v []byte) {
		a, ok := raw[k]
		if !ok {
			return
		}
		v = a[len(a)-1]
		return
	}
	lines := bytes.Split(msg, GroupsioMsgLineSeparator)
	item["MBox-N-Lines"] = len(lines)
	boundary := []byte("")
	isContinue := func(i int, line []byte) (is bool) {
		is = bytes.HasPrefix(line, []byte(" ")) || bytes.HasPrefix(line, []byte("\t"))
		return
	}
	keyRE := regexp.MustCompile(`^[\w_.-]+$`)
	getHeader := func(i int, line []byte) (key string, val []byte, ok bool) {
		sep := []byte(": ")
		ary := bytes.Split(line, sep)
		if len(ary) == 1 {
			ary := bytes.Split(line, []byte(":"))
			if len(ary) == 1 {
				return
			}
		}
		key = string(ary[0])
		if len(key) > 160 {
			return
		}
		match := keyRE.MatchString(string(key))
		if !match {
			return
		}
		val = bytes.Join(ary[1:], sep)
		ok = true
		return
	}
	getContinuation := func(i int, line []byte) (val []byte, ok bool) {
		val = bytes.TrimLeft(line, " \t")
		ok = len(val) > 0 || len(line) > 0
		return
	}
	isBoundarySep := func(i int, line []byte) (is, isEnd bool) {
		expect := []byte("--")
		expect = append(expect, boundary...)
		is = bytes.HasPrefix(line, expect)
		if is {
			isEnd = bytes.HasPrefix(line, append(expect, []byte("--")...))
		}
		return
	}
	type Body struct {
		ContentType []byte
		Properties  map[string][][]byte
		Data        []byte
	}
	bodies := []Body{}
	currContentType := []byte{}
	currProperties := make(map[string][][]byte)
	currData := []byte{}
	propertiesString := func(props map[string][][]byte) (s string) {
		s = "{"
		ks := []string{}
		for k := range props {
			ks = append(ks, k)
		}
		if len(ks) == 0 {
			s = "{}"
			return
		}
		sort.Strings(ks)
		for _, k := range ks {
			prop := props[k]
			if len(prop) == 1 {
				s += k + ":" + string(prop[0]) + " "
			} else {
				s2 := "["
				for _, p := range prop {
					s2 += string(p) + " "
				}
				s2 = s2[:len(s2)-1] + "]"
				s += k + ":" + s2 + " "
			}
		}
		s = s[:len(s)-1] + "}"
		return
	}
	boundarySep := []byte("boundary=")
	addBody := func(i int, line []byte) (added bool) {
		if len(currContentType) == 0 || len(currData) == 0 {
			return
		}
		defer func() {
			if bytes.HasSuffix(currData, []byte("\n")) {
				currData = currData[:len(currData)-1]
			}
			if ctx.Debug > 2 {
				Printf("message(%d,%s,%s): '%s'\n", len(msg), string(currContentType), propertiesString(currProperties), string(currData))
			}
			currContentType = []byte{}
			currProperties = make(map[string][][]byte)
			currData = []byte{}
		}()
		bodies = append(bodies, Body{ContentType: currContentType, Properties: currProperties, Data: currData})
		added = true
		return
	}
	savedBoundary := [][]byte{}
	savedContentType := [][]byte{}
	savedProperties := []map[string][][]byte{}
	push := func(newBoundary []byte) {
		savedBoundary = append(savedBoundary, boundary)
		savedContentType = append(savedContentType, currContentType)
		savedProperties = append(savedProperties, currProperties)
		boundary = newBoundary
	}
	pop := func() {
		n := len(savedContentType) - 1
		if n < 0 {
			Printf("%s(%d): cannot pop from an empty stack\n", groupName, len(msg))
			warn = true
			return
		}
		boundary = savedBoundary[n]
		currContentType = savedContentType[n]
		currProperties = savedProperties[n]
		savedBoundary = savedBoundary[:n]
		savedContentType = savedContentType[:n]
		savedProperties = savedProperties[:n]
	}
	possibleBodyProperties := []string{ContentType, "Content-Transfer-Encoding", "Content-Language"}
	currKey := ""
	body := false
	bodyHeadersParsed := false
	nLines := len(lines)
	nSkip := 0
	var mainMultipart *bool
	for idx, line := range lines {
		if nSkip > 0 {
			nSkip--
			continue
		}
		i := idx + 2
		if idx == 0 {
			sep := []byte("\n")
			ary := bytes.Split(line, sep)
			if len(ary) > 1 {
				line = bytes.Join(ary[1:], sep)
				if len(ary[0]) > 5 {
					data := ary[0][5:]
					spaceSep := []byte(" ")
					ary2 := bytes.Split(data, spaceSep)
					if len(ary2) == 1 {
						addRaw("MBox-From", data, 2)
					} else {
						addRaw("MBox-From", ary2[0], 2)
						addRaw("MBox-Date", bytes.Join(ary2[1:], spaceSep), 2)
					}
				}
			}
			line = ary[1]
		}
		if len(line) == 0 {
			if !body {
				contentType, ok := getRaw(ContentType)
				if !ok {
					contentType, ok = getRaw(LowerContentType)
					if !ok {
						contentType = []byte("text/plain")
						addRaw(LowerContentType, contentType, 0)
					}
					addRaw(ContentType, contentType, 0)
				}
				if bytes.Contains(contentType, boundarySep) {
					ary := bytes.Split(contentType, boundarySep)
					if len(ary) > 1 {
						ary2 := bytes.Split(ary[1], []byte(`"`))
						// Possibly even >= is enough here? - would fix possible buggy MBox data
						if len(ary2) > 2 {
							boundary = ary2[1]
						} else {
							ary2 := bytes.Split(ary[1], []byte(`;`))
							boundary = ary2[0]
						}
					}
					if len(boundary) == 0 {
						Printf("#%d cannot find multipart message boundary(%s,%d) '%s'\n", i, groupName, len(msg), string(contentType))
						warn = true
					}
					if mainMultipart == nil {
						dummy := true
						mainMultipart = &dummy
					}
				} else {
					currContentType = contentType
					for _, bodyProperty := range possibleBodyProperties {
						//propertyVal, ok := getRaw(bodyProperty)
						propertyVal, ok := raw[bodyProperty]
						if ok {
							currProperties[bodyProperty] = propertyVal
						} else {
							propertyVal, ok := raw[strings.ToLower(bodyProperty)]
							if ok {
								currProperties[bodyProperty] = propertyVal
							}
						}
					}
					if mainMultipart == nil {
						dummy := false
						mainMultipart = &dummy
					}
					bodyHeadersParsed = true
				}
				body = true
				continue
			}
			// we could possibly assume that header is parsed when empty line is met, but this is not so simple
			if bodyHeadersParsed {
				currData = append(currData, []byte("\n")...)
			}
			continue
		}
		if body {
			// We can attempt to parse buggy mbox file - they contain header data in body - only try to find boundary separator and never fail due to this
			if len(boundary) == 0 {
				key, val, ok := getHeader(i, line)
				if ok {
					lowerKey := strings.ToLower(key)
					if lowerKey == LowerContentType {
						lIdx := idx + 1
						for {
							lI := lIdx + 2
							if lIdx >= nLines {
								break
							}
							c := isContinue(lI, lines[lIdx])
							if !c {
								break
							}
							cVal, ok := getContinuation(lI, lines[lIdx])
							if ok {
								val = append(val, cVal...)
							}
							lIdx++
							nSkip++
						}
						if bytes.Contains(val, boundarySep) {
							ary := bytes.Split(val, boundarySep)
							if len(ary) > 1 {
								ary2 := bytes.Split(ary[1], []byte(`"`))
								if len(ary2) > 2 {
									boundary = ary2[1]
								} else {
									ary2 := bytes.Split(ary[1], []byte(`;`))
									boundary = ary2[0]
								}
							}
						}
					}
				}
			}
			isBoundarySep, end := isBoundarySep(i, line)
			if isBoundarySep {
				bodyHeadersParsed = false
				_ = addBody(i, line)
				if end {
					if len(savedBoundary) > 0 {
						pop()
					}
				}
				continue
			}
			if !bodyHeadersParsed {
				key, val, ok := getHeader(i, line)
				if ok {
					lIdx := idx + 1
					for {
						lI := lIdx + 2
						if lIdx >= nLines {
							break
						}
						c := isContinue(lI, lines[lIdx])
						if !c {
							break
						}
						cVal, ok := getContinuation(lI, lines[lIdx])
						if ok {
							val = append(val, cVal...)
						}
						lIdx++
						nSkip++
					}
					lowerKey := strings.ToLower(key)
					if lowerKey == LowerContentType {
						currContentType = val
						if bytes.Contains(currContentType, boundarySep) {
							ary := bytes.Split(currContentType, boundarySep)
							if len(ary) > 1 {
								ary2 := bytes.Split(ary[1], []byte(`"`))
								if len(ary2) > 2 {
									push(ary2[1])
								} else {
									ary2 := bytes.Split(ary[1], []byte(`;`))
									push(ary2[0])
								}
							}
							if len(boundary) == 0 {
								Printf("#%d cannot find multiboundary message boundary(%s,%d)\n", i, groupName, len(msg))
								warn = true
							}
						}
						continue
					}
					vals, ok := currProperties[key]
					if !ok {
						currProperties[key] = [][]byte{val}
						continue
					}
					currProperties[key] = append(vals, val)
					continue
				}
				bodyHeadersParsed = true
			}
			currData = append(currData, line...)
			continue
		}
		cont := isContinue(i, line)
		if cont {
			if currKey == "" {
				Printf("#%d no current key(%s,%d)\n", i, groupName, len(msg))
				warn = true
				break
			}
			currVal, ok := getRaw(currKey)
			if !ok {
				Printf("#%d missing %s key in %v\n", i, currKey, DumpKeys(raw))
				warn = true
				break
			}
			val, ok := getContinuation(i, line)
			if ok {
				addRaw(currKey, append(currVal, val...), 1)
				if strings.ToLower(currKey) == LowerContentType {
					addRaw(LowerContentType, mustGetRaw(currKey), 1)
				}
			}
		} else {
			key, val, ok := getHeader(i, line)
			if !ok {
				Printf("#%d incorrect header(%s,%d)\n", i, groupName, len(msg))
				warn = true
				break
			}
			addRaw(key, val, 0)
			currKey = key
			if strings.ToLower(currKey) == LowerContentType {
				addRaw(LowerContentType, mustGetRaw(currKey), 0)
			}
		}
	}
	if len(boundary) == 0 {
		_ = addBody(nLines, []byte{})
	}
	getRawStrings := func(k string) (sa []string) {
		a, ok := raw[k]
		if !ok {
			return
		}
		l := len(a)
		for i := l - 1; i >= 0; i-- {
			sa = append(sa, string(a[i]))
		}
		return
	}
	ks := []string{}
	for k := range raw {
		lk := strings.ToLower(k)
		sv := string(mustGetRaw(k))
		sa := getRawStrings(k)
		lsa := len(sa)
		// Consider skipping adding all items with lk starting with x-
		// Possible ES error due to > 1000 fields (but this seems not to be an issue with ES 7.x)
		if lsa == 1 {
			item[k] = sa[0]
		} else {
			item[k] = sa
		}
		if lk == GroupsioMessageIDField || lk == GroupsioMessageDateField {
			item[lk] = sv
			if lk != k {
				ks = append(ks, lk)
			} else {
				nk := k + "-raw"
				item[nk] = sa
				if lsa == 1 {
					item[nk] = sa[0]
				} else {
					item[nk] = sa
				}
				ks = append(ks, nk)
			}
		}
		if lk == GroupsioMessageReceivedField && lk != k {
			raw[lk] = raw[k]
		}
		ks = append(ks, k)
	}
	if ctx.Debug > 2 {
		sort.Strings(ks)
		for i, k := range ks {
			if k == GroupsioMessageReceivedField || k == GroupsioMessageIDField || k == GroupsioMessageDateField {
				Printf("#%d %s: %v\n", i+1, k, item[k])
			} else {
				a, ok := item[k].([]string)
				if ok {
					Printf("#%d %s: %d %v\n", i+1, k, len(a), a)
				} else {
					Printf("#%d %s: %v\n", i+1, k, item[k])
				}
			}
		}
		for i, body := range bodies {
			Printf("#%d: %s %s %d\n", i, string(body.ContentType), propertiesString(body.Properties), len(body.Data))
		}
	}
	_, ok := item[GroupsioMessageIDField]
	if !ok {
		Printf("%s(%d): missing Message-ID field\n", groupName, len(msg))
		dumpMBox()
		return
	}
	var (
		dt   time.Time
		dttz time.Time
		tz   float64
	)
	found := false
	mdt, ok := item[GroupsioMessageDateField]
	if !ok {
		rcvs, ok := raw[GroupsioMessageReceivedField]
		if !ok {
			Printf("%s(%d): missing Date & Received fields\n", groupName, len(msg))
		}
		type DtTz struct {
			Dt   time.Time
			DtTz time.Time
			Tz   float64
		}
		var dts []DtTz
		for _, rcv := range rcvs {
			ary := strings.Split(string(rcv), ";")
			sdt := ary[len(ary)-1]
			dt, dttz, tz, ok := ParseDateWithTz(sdt)
			if ok {
				dts = append(dts, DtTz{Dt: dt, DtTz: dttz, Tz: tz})
			}
		}
		nDts := len(dts)
		if nDts == 0 {
			Printf("%s(%d): missing Date field and cannot parse date from Received field(s)\n", groupName, len(msg))
			dumpMBox()
			return
		}
		if nDts > 1 {
			sort.Slice(dts, func(i, j int) bool { return dts[i].Dt.After(dts[j].Dt) })
		}
		dt = dts[0].Dt
		dttz = dts[0].DtTz
		tz = dts[0].Tz
		found = true
	}
	if !found {
		sdt, ok := mdt.(string)
		if !ok {
			Printf("%s(%d): non-string date field %v\n", groupName, len(msg), mdt)
		}
		dt, dttz, tz, ok = ParseDateWithTz(sdt)
		if !ok {
			Printf("%s(%d): unable to parse date from '%s'\n", groupName, len(msg), sdt)
			dumpMBox()
			return
		}
	}
	// item["Date"] = dt
	item[GroupsioMessageDateField] = dt
	item["date_tz"] = tz
	item["date_in_tz"] = dttz
	item["MBox-N-Bodies"] = len(bodies)
	bodyKeys := make(map[string]struct{})
	item["data"] = make(map[string]interface{})
	for i, body := range bodies {
		contentType := string(body.ContentType)
		ary := strings.Split(contentType, ";")
		contentType = strings.TrimSpace(ary[0])
		props := strings.Split(contentType, "/")
		for i := range props {
			props[i] = strings.TrimSpace(props[i])
		}
		sBody := BytesToStringTrunc(body.Data, GroupsioMaxMessageBodyLength, false)
		m := make(map[string]interface{})
		m["data"] = sBody
		m["content-type"] = string(body.ContentType)
		m["headers"] = make(map[string]interface{})
		for k, v := range body.Properties {
			if len(v) == 1 {
				m["headers"].(map[string]interface{})[k] = string(v[0])
			} else {
				a := []string{}
				for _, vi := range v {
					a = append(a, string(vi))
				}
				m["headers"].(map[string]interface{})[k] = a
			}
		}
		m["num"] = i
		path := []string{"data"}
		path = append(path, props...)
		key := strings.Join(path, "/")
		_, ok := bodyKeys[key]
		if !ok {
			FatalOnError(DeepSet(item, path, []interface{}{m}, true))
			bodyKeys[key] = struct{}{}
		} else {
			iface, _ := Dig(item, path, true, false)
			ifary, _ := iface.([]interface{})
			ifary = append(ifary, m)
			FatalOnError(DeepSet(item, path, ifary, true))
		}
		//Printf("#%d: %s %s %d\n", i, string(body.ContentType), propertiesString(body.Properties), len(body.Data))
	}
	if MBoxDropXFields {
		ks := []string{}
		for k := range item {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-") {
				ks = append(ks, k)
			}
		}
		for _, k := range ks {
			delete(item, k)
		}
	}
	valid = true
	return
}
