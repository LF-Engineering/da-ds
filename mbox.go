package dads

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

// ParseMBoxMsg - parse a raw MBox message into object to be inserte dinto raw ES
func ParseMBoxMsg(ctx *Ctx, groupName string, msg []byte) (item map[string]interface{}, valid bool, err error) {
	// FIXME
	_ = ioutil.WriteFile(fmt.Sprintf("%s_%d.mbox", groupName, len(msg)), msg, 0644)
	item = make(map[string]interface{})
	raw := make(map[string][]byte)
	lines := bytes.Split(msg, GroupsioMsgLineSeparator)
	if ctx.Debug > 1 {
		//Printf("%d bytes, %d lines\n", len(msg), len(lines))
	}
	boundary := []byte("")
	isContinue := func(i int, line []byte) (is bool) {
		if ctx.Debug > 1 {
			defer func() {
				//Printf("#%d isContinue '%s' --> %v\n", i, string(line), is)
			}()
		}
		is = bytes.HasPrefix(line, []byte(" ")) || bytes.HasPrefix(line, []byte("\t"))
		return
	}
	keyRE := regexp.MustCompile(`^[\w-]+$`)
	getHeader := func(i int, line []byte) (key string, val []byte, ok bool) {
		if ctx.Debug > 1 {
			defer func() {
				//Printf("#%d getHeader '%s' --> %s, %s, %v\n", i, line, key, string(val), ok)
			}()
		}
		sep := []byte(": ")
		ary := bytes.Split(line, sep)
		if len(ary) == 1 {
			ary2 := bytes.Split(line, []byte(":"))
			if len(ary2) == 2 {
				key = string(ary2[0])
				val = ary2[1]
				ok = true
			}
			return
		}
		key = string(ary[0])
		if len(key) > 160 {
			return
		}
		match := keyRE.MatchString(string(key))
		//Printf("(%d,%v,%s)\n", len(key), match, string(key))
		if !match {
			//Printf("invalid key: %s\n", key)
			return
		}
		val = bytes.Join(ary[1:], sep)
		ok = true
		return
	}
	getContinuation := func(i int, line []byte) (val []byte, ok bool) {
		if ctx.Debug > 1 {
			defer func() {
				//Printf("#%d getContinuation '%s' --> %s, %v\n", i, line, string(val), ok)
			}()
		}
		val = bytes.TrimLeft(line, " \t")
		ok = len(val) > 0 || len(line) > 0
		return
	}
	isBoundarySep := func(i int, line []byte) (is, isEnd bool) {
		if ctx.Debug > 1 {
			defer func() {
				//Printf("#%d isBoundarySep '%s' --> %v,%v\n", i, string(line), is, isEnd)
			}()
		}
		expect := []byte("--")
		expect = append(expect, boundary...)
		is = bytes.HasPrefix(line, expect)
		if is {
			//Printf("HasPrefix %s\n", string(append(expect, []byte("--")...)))
			isEnd = bytes.HasPrefix(line, append(expect, []byte("--")...))
		}
		return
	}
	type Body struct {
		ContentType []byte
		Properties  map[string][]byte
		Data        []byte
	}
	bodies := []Body{}
	currContentType := []byte{}
	currProperties := make(map[string][]byte)
	currData := []byte{}
	propertiesString := func(props map[string][]byte) (s string) {
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
			s += k + ":" + string(props[k]) + " "
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
			if bytes.Contains(currData, []byte(boundarySep)) {
				Printf("should not contain boundary= marker(%d): message(%s,%s): '%s'\n", len(msg), string(currContentType), propertiesString(currProperties), string(currData))
			}
			if ctx.Debug > 2 {
				//Printf("#%d addBody '%s' --> (%s,%s,%d,%v)\n", i, string(line), string(currContentType), currProperties, len(currData), added)
				//Printf("#%d addBody '%s' --> (%s,%s,%d,%v)\n", i, string(line), string(currContentType), DumpKeys(currProperties), len(currData), added)
				Printf("message(%d,%s,%s): '%s'\n", len(msg), string(currContentType), propertiesString(currProperties), string(currData))
			}
			currContentType = []byte{}
			currProperties = make(map[string][]byte)
			currData = []byte{}
		}()
		bodies = append(bodies, Body{ContentType: currContentType, Properties: currProperties, Data: currData})
		added = true
		return
	}
	savedBoundary := [][]byte{}
	savedContentType := [][]byte{}
	savedProperties := []map[string][]byte{}
	push := func(newBoundary []byte) {
		savedBoundary = append(savedBoundary, boundary)
		savedContentType = append(savedContentType, currContentType)
		savedProperties = append(savedProperties, currProperties)
		boundary = newBoundary
	}
	pop := func() {
		n := len(savedContentType) - 1
		if n < 0 {
			Printf("cannot pop from an empty stack\n")
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
			//Printf("skipping line, remain %d\n", nSkip)
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
						raw["Mbox-From"] = data
					} else {
						raw["Mbox-From"] = ary2[0]
						raw["Mbox-Date"] = bytes.Join(ary2[1:], spaceSep)
					}
				}
			}
			//Printf("line0: %s\nline1: %s\n", string(ary[0]), string(ary[1]))
			line = ary[1]
		}
		if len(line) == 0 {
			if !body {
				contentType, ok := raw[ContentType]
				if !ok {
					contentType, ok = raw[LowerContentType]
					if !ok {
						contentType = []byte("text/plain")
						raw[LowerContentType] = contentType
					}
					raw[ContentType] = contentType
				}
				//Printf("#%d empty: mode change, current content type: %s\n", i, contentType)
				if bytes.Contains(contentType, boundarySep) {
					ary := bytes.Split(contentType, boundarySep)
					if len(ary) > 1 {
						ary2 := bytes.Split(ary[1], []byte(`"`))
						if len(ary2) > 2 {
							boundary = ary2[1]
						} else {
							ary2 := bytes.Split(ary[1], []byte(`;`))
							boundary = ary2[0]
						}
					}
					if len(boundary) == 0 {
						Printf("#%d cannot find multipart message boundary(%d) '%s'\n", i, len(msg), string(contentType))
						break
					}
					if mainMultipart == nil {
						dummy := true
						mainMultipart = &dummy
					}
				} else {
					currContentType = contentType
					for _, bodyProperty := range possibleBodyProperties {
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
					//Printf("#%d no-multipart email, content type: %s, transfer encoding: %v\n", i, currContentType, currProperties)
					bodyHeadersParsed = true
				}
				body = true
				continue
			}
			//Printf("#%d empty line in body mode, headers parsed %v\n", i, bodyHeadersParsed)
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
					//Printf("#%d got header data in single body mode\n", i)
					if lowerKey == LowerContentType {
						//Printf("#%d got content-type data in single body mode\n", i)
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
							if mainMultipart != nil && !*mainMultipart {
								//Printf("#%d got a new boundary setting in single-body mode(%d)\n", i, len(msg))
							}
						}
					}
				}
			}
			isBoundarySep, end := isBoundarySep(i, line)
			//Printf("#%d %v,%v,%v\n", i, bodyHeadersParsed, isBoundarySep, end)
			if isBoundarySep {
				bodyHeadersParsed = false
				_ = addBody(i, line)
				if end {
					if len(savedBoundary) > 0 {
						// Printf("restore saved: %s -> %s, %s -> %s\n", string(savedBoundary), string(boundary), string(savedContentType), string(currContentType))
						pop()
					}
				}
				continue
			}
			if !bodyHeadersParsed {
				key, val, ok := getHeader(i, line)
				//Printf("#%d getHeader -> %v,%s,%s\n", i, ok, key, string(val))
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
						//Printf("added header %s continuation: %s --> %s\n", key, string(cVal), string(val))
					}
					lowerKey := strings.ToLower(key)
					if lowerKey == LowerContentType {
						//Printf("%s -> %s\n", currContentType, val)
						currContentType = val
						if bytes.Contains(currContentType, boundarySep) {
							ary := bytes.Split(currContentType, boundarySep)
							if len(ary) > 1 {
								ary2 := bytes.Split(ary[1], []byte(`"`))
								if len(ary2) > 2 {
									// Printf("save multi boundary: %s, %s\n", string(boundary), string(currContentType))
									push(ary2[1])
								} else {
									ary2 := bytes.Split(ary[1], []byte(`;`))
									push(ary2[0])
								}
							}
							if len(boundary) == 0 {
								Printf("#%d cannot find multiboundary message boundary(%d)\n", i, len(msg))
								break
							}
						}
						continue
					}
					// Printf("assigning %s %s\n", key, string(val))
					currProperties[key] = val
					continue
				}
				//Printf("#%d setting body headers passed\n", i)
				bodyHeadersParsed = true
			}
			//Printf("#%d body line, boundary %s\n", i, string(boundary))
			currData = append(currData, line...)
			continue
		}
		//Printf("header mode #%d\n", i)
		cont := isContinue(i, line)
		if cont {
			if currKey == "" {
				Printf("#%d no current key(%d)\n", i, len(msg))
				break
			}
			currVal, ok := raw[currKey]
			if !ok {
				Printf("#%d missing %s key in %v\n", i, currKey, DumpKeys(raw))
				break
			}
			val, ok := getContinuation(i, line)
			if ok {
				raw[currKey] = append(currVal, val...)
				if strings.ToLower(currKey) == LowerContentType {
					raw[LowerContentType] = raw[currKey]
				}
			}
		} else {
			key, val, ok := getHeader(i, line)
			if !ok {
				Printf("#%d incorrect header(%d)\n", i, len(msg))
				break
			}
			currVal, ok := raw[key]
			if ok {
				//Printf("#%d duplicated key %s, appending all of them with new line separator\n", i, key)
				currVal = append(currVal, []byte("\n")...)
				raw[key] = append(currVal, val...)
			} else {
				raw[key] = val
			}
			currKey = key
			if strings.ToLower(currKey) == LowerContentType {
				raw[LowerContentType] = raw[currKey]
			}
		}
	}
	if len(boundary) == 0 {
		//Printf("flush body\n")
		_ = addBody(nLines, []byte{})
	}
	ks := []string{}
	for k, v := range raw {
		item[k] = string(v)
		ks = append(ks, k)
	}
	sort.Strings(ks)
	/*
		for i, k := range ks {
			Printf("#%d %s: %s\n", i+1, k, item[k])
		}
		for i, body := range bodies {
			//Printf("#%d: %s %s %d\n", i, string(body.ContentType), DumpKeys(body.Properties), len(body.Data))
			Printf("#%d: %s %s %d\n", i, string(body.ContentType), body.Properties, len(body.Data))
		}
	*/
	// FIXME: continue
	// valid = true
	return
}
