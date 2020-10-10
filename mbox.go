package dads

import (
	"bytes"
	"fmt"
	"sort"
)

// ParseMBoxMsg - parse a raw MBox message into object to be inserte dinto raw ES
func ParseMBoxMsg(ctx *Ctx, msg []byte) (item map[string]interface{}, valid bool, err error) {
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
	getHeader := func(i int, line []byte) (key string, val []byte, ok bool) {
		if ctx.Debug > 1 {
			defer func() {
				//Printf("#%d getHeader '%s' --> %s, %s, %v\n", i, line, key, string(val), ok)
			}()
		}
		sep := []byte(": ")
		ary := bytes.Split(line, sep)
		if len(ary) == 1 {
			return
		}
		key = string(ary[0])
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
		ok = len(val) > 0
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
	addBody := func(i int, line []byte) (added bool) {
		if len(currContentType) == 0 || len(currData) == 0 {
			return
		}
		defer func() {
			if ctx.Debug > 1 {
				//Printf("#%d addBody '%s' --> (%s,%s,%d,%v)\n", i, string(line), string(currContentType), currProperties, len(currData), added)
				//Printf("#%d addBody '%s' --> (%s,%s,%d,%v)\n", i, string(line), string(currContentType), DumpKeys(currProperties), len(currData), added)
				//Printf("Body:\n%s\n", string(currData))
				// FIXME: remove this
				fmt.Printf("message: '%s'\n", string(currData))
			}
			currContentType = []byte{}
			currProperties = make(map[string][]byte)
			currData = []byte{}
		}()
		bodies = append(bodies, Body{ContentType: currContentType, Properties: currProperties, Data: currData})
		added = true
		return
	}
	//last := false
	currKey := ""
	body := false
	last := false
	savedBoundary := []byte{}
	savedContentType := []byte{}
	bodyHeadersParsed := false
	for idx, line := range lines {
		i := idx + 2
		if i == 0 {
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
		}
		if len(line) == 0 {
			if !body {
				//Printf("#%d empty: mode change\n", i)
				contentType, ok := raw["Content-Type"]
				if !ok {
					Printf("#%d no Content-Type defined, only headers will be parsed\n", i)
					break
				}
				boundarySep := []byte("boundary=")
				if bytes.Contains(contentType, boundarySep) {
					ary := bytes.Split(contentType, boundarySep)
					if len(ary) > 1 {
						ary2 := bytes.Split(ary[1], []byte(`"`))
						if len(ary2) > 2 {
							boundary = ary2[1]
						}
					}
					if len(boundary) == 0 {
						Printf("#%d cannot find multipart message boundary\n", i)
						break
					}
				} else {
					currContentType = contentType
					currProperties["Content-Transfer-Encoding"], ok = raw["Content-Transfer-Encoding"]
					if !ok {
						Printf("#%d no Content-Transfer-Encoding defined, only headers will be parsed\n", i)
						break
					}
					//Printf("#%d no-multipart email, content type: %s, transfer encoding: %v\n", i, currContentType, currProperties)
				}
				body = true
				continue
			}
			if len(boundary) == 0 {
				if last {
					Printf("#%d extra empty line in body mode when no multi part boundary is set\n", i)
					continue
				}
				_ = addBody(i, line)
				last = true
			}
			// FIXME: Should we just add a new line here?
			if bodyHeadersParsed {
				currData = append(currData, []byte("\n")...)
			}
			continue
		}
		if body {
			boundarySep, end := isBoundarySep(i, line)
			if boundarySep {
				bodyHeadersParsed = false
				_ = addBody(i, line)
				if end {
					if len(savedBoundary) > 0 {
						// Printf("restore saved: %s -> %s, %s -> %s\n", string(savedBoundary), string(boundary), string(savedContentType), string(currContentType))
						boundary = savedBoundary
						currContentType = savedContentType
						savedBoundary = []byte{}
						savedContentType = []byte{}
						// should we also store/restore properties map?
					}
				}
				continue
			}
			if !bodyHeadersParsed {
				key, val, ok := getHeader(i, line)
				if ok {
					if key == "Content-Type" {
						// Printf("%s -> %s\n", currContentType, val)
						currContentType = val
						boundarySep := []byte("boundary=")
						if bytes.Contains(currContentType, boundarySep) {
							ary := bytes.Split(currContentType, boundarySep)
							if len(ary) > 1 {
								ary2 := bytes.Split(ary[1], []byte(`"`))
								if len(ary2) > 2 {
									// Printf("save multi boundary: %s, %s\n", string(boundary), string(currContentType))
									savedBoundary = boundary
									savedContentType = currContentType
									boundary = ary2[1]
									// save properties map too?
								}
							}
							if len(boundary) == 0 {
								Printf("#%d cannot find multiboundary message boundary\n", i)
								break
							}
						}
						continue
					}
					// Printf("assigning %s %s\n", key, string(val))
					currProperties[key] = val
					continue
				}
				bodyHeadersParsed = true
			}
			//Printf("#%d body line, boundary %s\n", i, string(boundary))
			currData = append(currData, line...)
			// FIXME: do we actually know if we need to add newlines or not, MBOX seems to limit line length to 80 chars
			continue
		}
		cont := isContinue(i, line)
		if cont {
			if currKey == "" {
				Printf("#%d no current key\n", i)
				break
			}
			currVal, ok := raw[currKey]
			if !ok {
				Printf("#%d missing %s key in %v\n", i, currKey, DumpKeys(raw))
				break
			}
			val, ok := getContinuation(i, line)
			if !ok {
				Printf("#%d no continuation data in line %s\n", i, line)
			}
			raw[currKey] = append(currVal, val...)
		} else {
			key, val, ok := getHeader(i, line)
			if !ok {
				Printf("#%d incorrect header\n", i)
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
		}
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
