package dads

import (
	"bytes"
	"sort"
)

// ParseMBoxMsg - parse a raw MBox message into object to be inserte dinto raw ES
func ParseMBoxMsg(ctx *Ctx, msg []byte) (item map[string]interface{}, valid bool, err error) {
	item = make(map[string]interface{})
	raw := make(map[string][]byte)
	lines := bytes.Split(msg, GroupsioMsgLineSeparator)
	if ctx.Debug > 1 {
		Printf("%d bytes, %d lines\n", len(msg), len(lines))
	}
	isContinue := func(i int, line []byte) (is bool) {
		if ctx.Debug > 1 {
			defer func() {
				Printf("#%d isContinue '%s' --> %v\n", i, string(line), is)
			}()
		}
		is = bytes.HasPrefix(line, []byte(" ")) || bytes.HasPrefix(line, []byte("\t"))
		return
	}
	getHeader := func(i int, line []byte) (key string, val []byte, ok bool) {
		if ctx.Debug > 1 {
			defer func() {
				Printf("#%d getHeader '%s' --> %s, %s, %v\n", i, line, key, string(val), ok)
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
				Printf("#%d getContinuation '%s' --> %s, %v\n", i, line, string(val), ok)
			}()
		}
		val = bytes.TrimLeft(line, " \t")
		ok = len(val) > 0
		return
	}
	//last := false
	currKey := ""
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
				Printf("#%d duplicated key %s, appending all of them with new line separator\n", i, key)
				currVal = append(currVal, []byte("\n")...)
				raw[key] = append(currVal, val...)
			} else {
				raw[key] = val
			}
			currKey = key
		}
		/*
			if len(line) == 0 {
				if !last {
					Printf("#%d: last line reached\n", i)
					last = true
					continue
				}
				Printf("#%d: multiple empty lines in mbox message - this is not allowed, skipping\n", i)
				return
			}
		*/
	}
	ks := []string{}
	for k, v := range raw {
		item[k] = string(v)
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for i, k := range ks {
		Printf("#%d %s: %s\n", i+1, k, item[k])
	}
	// FIXME: continue
	// valid = true
	return
}
