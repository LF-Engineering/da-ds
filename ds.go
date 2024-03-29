package dads

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Typical run:
// DA_DS=jira DA_JIRA_ENRICH=1 DA_JIRA_ES_URL=... DA_JIRA_RAW_INDEX=proj-raw DA_JIRA_RICH_INDEX=proj DA_JIRA_URL=https://jira.xyz.org DA_JIRA_DEBUG=1 DA_JIRA_PROJECT=proj DA_JIRA_DB_NAME=db DA_JIRA_DB_USER=u DA_JIRA_DB_PASS=p DA_JIRA_MULTI_ORIGIN=1 ./dads

const (
	// BulkRefreshMode - bulk upload refresh mode, can be: false, true, wait_for (ES defaults to false)
	BulkRefreshMode = "true"
	// BulkWaitForActiveShardsMode - bulk upload wait_for_active_shards mode, can be: 1, 2, ..., all (ES defaults to 1)
	BulkWaitForActiveShardsMode = "all"
	// KeywordMaxlength - max description length
	KeywordMaxlength = 1000
	// DefaultRateLimitHeader - default value for rate limit header
	DefaultRateLimitHeader = "X-RateLimit-Remaining"
	// DefaultRateLimitResetHeader - default value for rate limit reset header
	DefaultRateLimitResetHeader = "X-RateLimit-Reset"
)

var (
	// SettingsFieldsNumberLimit - make maximum number of index fields bigger (some raw indices have a lot of fields and we don't control this)
	SettingsFieldsNumberLimit = []byte(`{"index.mapping.total_fields.limit":50000}`)
	// MappingNotAnalyzeString - make all string keywords by default (not analyze them)
	MappingNotAnalyzeString = []byte(`{"dynamic_templates":[{"notanalyzed":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"type":"date","format":"strict_date_optional_time||epoch_millis"}}}]}`)
	// RawFields - standard raw fields
	RawFields = []string{DefaultDateField, DefaultTimestampField, DefaultOriginField, DefaultTagField, UUID, Offset}
	// DefaultDateFrom - default date from
	DefaultDateFrom = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

// DS - interface for all data source types
type DS interface {
	ParseArgs(*Ctx) error
	Name() string
	Info() string
	Validate(*Ctx) error
	FetchRaw(*Ctx) error
	FetchItems(*Ctx) error
	Enrich(*Ctx) error
	DateField(*Ctx) string
	OffsetField(*Ctx) string
	OriginField(*Ctx) string
	Categories() map[string]struct{}
	CustomFetchRaw() bool
	CustomEnrich() bool
	SupportDateFrom() bool
	SupportOffsetFrom() bool
	ResumeNeedsOrigin(*Ctx, bool) bool
	ResumeNeedsCategory(*Ctx, bool) bool
	Origin(*Ctx) string
	ItemID(interface{}) string
	RichIDField(*Ctx) string
	RichAuthorField(*Ctx) string
	ItemUpdatedOn(interface{}) time.Time
	ItemCategory(interface{}) string
	ElasticRawMapping() []byte
	ElasticRichMapping() []byte
	AddMetadata(*Ctx, interface{}) map[string]interface{}
	GetItemIdentities(*Ctx, interface{}) (map[[3]string]struct{}, error)
	EnrichItems(*Ctx) error
	EnrichItem(*Ctx, map[string]interface{}, string, bool, interface{}) (map[string]interface{}, error)
	AffsItems(*Ctx, map[string]interface{}, []string, interface{}) (map[string]interface{}, error)
	GetRoleIdentity(*Ctx, map[string]interface{}, string) map[string]interface{}
	AllRoles(*Ctx, map[string]interface{}) ([]string, bool)
	CalculateTimeToReset(*Ctx, int, int) int
	HasIdentities() bool
	UseDefaultMapping(*Ctx, bool) bool
}

// CommonFields - common rich item fields
// { "is_dsname_category": 1, "grimoire_creation_date": dt}
func CommonFields(ds DS, date interface{}, category string) (fields map[string]interface{}) {
	dt, err := TimeParseInterfaceString(date)
	if err != nil {
		switch vdt := date.(type) {
		case string:
			// 1st date is in UTC, 2nd is in TZ, 3rd is TZ offset innhours
			var ok bool
			dt, _, _, ok = ParseDateWithTz(vdt)
			if !ok {
				Fatalf("CommonFields: cannot parse date %s\n", vdt)
				return
			}
		case time.Time:
			dt = vdt
		default:
			Fatalf("cannot parse date %T %v\n", vdt, vdt)
			return
		}
	}
	name := "is_" + ds.Name() + "_" + category
	fields = map[string]interface{}{"grimoire_creation_date": dt, name: 1}
	return
}

// ESBulkUploadFunc - function to bulk upload items to ES
// We assume here that docs maintained my iterator func contains a list of rich items
// outDocs is maintained with ES bulk size
// last flag signalling that this is the last (so it must flush output then)
//         there can be no items in input pack in the last flush call
func ESBulkUploadFunc(ctx *Ctx, ds DS, thrN int, docs, outDocs *[]interface{}, last bool) (e error) {
	if ctx.Debug > 0 {
		Printf("ES bulk uploading %d/%d func\n", len(*docs), len(*outDocs))
	}
	bulkSize := ctx.ESBulkSize
	itemID := ds.RichIDField(ctx)
	run := func() (err error) {
		nItems := len(*outDocs)
		if ctx.Debug > 0 {
			Printf("ES bulk uploading %d items to ES\n", nItems)
		}
		nPacks := nItems / bulkSize
		if nItems%bulkSize != 0 {
			nPacks++
		}
		for i := 0; i < nPacks; i++ {
			from := i * bulkSize
			to := from + bulkSize
			if to > nItems {
				to = nItems
			}
			if ctx.Debug > 0 {
				Printf("ES bulk upload: bulk uploading pack #%d %d-%d (%d/%d) to ES\n", i+1, from, to, to-from, nPacks)
			}
			err = SendToElastic(ctx, ds, false, itemID, (*outDocs)[from:to])
			if err != nil {
				return
			}
		}
		return
	}
	nDocs := len(*docs)
	nOutDocs := len(*outDocs)
	if ctx.Debug > 0 {
		Printf("ES bulk upload pack size %d/%d last %v\n", nDocs, nOutDocs, last)
	}
	for _, doc := range *docs {
		*outDocs = append(*outDocs, doc)
		nOutDocs = len(*outDocs)
		if nOutDocs >= bulkSize {
			if ctx.Debug > 0 {
				Printf("ES bulk pack size %d/%d reached, flushing\n", nOutDocs, bulkSize)
			}
			e = run()
			if e != nil {
				return
			}
			*outDocs = []interface{}{}
		}
	}
	if last {
		nOutDocs := len(*outDocs)
		if nOutDocs > 0 {
			e = run()
			if e != nil {
				return
			}
			*outDocs = []interface{}{}
		}
	}
	*docs = []interface{}{}
	if ctx.Debug > 0 {
		nOutDocs = len(*outDocs)
		if nOutDocs > 0 {
			Printf("ES bulk upload %d items left (last %v)\n", nOutDocs, last)
		}
	}
	return
}

// DBUploadIdentitiesFunc - function to upload identities to affiliation DB
// We assume here that docs maintained my iterator func contains a list of [3]string
// Each identity is [3]string [name, username, email]
// outDocs is maintained with DB bulk size
// last flag signalling that this is the last (so it must flush output then)
//         there can be no items in input pack in the last flush call
func DBUploadIdentitiesFunc(ctx *Ctx, ds DS, thrN int, docs, outDocs *[]interface{}, last bool) (e error) {
	if ctx.Debug > 0 {
		Printf("DB bulk uploading %d/%d identities func\n", len(*docs), len(*outDocs))
	}
	//bulkSize := ctx.DBBulkSize / 6
	// We don't insert 1000 parameters into one () but 100 times (?,?,?,?,?,?)
	bulkSize := ctx.DBBulkSize
	run := func() (err error) {
		var tx *sql.Tx
		err = SetDBSessionOrigin(ctx)
		if err != nil {
			return
		}
		tx, err = ctx.DB.Begin()
		if err != nil {
			return
		}
		// Dedup (data comes from possibly multiple input packs
		// Each one is already deduped but the combination may have duplicates
		nNonUni := len(*outDocs)
		idents := make(map[[3]string]struct{})
		for _, doc := range *outDocs {
			idents[doc.([3]string)] = struct{}{}
		}
		identsAry := [][3]string{}
		for ident := range idents {
			identsAry = append(identsAry, ident)
		}
		nIdents := len(identsAry)
		source := ds.Name()
		runOneByOne := func() (err error) {
			Printf("DB bulk upload: falling back to one-by-one mode for %d items\n", nIdents)
			var (
				er   error
				errs []error
				itx  *sql.Tx
			)
			defer func() {
				nErrs := len(errs)
				if nErrs == 0 {
					Printf("DB bulk upload: one-by-one mode for %d items - all succeeded\n", nIdents)
					return
				}
				s := fmt.Sprintf("%d errors: ", nErrs)
				for _, er := range errs {
					s += er.Error() + ", "
				}
				s = s[:len(s)-2]
				err = fmt.Errorf("%s", s)
				Printf("DB bulk upload: one-by-one mode for %d items: %d errors\n", nIdents, nErrs)
			}()
			for i := 0; i < nIdents; i++ {
				ident := identsAry[i]
				if ctx.DebugSQL > 0 {
					Printf("DB bulk upload: one-by-one: ident %d/%d: %+v\n", i, nIdents, ident)
				}
				queryU := "insert ignore into uidentities(uuid,last_modified) values"
				queryI := "insert ignore into identities(id,source,name,email,username,uuid,last_modified) values"
				queryP := "insert ignore into profiles(uuid,name,email) values"
				argsU := []interface{}{}
				argsI := []interface{}{}
				argsI2 := []interface{}{}
				argsP := []interface{}{}
				name := ident[0]
				username := ident[1]
				email := ident[2]
				// DA-4391 - future
				// returns (valid, newEmail), if email is invalid "" is returned this is why we can skip testing for valid
				// params: (email, validateDomain, guess), guess means that we do some replaces like " at " -> "@" etc.
				origemail := email
				_, email = IsValidEmail(email, true, true)
				// DA-4366: starts
				origname := name
				origusername := username
				// DA-4366: ends
				name, username = PostprocessNameUsername(name, username, email)
				var (
					pname         *string
					pemail        *string
					pusername     *string
					profname      *string
					porigname     *string
					porigusername *string
					porigemail    *string
				)
				if name != Nil {
					pname = &name
					profname = &name
				}
				if email != Nil {
					pemail = &email
				}
				if username != Nil {
					pusername = &username
				}
				if origname != Nil {
					porigname = &origname
				}
				if origusername != Nil {
					porigusername = &origusername
				}
				if origemail != Nil {
					porigemail = &origemail
				}
				if ctx.DebugSQL > 0 {
					Printf("DB bulk upload: one-by-one: %d/%d: ('%s','%s','%s','%s','%s','%s')\n", i, nIdents, name, username, email, origname, origusername, origemail)
				}
				if pname == nil && pemail == nil && pusername == nil {
					continue
				}
				// if username matches a real email and there is no email set, assume email=username
				if pemail == nil && pusername != nil {
					valid, em := IsValidEmail(username, true, true)
					if valid {
						username = em
						pemail = &username
						email = username
					}
				}
				// if name matches a real email and there is no email set, assume email=name
				if pemail == nil && pname != nil {
					valid, em := IsValidEmail(name, true, true)
					if valid {
						name = em
						pemail = &name
						email = name
					}
				}
				// uuid(source, email, name, username)
				// DA-4366: starts
				origuuid := UUIDAffs(ctx, source, origemail, origname, origusername)
				if origuuid == "" {
					er := fmt.Errorf("error: uploadToDB: failed to generate orig uuid for (%s,%s,%s,%s)", source, origemail, origname, origusername)
					Printf("DB bulk upload: one-by-one(%d/%d): %v\n", i+1, nIdents, er)
					errs = append(errs, er)
					continue
				}
				// DA-4366: ends
				uuid := UUIDAffs(ctx, source, email, name, username)
				if ctx.DebugSQL > 0 {
					Printf("DB bulk upload: one-by-one: %d/%d: ('%s','%s','%s','%s','%s','%s','%s','%s')\n", i, nIdents, name, username, email, origname, origusername, origemail, uuid, origuuid)
				}
				if uuid == "" {
					er := fmt.Errorf("error: uploadToDB: failed to generate uuid for (%s,%s,%s,%s)", source, email, name, username)
					Printf("DB bulk upload: one-by-one(%d/%d): %v\n", i+1, nIdents, er)
					errs = append(errs, er)
					continue
				}
				var rows *sql.Rows
				rows, er = QuerySQL(ctx, nil, "select uuid from identities where id in (?, ?)", uuid, origuuid)
				if er != nil {
					errs = append(errs, er)
					continue
				}
				mergedUUID := uuid
				// DA-4366: starts
				foundUUID := 0
				// DA-4366: ends
				for rows.Next() {
					er = rows.Scan(&mergedUUID)
					if er == nil {
						foundUUID = 1
					}
					break
				}
				if er != nil {
					errs = append(errs, er)
					continue
				}
				er = rows.Err()
				if er != nil {
					errs = append(errs, er)
					continue
				}
				er = rows.Close()
				if er != nil {
					errs = append(errs, er)
					continue
				}
				if foundUUID == 0 {
					// Try to find matching identity even it it was generated with an old UUID library
					rows, er = QuerySQL(ctx, nil, "select id, uuid from identities where source = ? and name = ? and username = ? and email = ?", source, porigname, porigusername, porigemail)
					if er != nil {
						errs = append(errs, er)
						continue
					}
					iid := ""
					for rows.Next() {
						er = rows.Scan(&iid, &mergedUUID)
						if er == nil {
							foundUUID = 2
						}
						break
					}
					if er != nil {
						errs = append(errs, er)
						continue
					}
					er = rows.Err()
					if er != nil {
						errs = append(errs, er)
						continue
					}
					er = rows.Close()
					if er != nil {
						errs = append(errs, er)
						continue
					}
					// Fix that identity's id
					if foundUUID == 2 {
						_, er = ExecSQL(ctx, itx, "update identities set id = ? where id = ?", origuuid, iid)
						if er != nil {
							Printf("DB bulk upload: one-by-one(%d/%d): failed to update identity id %s->%s: %+v\n", i+1, nIdents, origuuid, iid, er)
							// _ = itx.Rollback()
							// errs = append(errs, er)
							// continue
						}
					}
				}
				if ctx.Debug > 0 && (uuid != mergedUUID || origuuid != mergedUUID) {
					Printf("one-by-one: merged profile detected: %s/%s -> %s,%d\n", uuid, origuuid, mergedUUID, foundUUID)
				}
				// DA-4366: starts
				// skip adding identity/profile/uidentity if identity already exists
				/*
					if isMerged {
						continue
					}
					rows, er = QuerySQL(ctx, nil, "select 1 from profiles where uuid in (?, ?)", uuid, origuuid)
					if er != nil {
						errs = append(errs, er)
						continue
					}
					dummy := 0
					for rows.Next() {
						er = rows.Scan(&dummy)
						break
					}
					if er != nil {
						errs = append(errs, er)
						continue
					}
					er = rows.Err()
					if er != nil {
						errs = append(errs, er)
						continue
					}
					er = rows.Close()
					if er != nil {
						errs = append(errs, er)
						continue
					}
					// skip adding identity/profile/uidentity if identity already exists
					if dummy != 0 {
						continue
					}
				*/
				// DA-4366: ends
				queryU += fmt.Sprintf("(?,now())")
				queryI += fmt.Sprintf("(?,?,?,?,?,?,now())")
				queryP += fmt.Sprintf("(?,?,?)")
				argsU = append(argsU, mergedUUID)
				// DA-4366: starts
				// argsI = append(argsI, uuid, source, pname, pemail, pusername, mergedUUID)
				argsI = append(argsI, origuuid, source, porigname, porigemail, porigusername, mergedUUID)
				// DA-4366: ends
				argsP = append(argsP, mergedUUID, profname, pemail)
				itx, err = ctx.DB.Begin()
				if err != nil {
					return
				}
				_, er = ExecSQL(ctx, itx, queryU, argsU...)
				if er != nil {
					Printf("DB bulk upload: one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryU, argsU, er)
					_ = itx.Rollback()
					errs = append(errs, er)
					continue
				}
				_, er = ExecSQL(ctx, itx, queryP, argsP...)
				if er != nil {
					Printf("DB bulk upload: one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryP, argsP, er)
					_ = itx.Rollback()
					errs = append(errs, er)
					continue
				}
				_, er = ExecSQL(ctx, itx, queryI, argsI...)
				if er != nil {
					Printf("DB bulk upload: one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryI, argsI, er)
					_ = itx.Rollback()
					errs = append(errs, er)
					continue
				}
				if uuid != origuuid {
					argsI2 = append(argsI2, uuid, source, pname, pemail, pusername, mergedUUID)
					_, er = ExecSQL(ctx, itx, queryI, argsI2...)
					if er != nil {
						Printf("DB bulk upload: one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryI, argsI2, er)
						_ = itx.Rollback()
						errs = append(errs, er)
						continue
					}
				}
				err = itx.Commit()
				if err != nil {
					return
				}
				itx = nil
			}
			return
		}
		if ctx.Debug > 0 {
			Printf("DB bulk upload: bulk adding %d (%d unique) idents\n", nNonUni, nIdents)
		}
		// NOTE: For normal bulk mode operation, uncomment the deferred function
		/*
			defer func() {
				if tx != nil {
					if ctx.DryRun {
						Printf("DB bulk upload: dry-run: rolling back %d identities insert (possibly due to dry-run mode)\n", nIdents)
					} else {
						Printf("DB bulk upload: rolling back %d identities insert\n", nIdents)
					}
					_ = tx.Rollback()
					err = runOneByOne()
				}
			}()
		*/
		// NOTE: now because new specs were added - and they prevent bulk mode, because we need to check the DB state first, before any insert
		// Comment out manually called runOneByOne() and uncomment deferred
		err = runOneByOne()
		if 1 == 1 {
			return
		}
		// End note.
		nPacks := nIdents / bulkSize
		if nIdents%bulkSize != 0 {
			nPacks++
		}
		for i := 0; i < nPacks; i++ {
			from := i * bulkSize
			to := from + bulkSize
			if to > nIdents {
				to = nIdents
			}
			queryU := "insert ignore into uidentities(uuid,last_modified) values"
			queryI := "insert ignore into identities(id,source,name,email,username,uuid,last_modified) values"
			queryP := "insert ignore into profiles(uuid,name,email) values"
			argsU := []interface{}{}
			argsI := []interface{}{}
			argsP := []interface{}{}
			if ctx.Debug > 0 {
				Printf("DB bulk upload: bulk adding idents pack #%d %d-%d (%d/%d)\n", i+1, from, to, to-from, nIdents)
			}
			uuids := map[int][]interface{}{}
			for j := from; j < to; j++ {
				ident := identsAry[j]
				name := ident[0]
				username := ident[1]
				email := ident[2]
				name, username = PostprocessNameUsername(name, username, email)
				var (
					pname     *string
					pemail    *string
					pusername *string
					profname  *string
				)
				if name != Nil {
					pname = &name
					profname = &name
				}
				if email != Nil {
					pemail = &email
				}
				if username != Nil {
					pusername = &username
				}
				if pname == nil && pemail == nil && pusername == nil {
					continue
				}
				// if username matches a real email and there is no email set, assume email=username
				if pemail == nil && pusername != nil {
					valid, em := IsValidEmail(username, true, true)
					if valid {
						username = em
						pemail = &username
						email = username
					}
				}
				// if name matches a real email and there is no email set, assume email=name
				if pemail == nil && pname != nil {
					valid, em := IsValidEmail(name, true, true)
					if valid {
						name = em
						pemail = &name
						email = name
					}
				}
				// uuid(source, email, name, username)
				uuid := UUIDAffs(ctx, source, email, name, username)
				if uuid == "" {
					Printf("error: uploadToDb(bulk): failed to generate uuid for (%s,%s,%s,%s), skipping this one\n", source, email, name, username)
					continue
				}
				uuids[j] = []interface{}{uuid, source, pname, pemail, pusername, profname}
			}
			queryS := "select id, uuid from identities where id in("
			argsS := []interface{}{}
			for _, data := range uuids {
				queryS += "?,"
				argsS = append(argsS, data[0])
			}
			queryS = queryS[:len(queryS)-1] + ")"
			var rows *sql.Rows
			rows, err = QuerySQL(ctx, nil, queryS, argsS...)
			if err != nil {
				return
			}
			var (
				id   string
				uuid string
			)
			id2uuid := map[string]string{}
			for rows.Next() {
				err = rows.Scan(&id, &uuid)
				if err != nil {
					return
				}
				id2uuid[id] = uuid
			}
			err = rows.Err()
			if err != nil {
				return
			}
			err = rows.Close()
			if err != nil {
				return
			}
			for j := from; j < to; j++ {
				data, ok := uuids[j]
				if !ok {
					continue
				}
				uuid, _ := data[0].(string)
				mergedUUID, ok := id2uuid[uuid]
				if !ok {
					mergedUUID = uuid
				}
				if ctx.Debug > 0 && uuid != mergedUUID {
					Printf("mass: merged profile detected: %s -> %s\n", uuid, mergedUUID)
				}
				source := data[1]
				pname := data[2]
				pemail := data[3]
				pusername := data[4]
				profname := data[5]
				queryU += fmt.Sprintf("(?,now()),")
				queryI += fmt.Sprintf("(?,?,?,?,?,?,now()),")
				queryP += fmt.Sprintf("(?,?,?),")
				argsU = append(argsU, mergedUUID)
				argsI = append(argsI, uuid, source, pname, pemail, pusername, mergedUUID)
				argsP = append(argsP, mergedUUID, profname, pemail)
			}
			queryU = queryU[:len(queryU)-1]
			queryI = queryI[:len(queryI)-1]
			queryP = queryP[:len(queryP)-1] // + " on duplicate key update name=values(name),email=values(email),last_modified=now()"
			_, err = ExecSQL(ctx, tx, queryU, argsU...)
			if err != nil {
				return
			}
			_, err = ExecSQL(ctx, tx, queryP, argsP...)
			if err != nil {
				return
			}
			_, err = ExecSQL(ctx, tx, queryI, argsI...)
			if err != nil {
				return
			}
		}
		// Will not commit in dry-run mode, deferred function will rollback - so we can still test any errors
		// but the final commit is replaced with rollback
		if !ctx.DryRun {
			err = tx.Commit()
			if err != nil {
				return
			}
			tx = nil
		}
		return
	}
	nDocs := len(*docs)
	nOutDocs := len(*outDocs)
	if ctx.Debug > 0 {
		Printf("DB bulk upload: upload idents pack size %d/%d last %v\n", nDocs, nOutDocs, last)
	}
	for _, doc := range *docs {
		*outDocs = append(*outDocs, doc)
		nOutDocs = len(*outDocs)
		if nOutDocs >= bulkSize {
			if ctx.Debug > 0 {
				Printf("DB bulk upload: upload idents pack size %d/%d reached, flushing\n", nOutDocs, bulkSize)
			}
			e = run()
			if e != nil {
				return
			}
			*outDocs = []interface{}{}
		}
	}
	if last {
		nOutDocs := len(*outDocs)
		if nOutDocs > 0 {
			e = run()
			if e != nil {
				return
			}
			*outDocs = []interface{}{}
		}
	}
	*docs = []interface{}{}
	if ctx.Debug > 0 {
		nOutDocs = len(*outDocs)
		Printf("DB bulk upload: upload idents %d items left (last %v)\n", nOutDocs, last)
	}
	return
}

// StandardItemsFunc - just get each doument's _source and append to output docs
// items is a current pack of input items
// docs is a pointer to where extracted items will be stored
func StandardItemsFunc(ctx *Ctx, ds DS, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("standard items %d/%d func\n", len(items), len(*docs))
	}
	for _, item := range items {
		doc, ok := item.(map[string]interface{})["_source"]
		if !ok {
			err = fmt.Errorf("Missing _source in item %+v", DumpKeys(item))
			return
		}
		*docs = append(*docs, doc)
	}
	return
}

// ItemsIdentitiesFunc - extract identities from items
// items is a current pack of ES input items
// docs is a pointer to where extracted identities will be stored
// each identity is [3]string [name, username, email]
func ItemsIdentitiesFunc(ctx *Ctx, ds DS, thrN int, items []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("items identities %d/%d func\n", len(items), len(*docs))
	}
	var (
		mtx *sync.Mutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.Mutex{}
		ch = make(chan error)
	}
	idents := make(map[[3]string]struct{})
	for _, doc := range *docs {
		idents[doc.([3]string)] = struct{}{}
	}
	procItem := func(c chan error, it interface{}) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		doc, ok := it.(map[string]interface{})["_source"]
		if !ok {
			e = fmt.Errorf("Missing _source in item %+v", DumpKeys(it))
			return
		}
		var identities map[[3]string]struct{}
		identities, e = ds.GetItemIdentities(ctx, doc)
		if e != nil {
			e = fmt.Errorf("Cannot get identities from doc %+v", DumpKeys(doc))
			return
		}
		if identities == nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		for identity := range identities {
			idents[identity] = struct{}{}
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	updateDocs := func() {
		*docs = []interface{}{}
		for ident := range idents {
			*docs = append(*docs, ident)
		}
	}
	if thrN > 1 {
		nThreads := 0
		for _, item := range items {
			go func(it interface{}) {
				_ = procItem(ch, it)
			}(item)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		updateDocs()
		return
	}
	for _, item := range items {
		err = procItem(nil, item)
		if err != nil {
			return
		}
	}
	updateDocs()
	return
}

// ItemsRefreshIdentitiesFunc - refresh input items/re-enrich
// items is a current pack of ES rich items
// docs is a pointer to where updated rich items will be stored
func ItemsRefreshIdentitiesFunc(ctx *Ctx, ds DS, thrN int, richItems []interface{}, docs *[]interface{}) (err error) {
	if ctx.Debug > 0 {
		Printf("refresh identities %d/%d func\n", len(richItems), len(*docs))
	}
	var (
		mtx *sync.Mutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.Mutex{}
		ch = make(chan error)
	}
	roles, staticRoles := ds.AllRoles(ctx, nil)
	procRich := func(c chan error, rItem interface{}) (e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		var values map[string]interface{}
		doc, ok := rItem.(map[string]interface{})["_source"]
		if !ok {
			e = fmt.Errorf("Missing _source in item %+v", DumpKeys(rItem))
			return
		}
		rich, _ := doc.(map[string]interface{})
		var rols []string
		if staticRoles {
			rols = roles
		} else {
			rols, _ = ds.AllRoles(ctx, rich)
		}
		var er error
		values, er = AffsDataForRoles(ctx, ds, rich, rols)
		if er != nil {
			Printf("ItemsRefreshIdentitiesFunc/AffsDataForRoles: error: %v for %v %v\n", er, DumpKeys(rich), rols)
		}
		for prop, val := range values {
			rich[prop] = val
		}
		rich["groups"] = ctx.Groups
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		nThreads := 0
		for _, richItem := range richItems {
			go func(rItem interface{}) {
				_ = procRich(ch, rItem)
			}(richItem)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for _, richItem := range richItems {
		err = procRich(nil, richItem)
		if err != nil {
			return
		}
	}
	return
}

// UploadIdentities - upload identities to SH DB
// We assume here that docs maintained my iterator func contains a list of [3]string
// Each identity is [3]string [name, username, email]
func UploadIdentities(ctx *Ctx, ds DS) (err error) {
	Printf("%s: uploading identities\n", ds.Name())
	if ds.HasIdentities() {
		err = ForEachESItem(ctx, ds, true, DBUploadIdentitiesFunc, ItemsIdentitiesFunc, nil, true)
		Printf("%s: identities uploaded\n", ds.Name())
	} else {
		Printf("%s: identities not defined for the current datasource (no upload needed)\n", ds.Name())
	}
	return
}

// RefreshIdentities - refresh identities
// We iterate over rich index to refresh its affiliation data
func RefreshIdentities(ctx *Ctx, ds DS) (err error) {
	Printf("%s: refreshing identities\n", ds.Name())
	if ds.HasIdentities() {
		err = ForEachESItem(ctx, ds, false, ESBulkUploadFunc, ItemsRefreshIdentitiesFunc, nil, true)
		Printf("%s: identities refreshed\n", ds.Name())
	} else {
		Printf("%s: identities not defined for the current datasource (no refresh needed)\n", ds.Name())
	}
	return
}

// ForEachESItem - perform specific function for all raw/rich items
// ufunct: function to perform on input pack, receives input pack, pointer to an output pack
//         and a flag signalling that this is the last (so it must flush output then)
//         there can be no items in input pack in the last flush call
// uitems: function to extract items from input data: can just add documents, but can also maintain a pack of documents identities
//         receives items and pointer to output items (which then become input for ufunct)
func ForEachESItem(
	ctx *Ctx,
	ds DS,
	raw bool,
	ufunct func(*Ctx, DS, int, *[]interface{}, *[]interface{}, bool) error,
	uitems func(*Ctx, DS, int, []interface{}, *[]interface{}) error,
	cacheFor *time.Duration,
	mt bool,
) (err error) {
	dateField := JSONEscape(ds.DateField(ctx))
	originField := JSONEscape(ds.OriginField(ctx))
	origin := JSONEscape(ds.Origin(ctx))
	packSize := ctx.ESScrollSize
	var (
		scroll   *string
		dateFrom string
		res      interface{}
		status   int
	)
	headers := map[string]string{"Content-Type": "application/json"}
	if ctx.DateFrom != nil {
		dateFrom = ToESDate(*ctx.DateFrom)
	}
	attemptAt := time.Now()
	total := 0
	// Defer free scroll
	defer func() {
		if scroll == nil {
			return
		}
		url := ctx.ESURL + "/_search/scroll"
		payload := []byte(`{"scroll_id":"` + *scroll + `"}`)
		_, _, _, _, err := Request(
			ctx,
			url,
			Delete,
			headers,
			payload,
			[]string{},
			nil,
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}}, // OK statuses
			nil,                                 // Cache statuses
			false,                               // retry request
			nil,                                 // cacheExpire duration
			false,                               // skip in dry-run mode
		)
		if err != nil {
			Printf("Error releasing scroll %s: %+v, ignored\n", *scroll, err)
			err = nil
		}
	}()
	thrN := GetThreadsNum(ctx)
	fThrN := thrN
	if !mt {
		fThrN = 1
	}
	Printf("Multithreaded: %v, using %d threads\n", MT, thrN)
	nThreads := 0
	var (
		mtx *sync.Mutex
		ch  chan error
	)
	docs := []interface{}{}
	outDocs := []interface{}{}
	if thrN > 1 {
		mtx = &sync.Mutex{}
		ch = make(chan error)
	}
	funct := func(c chan error, last bool) (e error) {
		defer func() {
			if thrN > 1 {
				mtx.Unlock()
			}
			if c != nil {
				c <- e
			}
		}()
		if thrN > 1 {
			mtx.Lock()
		}
		e = ufunct(ctx, ds, fThrN, &docs, &outDocs, last)
		return
	}
	needsOrigin := ds.ResumeNeedsOrigin(ctx, raw)
	needsCategory := ds.ResumeNeedsCategory(ctx, raw)
	for {
		var (
			url     string
			payload []byte
		)
		if scroll == nil {
			if raw {
				url = ctx.ESURL + "/" + ctx.RawIndex + "/_search?scroll=" + ctx.ESScrollWait + "&size=" + strconv.Itoa(ctx.ESScrollSize)
			} else {
				url = ctx.ESURL + "/" + ctx.RichIndex + "/_search?scroll=" + ctx.ESScrollWait + "&size=" + strconv.Itoa(ctx.ESScrollSize)
			}
			if needsCategory {
				category := ds.ItemCategory(ctx)
				categoryField := "is_" + ds.Name() + "_" + category
				if needsOrigin {
					if ctx.DateFrom == nil {
						payload = []byte(`{"query":{"bool":{"filter":[{"term":{"` + originField + `":"` + origin + `"}},{"term":{"` + categoryField + `":1}}]}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					} else {
						payload = []byte(`{"query":{"bool":{"filter":[{"term":{"` + originField + `":"` + origin + `"}},{"term":{"` + categoryField + `":1}},{"range":{"` + dateField + `":{"gte":"` + dateFrom + `"}}}]}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					}
				} else {
					if ctx.DateFrom == nil {
						payload = []byte(`{"query":{"bool":{"filter":{"term":{"` + categoryField + `":1}}}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					} else {
						payload = []byte(`{"query":{"bool":{"filter":[{"term":{"` + categoryField + `":1}},{"range":{"` + dateField + `":{"gte":"` + dateFrom + `"}}}]}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					}
				}
			} else {
				if needsOrigin {
					if ctx.DateFrom == nil {
						payload = []byte(`{"query":{"bool":{"filter":{"term":{"` + originField + `":"` + origin + `"}}}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					} else {
						payload = []byte(`{"query":{"bool":{"filter":[{"term":{"` + originField + `":"` + origin + `"}},{"range":{"` + dateField + `":{"gte":"` + dateFrom + `"}}}]}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					}
				} else {
					if ctx.DateFrom == nil {
						payload = []byte(`{"sort":{"` + dateField + `":{"order":"asc"}}}`)
					} else {
						payload = []byte(`{"query":{"bool":{"filter":{"range":{"` + dateField + `":{"gte":"` + dateFrom + `"}}}}},"sort":{"` + dateField + `":{"order":"asc"}}}`)
					}
				}
			}
			if ctx.Debug > 0 {
				Printf("feed raw=%v: processing query: %s\n", raw, string(payload))
			}
		} else {
			url = ctx.ESURL + "/_search/scroll"
			payload = []byte(`{"scroll":"` + ctx.ESScrollWait + `","scroll_id":"` + *scroll + `"}`)
		}
		res, status, _, _, err = Request(
			ctx,
			url,
			Post,
			headers,
			payload,
			[]string{},
			map[[2]int]struct{}{{200, 200}: {}}, // JSON statuses
			nil,                                 // Error statuses
			map[[2]int]struct{}{{200, 200}: {}, {404, 404}: {}, {500, 500}: {}}, // OK statuses
			map[[2]int]struct{}{{200, 200}: {}},                                 // Cache statuses
			true,
			cacheFor,
			false,
		)
		if ctx.Debug > 1 {
			Printf("%s%s --> %d\n", url, string(payload), status)
		}
		FatalOnError(err)
		if status == 404 {
			if scroll != nil && strings.Contains(string(res.([]byte)), NoSearchContextFound) {
				Printf("scroll %s probably expired, seeting it to 20 items/59 minutes for a safe retry, you should adjust your config: scroll wait and/or scroll size\n", *scroll)
				Printf("note that scroll will now restart, so the same data (with a small pack size 20) will be processed again\n")
				Printf("all documents should have unique id fields so this should not be an issue\n")
				if ctx.ESScrollWait != Wait59m {
					savedScrollWait := ctx.ESScrollWait
					ctx.ESScrollWait = Wait59m
					defer func() {
						ctx.ESScrollWait = savedScrollWait
					}()
				}
				if ctx.ESScrollSize > 20 {
					savedScrollSize := ctx.ESScrollSize
					ctx.ESScrollSize = 20
					defer func() {
						ctx.ESScrollSize = savedScrollSize
					}()
				}
				scroll = nil
				err = nil
				continue
			}
			Fatalf("got status 404 but not because of scroll context expiration:\n%s\n", string(res.([]byte)))
		}
		if status == 500 {
			if scroll == nil && status == 500 && strings.Contains(string(res.([]byte)), TooManyScrolls) {
				time.Sleep(5)
				now := time.Now()
				elapsed := now.Sub(attemptAt)
				Printf("%d retrying scroll, first attempt at %+v, elapsed %+v/%.0fs\n", len(res.(map[string]interface{})), attemptAt, elapsed, ctx.ESScrollWaitSecs)
				if elapsed.Seconds() > ctx.ESScrollWaitSecs {
					Fatalf("Tried to acquire scroll too many times, first attempt at %v, elapsed %v/%.0fs", attemptAt, elapsed, ctx.ESScrollWaitSecs)
				}
				continue
			}
			Fatalf("got status 500 but not because of too many scrolls:\n%s\n", string(res.([]byte)))
		}
		sScroll, ok := res.(map[string]interface{})["_scroll_id"].(string)
		if !ok {
			err = fmt.Errorf("Missing _scroll_id in the response %+v", DumpKeys(res))
			return
		}
		scroll = &sScroll
		items, ok := res.(map[string]interface{})["hits"].(map[string]interface{})["hits"].([]interface{})
		if !ok {
			err = fmt.Errorf("Missing hits.hits in the response %+v", DumpKeys(res))
			return
		}
		nItems := len(items)
		if nItems == 0 {
			break
		}
		if ctx.Debug > 0 {
			Printf("feed raw=%v: processing %d items\n", raw, nItems)
		}
		if thrN > 1 {
			mtx.Lock()
		}
		err = uitems(ctx, ds, fThrN, items, &docs)
		if err != nil {
			return
		}
		nDocs := len(docs)
		if nDocs >= packSize {
			if thrN > 1 {
				go func() {
					_ = funct(ch, false)
				}()
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			} else {
				err = funct(nil, false)
				if err != nil {
					return
				}
			}
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		total += nItems
	}
	if thrN > 1 {
		mtx.Lock()
	}
	if thrN > 1 {
		go func() {
			_ = funct(ch, true)
		}()
		nThreads++
		if nThreads == thrN {
			err = <-ch
			if err != nil {
				return
			}
			nThreads--
		}
	} else {
		err = funct(nil, true)
		if err != nil {
			return
		}
	}
	if thrN > 1 {
		mtx.Unlock()
	}
	for thrN > 1 && nThreads > 0 {
		err = <-ch
		nThreads--
		if err != nil {
			return
		}
	}
	if ctx.Debug > 0 {
		Printf("feed raw=%v: total number of items processed: %d\n", raw, total)
	}
	return
}

// IsOldFormat - is this an old format index (bitergia one)?
func IsOldFormat(ctx *Ctx, idx string) (old bool) {
	result, _, _, _, _ := Request(
		ctx,
		idx+"/_mapping",
		Get,
		nil,        // headers
		[]byte{},   // payload
		[]string{}, // cookies
		nil,        // JSON statuses
		nil,        // Error statuses
		nil,        // OK statuses
		nil,        // Cache statuses
		false,      // retry
		nil,        // cache duration
		true,       // skip in dry run
	)
	bResult, ok := result.([]byte)
	if !ok {
		return
	}
	sResult := string(bResult)
	// Printf("sResult:\n%s\n", sResult)
	old = strings.Contains(sResult, `"metadata__gelk_backend_name"`) || strings.Contains(sResult, `"metadata__gelk_version"`) || strings.Contains(sResult, `"perceval_version"`)
	return
}

// HandleMapping - create/update mapping for raw or rich index
func HandleMapping(ctx *Ctx, ds DS, raw bool) (err error) {
	var url string
	if raw {
		url = ctx.ESURL + "/" + ctx.RawIndex
	} else {
		url = ctx.ESURL + "/" + ctx.RichIndex
	}
	Printf("index: %s\n", url)
	var (
		result interface{}
		status int
	)
	stringResult := func(r interface{}) string {
		bR, ok := r.([]byte)
		if ok {
			return string(bR)
		}
		iR, ok := r.(map[string]interface{})
		if ok {
			return fmt.Sprintf("%+v", iR)
		}
		return fmt.Sprintf("%+v", r)
	}
	// Drop raw/rich if that flag is set
	if (raw && ctx.DropRaw) || (!raw && ctx.DropRich) {
		// If we want o drop raw or rich and if raw or rich is in old format
		if IsOldFormat(ctx, url) {
			result, status, _, _, err = Request(
				ctx,
				url+"?expand_wildcards=none&allow_no_indices=true&ignore_unavailable=true",
				Delete,
				nil,                                 // headers
				[]byte{},                            // payload
				[]string{},                          // cookies
				nil,                                 // JSON statuses
				map[[2]int]struct{}{{400, 599}: {}}, // error statuses: 401-599
				nil,                                 // OK statuses
				nil,                                 // Cache statuses
				true,                                // retry
				nil,                                 // cache duration
				true,                                // skip in dry run
			)
			if err == nil {
				Printf("index %s deleted: status=%d, result: %+v\n", url, status, stringResult(result))
			} else {
				Printf("index %s not deleted: status=%d, err=%+v, result: %+v\n", url, status, err, stringResult(result))
			}
		}
	}
	// Create index, ignore if exists (see status 400 is not in error statuses)
	result, status, _, _, err = Request(
		ctx,
		url+"?wait_for_active_shards=all",
		Put,
		nil,                                 // headers
		[]byte{},                            // payload
		[]string{},                          // cookies
		nil,                                 // JSON statuses
		map[[2]int]struct{}{{401, 599}: {}}, // error statuses: 401-599
		nil,                                 // OK statuses
		nil,                                 // Cache statuses
		true,                                // retry
		nil,                                 // cache duration
		true,                                // skip in dry run
	)
	Printf("index %s created: status=%d, result: %+v\n", url, status, stringResult(result))
	FatalOnError(err)
	// DS specific raw index mapping
	var mapping []byte
	if raw {
		mapping = ds.ElasticRawMapping()
	} else {
		mapping = ds.ElasticRichMapping()
	}
	result, status, _, _, err = Request(
		ctx,
		url+"/_mapping",
		Put,
		map[string]string{"Content-Type": "application/json"},
		mapping,
		[]string{},
		nil,
		nil,
		map[[2]int]struct{}{{200, 200}: {}},
		nil,
		true,
		nil,
		true,
	)
	if ctx.Debug > 0 {
		Printf("index mapping %s -> status=%d, result: %+v\n", url, status, stringResult(result))
		Printf("mapping: %+v\n", string(mapping))
	}
	FatalOnError(err)
	if ds.UseDefaultMapping(ctx, raw) {
		// Global not analyze string mapping
		result, status, _, _, err = Request(
			ctx,
			url+"/_mapping",
			Put,
			map[string]string{"Content-Type": "application/json"},
			MappingNotAnalyzeString,
			[]string{},
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}},
			nil,
			true,
			nil,
			true,
		)
		if ctx.Debug > 0 {
			Printf("index not analyze string mapping %s -> status=%d, result: %+v\n", url, status, stringResult(result))
		}
		FatalOnError(err)
	}
	if raw {
		result, status, _, _, err = Request(
			ctx,
			url+"/_settings",
			Put,
			map[string]string{"Content-Type": "application/json"},
			SettingsFieldsNumberLimit,
			[]string{},
			nil,
			nil,
			map[[2]int]struct{}{{200, 200}: {}},
			nil,
			true,
			nil,
			true,
		)
		if ctx.Debug > 0 {
			Printf("index settings %s -> status=%d, result: %+v\n", url, status, stringResult(result))
		}
		FatalOnError(err)
	}
	return
}

// FetchRaw - implement fetch raw data (generic)
func FetchRaw(ctx *Ctx, ds DS) (err error) {
	// FIXME
	_, directFetch := os.LookupEnv("DIRECT_FETCH_ITEMS")
	if directFetch {
		return ds.FetchItems(ctx)
	}
	err = HandleMapping(ctx, ds, true)
	if err != nil {
		Fatalf(ds.Name()+": HandleMapping error: %+v\n", err)
	}
	if ds.CustomFetchRaw() {
		return ds.FetchRaw(ctx)
	}
	if ctx.DateFrom != nil && ctx.OffsetFrom >= 0.0 {
		Fatalf(ds.Name() + ": you cannot use both date from and offset from\n")
	}
	if ctx.DateTo != nil && ctx.OffsetTo >= 0.0 {
		Fatalf(ds.Name() + ": you cannot use both date to and offset to\n")
	}
	var (
		lastUpdate *time.Time
		offset     *float64
	)
	if !ctx.ForceFull && ds.SupportDateFrom() {
		lastUpdate = ctx.DateFrom
		if lastUpdate == nil {
			lastUpdate = GetLastUpdate(ctx, ds, true)
		}
		if lastUpdate != nil {
			if ctx.DateFrom == nil {
				ctx.DateFromDetected = true
			}
			Printf("%s: raw: starting from date: %v, detected: %v\n", ds.Name(), *lastUpdate, ctx.DateFromDetected)
			ctx.DateFrom = lastUpdate
		} else {
			Printf("%s: raw: no start date detected\n", ds.Name())
		}
	}
	if !ctx.ForceFull && ds.SupportOffsetFrom() {
		if ctx.OffsetFrom >= 0.0 {
			offset = &ctx.OffsetFrom
		}
		if offset == nil {
			lastOffset := GetLastOffset(ctx, ds, true)
			if lastOffset >= 0.0 {
				offset = &lastOffset
			}
		}
		if offset != nil {
			if ctx.OffsetFrom < 0.0 {
				ctx.OffsetFromDetected = true
			}
			Printf("%s: raw: starting from offset: %v, detected: %v\n", ds.Name(), *offset, ctx.OffsetFromDetected)
			ctx.OffsetFrom = *offset
		} else {
			Printf("%s: raw: no start offset detected\n", ds.Name())
		}
	}
	if lastUpdate != nil && offset != nil {
		Fatalf(ds.Name() + ": you cannot use both date from and offset from\n")
	}
	if ctx.Category != "" {
		_, ok := ds.Categories()[ctx.Category]
		if !ok {
			Fatalf(ds.Name() + ": category " + ctx.Category + " not supported")
		}
	}
	err = ds.FetchItems(ctx)
	return
}

// Enrich - implement fetch raw data (generic)
func Enrich(ctx *Ctx, ds DS) (err error) {
	defer func() {
		err = ctx.DB.Close()
	}()
	err = HandleMapping(ctx, ds, false)
	if err != nil {
		Fatalf(ds.Name()+": HandleMapping error: %+v\n", err)
	}
	if ds.CustomEnrich() {
		return ds.Enrich(ctx)
	}
	dbConfigured := ctx.AffsDBConfigured()
	if !dbConfigured && ctx.OnlyIdentities {
		Fatalf("Only identities mode specified and DB not configured")
	}
	if !dbConfigured && ctx.RefreshAffs {
		Fatalf("Refresh affiliations mode specified and DB not configured")
	}
	if dbConfigured {
		ConnectAffiliationsDB(ctx)
	}
	var (
		lastUpdate *time.Time
		offset     *float64
		adjusted   bool
	)
	if !ctx.ForceFull && ds.SupportDateFrom() {
		if ctx.DateFromDetected {
			lastUpdate = GetLastUpdate(ctx, ds, false)
			if lastUpdate != nil && (*lastUpdate).After(*ctx.DateFrom) {
				lastUpdate = ctx.DateFrom
				adjusted = true
			}
		} else {
			lastUpdate = ctx.DateFrom
		}
		if lastUpdate != nil {
			Printf("%s: rich: starting from date: %v, detected: %v, adjusted: %v\n", ds.Name(), *lastUpdate, ctx.DateFromDetected, adjusted)
		} else {
			Printf("%s: rich: no start date detected\n", ds.Name())
		}
		ctx.DateFrom = lastUpdate
	}
	if !ctx.ForceFull && ds.SupportOffsetFrom() {
		adjusted = false
		if ctx.OffsetFromDetected {
			lastOffset := GetLastOffset(ctx, ds, false)
			if lastOffset >= 0.0 {
				offset = &lastOffset
				if lastOffset > ctx.OffsetFrom {
					offset = &ctx.OffsetFrom
					adjusted = true
				}
			}
		} else {
			if ctx.OffsetFrom >= 0.0 {
				offset = &ctx.OffsetFrom
			}
		}
		if offset != nil {
			Printf("%s: rich: starting from offset: %v, detected: %v, adjusted: %v\n", ds.Name(), *offset, ctx.OffsetFromDetected, adjusted)
			ctx.OffsetFrom = *offset
		} else {
			Printf("%s: rich: no start offset detected\n", ds.Name())
			ctx.OffsetFrom = -1.0
		}
	}
	if ctx.RefreshAffs {
		err = RefreshIdentities(ctx, ds)
		if err != nil {
			Fatalf(ds.Name()+": RefreshIdentities error: %+v\n", err)
		}
		return
	}
	if ctx.AffsDBConfigured() && !ctx.NoIdentities {
		err = UploadIdentities(ctx, ds)
		if err != nil {
			Fatalf(ds.Name()+": UploadIdentities error: %+v\n", err)
		}
	}
	if ctx.OnlyIdentities {
		return
	}
	err = ds.EnrichItems(ctx)
	if err != nil {
		Fatalf(ds.Name()+": EnrichItems error: %+v\n", err)
	}
	return
}

// EnrichItem - perform generic additional operations on already enriched item
func EnrichItem(ctx *Ctx, ds DS, richItem map[string]interface{}) (err error) {
	richItem[DefaultEnrichDateField] = time.Now()
	richItem[ProjectSlug] = ctx.ProjectSlug
	richItem["groups"] = ctx.Groups
	return
}

// UpdateRateLimit - generic function to get rate limit data from header
func UpdateRateLimit(ctx *Ctx, ds DS, headers map[string][]string, rateLimitHeader, rateLimitResetHeader string) (rateLimit, rateLimitReset, secondsToReset int) {
	if rateLimitHeader == "" {
		rateLimitHeader = DefaultRateLimitHeader
	}
	if rateLimitResetHeader == "" {
		rateLimitResetHeader = DefaultRateLimitResetHeader
	}
	v, ok := headers[rateLimitHeader]
	if !ok {
		lRateLimitHeader := strings.ToLower(rateLimitHeader)
		for k, va := range headers {
			kl := strings.ToLower(k)
			if kl == lRateLimitHeader {
				v = va
				ok = true
				break
			}
		}
	}
	if ok {
		if len(v) > 0 {
			rateLimit, _ = strconv.Atoi(v[0])
		}
	}
	v, ok = headers[rateLimitResetHeader]
	if !ok {
		lRateLimitResetHeader := strings.ToLower(rateLimitResetHeader)
		for k, va := range headers {
			kl := strings.ToLower(k)
			if kl == lRateLimitResetHeader {
				v = va
				ok = true
				break
			}
		}
	}
	if ok {
		if len(v) > 0 {
			var err error
			rateLimitReset, err = strconv.Atoi(v[0])
			if err == nil {
				secondsToReset = ds.CalculateTimeToReset(ctx, rateLimit, rateLimitReset)
			}
		}
	}
	if ctx.Debug > 1 {
		Printf("UpdateRateLimit(%+v,%s,%s) --> (%d,%d,%d)\n", headers, rateLimitHeader, rateLimitResetHeader, rateLimit, rateLimitReset, secondsToReset)
	}
	return
}

// SleepForRateLimit - sleep for rate or return error when rate exceeded
func SleepForRateLimit(ctx *Ctx, ds DS, rateLimit, rateLimitReset, minRate int, waitRate bool) (err error) {
	if rateLimit <= 0 || rateLimit > minRate {
		if ctx.Debug > 1 {
			Printf("rate limit is %d, min rate is %d, no need to wait\n", rateLimit, minRate)
		}
		return
	}
	secondsToReset := ds.CalculateTimeToReset(ctx, rateLimit, rateLimitReset)
	if secondsToReset < 0 {
		Printf("Warning: time to reset is negative %d, resetting to 0\n", secondsToReset)
		secondsToReset = 0
	}
	if waitRate {
		Printf("Waiting %d seconds for rate limit reset.\n", secondsToReset)
		time.Sleep(time.Duration(secondsToReset) * time.Second)
		return
	}
	err = fmt.Errorf("rate limit exceeded, not waiting %d seconds", secondsToReset)
	return
}
