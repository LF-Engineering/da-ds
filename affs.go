package dads

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// MultiOrgNames - suffix for multiple orgs affiliation data
	MultiOrgNames = "_multi_org_names"
)

var (
	// AffsFields - all properties added by affiliations (excluding multi org name)
	AffsFields = []string{"_id", "_uuid", "_name", "_user_name", "_domain", "_gender", "_gender_acc", "_org_name", "_bot"}
	// RequiredAffsFields - required affs fields
	RequiredAffsFields = []string{"_org_name", "_name", "_user_name"}
	identityCache      = map[string][2]interface{}{}
	identityCacheMtx   *sync.RWMutex
	rollsCache         = map[string][]string{}
	rollsCacheMtx      *sync.RWMutex
	i2uCache           = map[string]interface{}{}
	i2uCacheMtx        *sync.RWMutex
)

// EmptyAffsItem - return empty affiliation sitem for a given role
func EmptyAffsItem(role string, undef bool) map[string]interface{} {
	emp := ""
	if undef {
		emp = "-- UNDEFINED --"
		// panic("track empty")
	}
	return map[string]interface{}{
		role + "_id":         emp,
		role + "_uuid":       emp,
		role + "_name":       emp,
		role + "_user_name":  emp,
		role + "_domain":     emp,
		role + "_gender":     emp,
		role + "_gender_acc": nil,
		role + "_org_name":   emp,
		role + "_bot":        false,
		role + MultiOrgNames: []interface{}{},
	}
}

// IdentityAffsDomain -return domain for given identity using email if specified
func IdentityAffsDomain(identity map[string]interface{}) (domain interface{}) {
	email, ok := identity["email"].(string)
	if ok {
		ary := strings.Split(email, "@")
		if len(ary) > 1 {
			domain = ary[1]
		}
	}
	return
}

// FindObject - fetch given fields from object (identities, profiles, uidentities etc.) having key=id
// Assuming that given object has an unique key to gte it
func FindObject(ctx *Ctx, object, key, id string, fields []string) (obj map[string]interface{}, err error) {
	var rows *sql.Rows
	rows, err = QuerySQL(ctx, nil, fmt.Sprintf("select %s from %s where %s = ? limit 1", strings.Join(fields, ", "), object, key), id)
	if err != nil {
		return
	}
	for rows.Next() {
		obj = make(map[string]interface{})
		data := make([]interface{}, len(fields))
		for i := range data {
			data[i] = new(interface{})
		}
		err = rows.Scan(data...)
		if err != nil {
			return
		}
		for i, val := range data {
			v := *val.(*interface{})
			if v == nil {
				obj[fields[i]] = v
				continue
			}
			switch cV := v.(type) {
			case []byte:
				obj[fields[i]] = string(cV)
			default:
				obj[fields[i]] = cV
			}
		}
		break
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	return
}

// GetIdentityUUID - identity's UUID for a given ID
// uses internal cache
func GetIdentityUUID(ctx *Ctx, ds DS, id string) (uuid interface{}) {
	if MT {
		i2uCacheMtx.RLock()
	}
	uuid, ok := i2uCache[id]
	if MT {
		i2uCacheMtx.RUnlock()
	}
	if ok {
		return
	}
	defer func() {
		if MT {
			i2uCacheMtx.Lock()
		}
		i2uCache[id] = uuid
		if MT {
			i2uCacheMtx.Unlock()
		}
	}()
	i, err := FindObject(ctx, "identities", "id", id, []string{"uuid"})
	if err != nil || i == nil {
		return
	}
	uuid = i["uuid"]
	return
}

// AffsIdentityIDs - returns affiliations identity id, uuid data
// uses internal cache
func AffsIdentityIDs(ctx *Ctx, ds DS, identity map[string]interface{}) (ids [2]interface{}) {
	email, _ := identity["email"]
	name, _ := identity["name"]
	username, _ := identity["username"]
	if email == nil && name == nil && username == nil {
		return
	}
	sEmail, okE := email.(string)
	sName, okN := name.(string)
	sUsername, okU := username.(string)
	k := sEmail + ":" + sName + ":" + sUsername
	if MT {
		identityCacheMtx.RLock()
	}
	ids, ok := identityCache[k]
	if MT {
		identityCacheMtx.RUnlock()
	}
	if ok {
		return
	}
	defer func() {
		if MT {
			identityCacheMtx.Lock()
		}
		identityCache[k] = ids
		if MT {
			identityCacheMtx.Unlock()
		}
	}()
	if !okE {
		sEmail = Nil
	}
	if !okN {
		sName = Nil
	}
	if !okU {
		sUsername = Nil
	}
	source := ds.Name()
	id := UUIDAffs(ctx, source, sEmail, sName, sUsername)
	identityFound, err := FindObject(ctx, "identities", "id", id, []string{"id", "uuid"})
	if err != nil || identityFound == nil {
		return
	}
	ids[0] = identityFound["id"]
	ids[1] = identityFound["uuid"]
	return
}

// QueryToStringArray - execute SQL query returning multiple rows each containitg a single string column
func QueryToStringArray(ctx *Ctx, query string, args ...interface{}) (res []string) {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = QuerySQL(ctx, nil, query, args...)
	if err != nil {
		return
	}
	var item string
	for rows.Next() {
		err = rows.Scan(&item)
		if err != nil {
			return
		}
		res = append(res, item)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	_ = rows.Close()
	return
}

// QueryToStringIntArrays - execute SQL query returning multiple rows each containitg (string,int64)
func QueryToStringIntArrays(ctx *Ctx, query string, args ...interface{}) (sa []string, ia []int64) {
	var (
		rows *sql.Rows
		err  error
	)
	rows, err = QuerySQL(ctx, nil, query, args...)
	if err != nil {
		return
	}
	var (
		s string
		i int64
	)
	for rows.Next() {
		err = rows.Scan(&s, &i)
		if err != nil {
			return
		}
		sa = append(sa, s)
		ia = append(ia, i)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	_ = rows.Close()
	return
}

// GetEnrollments - returns enrollments for a given uuid in a given date, possibly multiple
// uses cache with date resolution (uuid,dt.YYYYMMDD)
func GetEnrollments(ctx *Ctx, ds DS, uuid string, dt time.Time, single bool) (orgs []string) {
	sSep := "m"
	if single {
		sSep = "s"
	}
	k := uuid + sSep + ToYMDDate(dt)
	if MT {
		rollsCacheMtx.RLock()
	}
	orgs, ok := rollsCache[k]
	if MT {
		rollsCacheMtx.RUnlock()
	}
	if ok {
		return
	}
	defer func() {
		if MT {
			rollsCacheMtx.Lock()
		}
		rollsCache[k] = orgs
		if MT {
			rollsCacheMtx.Unlock()
		}
	}()
	pSlug := ctx.ProjectSlug
	// Step 1: Try project slug first
	// in single mode, if multiple companies are found, return the most recent
	// in multiple mode this can return many different companies and this is ok
	if pSlug != "" {
		rows := QueryToStringArray(
			ctx,
			"select distinct o.name from enrollments e, organizations o where e.organization_id = o.id and e.uuid = ? and e.project_slug = ? and e.start <= ? and e.end > ? order by e.id desc",
			uuid,
			pSlug,
			dt,
			dt,
		)
		if single {
			if len(rows) > 0 {
				orgs = []string{rows[0]}
				return
			}
		} else {
			orgs = append(orgs, rows...)
		}
	}
	// Step 2: Try foundation-f (for example cncf/* --> cncf-f)
	// in single mode, if multiple companies are found, return the most recent
	// in multiple mode this can return many different companies and this is ok
	if pSlug != "" && len(orgs) == 0 {
		ary := strings.Split(pSlug, "/")
		if len(ary) > 1 {
			slugF := ary[0] + "-f"
			rows, ids := QueryToStringIntArrays(
				ctx,
				"select o.name, max(e.id) from enrollments e, organizations o where e.organization_id = o.id and e.uuid = ? and e.project_slug = ? and e.start <= ? and e.end > ? group by o.name order by e.id desc",
				uuid,
				slugF,
				dt,
				dt,
			)
			if single {
				if len(rows) > 0 {
					orgs = []string{rows[0]}
					_ = SetDBSessionOrigin(ctx)
					_, _ = ExecSQL(
						ctx,
						nil,
						"insert ignore into enrollments(start, end, uuid, organization_id, project_slug, role) select start, end, uuid, organization_id, ?, ? from enrollments where id = ?",
						pSlug,
						"Contributor",
						ids[0],
					)
					return
				}
			} else {
				orgs = append(orgs, rows...)
			}
		}
	}
	// Step 3: try global second, only if no project specific were found
	// in single mode, if multiple companies are found, return the most recent
	// in multiple mode this can return many different companies and this is ok
	if len(orgs) == 0 {
		rows := QueryToStringArray(
			ctx,
			"select distinct o.name from enrollments e, organizations o where e.organization_id = o.id and e.uuid = ? and e.project_slug is null and e.start <= ? and e.end > ? order by e.id desc",
			uuid,
			dt,
			dt,
		)
		if single {
			if len(rows) > 0 {
				orgs = []string{rows[0]}
				return
			}
		} else {
			orgs = append(orgs, rows...)
		}
	}
	// Step 4: try anything from the same foundation, only if nothing is found so far
	// in single mode, if multiple companies are found, return the most recent
	// in multiple mode this can return many different companies and this is ok
	if pSlug != "" && len(orgs) == 0 {
		ary := strings.Split(pSlug, "/")
		if len(ary) > 1 {
			slugLike := ary[0] + "/%"
			rows, ids := QueryToStringIntArrays(
				ctx,
				"select o.name, max(e.id) from enrollments e, organizations o where e.organization_id = o.id and e.uuid = ? and e.project_slug like ? and e.start <= ? and e.end > ? group by o.name order by e.id desc",
				uuid,
				slugLike,
				dt,
				dt,
			)
			if single {
				if len(rows) > 0 {
					orgs = []string{rows[0]}
					_ = SetDBSessionOrigin(ctx)
					_, _ = ExecSQL(
						ctx,
						nil,
						"insert ignore into enrollments(start, end, uuid, organization_id, project_slug, role) select start, end, uuid, organization_id, ?, ? from enrollments where id = ?",
						pSlug,
						"Contributor",
						ids[0],
					)
					return
				}
			} else {
				orgs = append(orgs, rows...)
			}
		}
	}
	// Step 5: try anything else, only if nothing is found so far
	// in single mode, if multiple companies are found, return the most recent
	// in multiple mode this can return many different companies and this is ok
	if len(orgs) == 0 {
		rows, ids := QueryToStringIntArrays(
			ctx,
			"select o.name, max(e.id) from enrollments e, organizations o where e.organization_id = o.id and e.uuid = ? and e.start <= ? and e.end > ? group by o.name order by e.id desc",
			uuid,
			dt,
			dt,
		)
		if single {
			if len(rows) > 0 {
				orgs = []string{rows[0]}
				if pSlug != "" {
					_ = SetDBSessionOrigin(ctx)
					_, _ = ExecSQL(
						ctx,
						nil,
						"insert ignore into enrollments(start, end, uuid, organization_id, project_slug, role) select start, end, uuid, organization_id, ?, ? from enrollments where id = ?",
						pSlug,
						"Contributor",
						ids[0],
					)
				}
				return
			}
		} else {
			orgs = append(orgs, rows...)
		}
	}
	return
}

// GetEnrollmentsSingle - returns org name (or Unknown) for given uuid and date
func GetEnrollmentsSingle(ctx *Ctx, ds DS, uuid string, dt time.Time) (org string) {
	orgs := GetEnrollments(ctx, ds, uuid, dt, true)
	if len(orgs) == 0 {
		org = Unknown
		return
	}
	org = orgs[0]
	return
}

// GetEnrollmentsMulti - returns org name(s) for given uuid and name
// Returns 1 or more organizations (all that matches the current date)
// If none matches it returns array [Unknown]
func GetEnrollmentsMulti(ctx *Ctx, ds DS, uuid string, dt time.Time) (orgs []string) {
	orgs = GetEnrollments(ctx, ds, uuid, dt, false)
	if len(orgs) == 0 {
		orgs = append(orgs, Unknown)
	}
	return
}

// CopyAffsRoleData - copy affiliations fields from source role to dest role
func CopyAffsRoleData(dst, src map[string]interface{}, dstRole, srcRole string) {
	for _, suff := range AffsFields {
		dst[dstRole+suff], _ = Dig(src, []string{srcRole + suff}, false, true)
	}
	dst[dstRole+MultiOrgNames], _ = Dig(src, []string{srcRole + MultiOrgNames}, false, true)
}

// IdenityAffsData - add affiliations related data
// identity - full identity
// aid identity ID value (which is uuid), for example from "author_id", "creator_id" etc.
// either identity or aid must be specified
func IdenityAffsData(ctx *Ctx, ds DS, identity map[string]interface{}, aid interface{}, dt time.Time, role string) (outItem map[string]interface{}, empty bool) {
	outItem = EmptyAffsItem(role, false)
	var uuid interface{}
	if identity != nil {
		ids := AffsIdentityIDs(ctx, ds, identity)
		outItem[role+"_id"] = ids[0]
		outItem[role+"_uuid"] = ids[1]
		name, _ := identity["name"]
		sName, _ := name.(string)
		if name == nil || sName == Nil {
			outItem[role+"_name"] = ""
		} else {
			outItem[role+"_name"] = name
		}
		username, _ := identity["username"]
		sUsername, _ := username.(string)
		if username == nil || sUsername == Nil {
			outItem[role+"_user_name"] = ""
		} else {
			outItem[role+"_user_name"] = username
		}
		outItem[role+"_domain"] = IdentityAffsDomain(identity)
		uuid = ids[1]
	}
	if aid != nil {
		outItem[role+"_id"] = aid
		uuid = GetIdentityUUID(ctx, ds, aid.(string))
		outItem[role+"_uuid"] = uuid
	}
	if uuid == nil {
		outItem = EmptyAffsItem(role, true)
		empty = true
		return
	}
	suuid, _ := uuid.(string)
	profile, err := FindObject(ctx, "profiles", "uuid", suuid, []string{"name", "email", "gender", "gender_acc", "is_bot"})
	isBot := 0
	if aid != nil && profile == nil {
		Printf("warning cannot find profile for identity id %v\n", aid)
	}
	if err == nil && profile != nil {
		pName, _ := profile["name"]
		if pName != nil {
			outItem[role+"_name"] = pName
		}
		email, _ := profile["email"]
		if email != nil {
			ary := strings.Split(email.(string), "@")
			if len(ary) > 1 {
				outItem[role+"_domain"] = ary[1]
			}
		}
		gender, _ := profile["gender"]
		if gender != nil {
			outItem[role+"_gender"] = gender
		} else {
			outItem[role+"_gender"] = Unknown
		}
		genderAcc, _ := profile["gender_acc"]
		if genderAcc != nil {
			outItem[role+"_gender_acc"] = genderAcc
		} else {
			outItem[role+"_gender_acc"] = nil
		}
		bot, ok := profile["is_bot"].(int64)
		if ok && bot > 0 {
			isBot = 1
		}
	}
	gender, ok := outItem[role+"_gender"]
	if !ok || gender == nil {
		outItem[role+"_gender"] = Unknown
		// outItem[role+"_gender_acc"] = 0
		outItem[role+"_gender_acc"] = nil
	}
	if isBot == 0 {
		outItem[role+"_bot"] = false
	} else {
		outItem[role+"_bot"] = true
	}
	outItem[role+"_org_name"] = GetEnrollmentsSingle(ctx, ds, suuid, dt)
	outItem[role+MultiOrgNames] = GetEnrollmentsMulti(ctx, ds, suuid, dt)
	return
}

// AffsDataForRoles - return affs data for given roles
func AffsDataForRoles(ctx *Ctx, ds DS, rich map[string]interface{}, roles []string) (data map[string]interface{}) {
	/*
		defer func() {
			Printf("AffsDataForRoles: %+v --> %+v\n", roles, data)
		}()
	*/
	data = make(map[string]interface{})
	authorField := ds.RichAuthorField(ctx)
	if len(roles) == 0 {
		roles = append(roles, authorField)
	}
	dateField := ds.DateField(ctx)
	idt, ok := rich[dateField]
	if !ok {
		Printf("cannot read %s from %v\n", dateField, DumpKeys(rich))
		return
	}
	date, err := TimeParseInterfaceString(idt)
	if err != nil {
		Printf("cannot parse date %v\n", idt)
		return
	}
	var idAuthor interface{}
	for _, role := range roles {
		roleID := role + "_id"
		id, ok := Dig(rich, []string{roleID}, false, true)
		if !ok || id == nil {
			if ctx.Debug > 1 {
				Printf("no %s role in %v (or nil), skipping\n", roleID, DumpKeys(rich))
			}
			continue
		}
		if role == authorField {
			idAuthor = id
		}
		affsIdentity, empty := IdenityAffsData(ctx, ds, nil, id, date, role)
		if empty {
			Printf("no identity affiliation data for %s id %+v\n", role, id)
			continue
		}
		for prop, value := range affsIdentity {
			data[prop] = value
		}
	}
	if idAuthor != nil && authorField != Author {
		affsIdentity, empty := IdenityAffsData(ctx, ds, nil, idAuthor, date, Author)
		if !empty {
			for prop, value := range affsIdentity {
				data[prop] = value
			}
		} else {
			Printf("no identity affiliation data for author role id %+v\n", idAuthor)
		}
	}
	return
}
