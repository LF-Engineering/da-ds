package dads

import (
	"database/sql"
	"fmt"
	"net/url"
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
	if id == "" {
		return
	}
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
// uses cache with date resolution (pslug,uuid,dt.YYYYMMDD)
func GetEnrollments(ctx *Ctx, ds DS, uuid string, dt time.Time, single bool) (orgs []string, e error) {
	pSlug := ctx.ProjectSlug
	sSep := "m"
	if single {
		sSep = "s"
	}
	k := pSlug + uuid + sSep + ToYMDDate(dt)
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
	if pSlug == "" {
		pSlug = "(empty)"
	}
	pSlug = url.QueryEscape(pSlug)
	api := "single"
	if !single {
		api = "multi"
	}
	sdt := url.QueryEscape(ToYMDTHMSZDate(dt))
	data, err := ExecuteAffiliationsAPICall(ctx, "GET", fmt.Sprintf("/v1/affiliation/%s/%s/%s/%s", pSlug, api, uuid, sdt), true)
	if err != nil {
		Printf("GetEnrollments(%s,%s,%s,%s) error: %v\n", pSlug, api, uuid, sdt, err)
		e = err
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
	if single {
		orgs = []string{data["org"].(string)}
		return
	}
	orgsI, _ := data["orgs"].([]interface{})
	for _, orgI := range orgsI {
		orgs = append(orgs, orgI.(string))
	}
	return
}

// GetEnrollmentsBoth - returns org name(s) for given uuid and name
// returns data returned by bot GetEnrollmentsSingle and GetEnrollmentsMulti
// by using a single HTTP request when both were not yet called for a given key
func GetEnrollmentsBoth(ctx *Ctx, ds DS, uuid string, dt time.Time) (org string, orgs []string, e error) {
	pSlug := ctx.ProjectSlug
	kS := pSlug + uuid + "s" + ToYMDDate(dt)
	kM := pSlug + uuid + "m" + ToYMDDate(dt)
	if MT {
		rollsCacheMtx.RLock()
	}
	orgsS, okS := rollsCache[kS]
	orgsM, okM := rollsCache[kM]
	if MT {
		rollsCacheMtx.RUnlock()
	}
	if okS {
		if len(orgsS) == 0 {
			org = Unknown
		} else {
			org = orgsS[0]
		}
	}
	if okM {
		orgs = orgsM
		if len(orgs) == 0 {
			orgs = append(orgs, Unknown)
		}
	}
	if okS && okM {
		return
	}
	if okS && !okM {
		orgs, e = GetEnrollmentsMulti(ctx, ds, uuid, dt)
		return
	}
	if !okS && okM {
		org, e = GetEnrollmentsSingle(ctx, ds, uuid, dt)
		return
	}
	if pSlug == "" {
		pSlug = "(empty)"
	}
	pSlug = url.QueryEscape(pSlug)
	sdt := url.QueryEscape(ToYMDTHMSZDate(dt))
	data, err := ExecuteAffiliationsAPICall(ctx, "GET", fmt.Sprintf("/v1/affiliation/%s/both/%s/%s", pSlug, uuid, sdt), true)
	if err != nil {
		Printf("GetEnrollmentsBoth(%s,%s,%s) error: %v\n", pSlug, uuid, sdt, err)
		e = err
		return
	}
	defer func() {
		if MT {
			rollsCacheMtx.Lock()
		}
		rollsCache[kS] = orgsS
		rollsCache[kM] = orgsM
		if MT {
			rollsCacheMtx.Unlock()
		}
	}()
	org, _ = data["org"].(string)
	orgsS = []string{org}
	orgsI, _ := data["orgs"].([]interface{})
	for _, orgI := range orgsI {
		orgsM = append(orgsM, orgI.(string))
	}
	orgs = orgsM
	return
}

// GetEnrollmentsSingle - returns org name (or Unknown) for given uuid and date
func GetEnrollmentsSingle(ctx *Ctx, ds DS, uuid string, dt time.Time) (org string, e error) {
	var orgs []string
	orgs, e = GetEnrollments(ctx, ds, uuid, dt, true)
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
func GetEnrollmentsMulti(ctx *Ctx, ds DS, uuid string, dt time.Time) (orgs []string, e error) {
	orgs, e = GetEnrollments(ctx, ds, uuid, dt, false)
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

// IdentityAffsData - add affiliations related data
// identity - full identity
// aid identity ID value (which is uuid), for example from "author_id", "creator_id" etc.
// either identity or aid must be specified
func IdentityAffsData(ctx *Ctx, ds DS, identity map[string]interface{}, aid interface{}, dt time.Time, role string) (outItem map[string]interface{}, empty bool, e error) {
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
	//outItem[role+"_org_name"], e = GetEnrollmentsSingle(ctx, ds, suuid, dt)
	//outItem[role+MultiOrgNames], e = GetEnrollmentsMulti(ctx, ds, suuid, dt)
	outItem[role+"_org_name"], outItem[role+MultiOrgNames], e = GetEnrollmentsBoth(ctx, ds, suuid, dt)
	return
}

// AffsDataForRoles - return affs data for given roles
func AffsDataForRoles(ctx *Ctx, ds DS, rich map[string]interface{}, roles []string) (data map[string]interface{}, e error) {
	/*
		defer func() {
			Printf("AffsDataForRoles: %+v --> %+v,%v\n", roles, data, e)
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
		affsIdentity, empty, e := IdentityAffsData(ctx, ds, nil, id, date, role)
		if e != nil {
			Printf("AffsDataForRoles: IdentityAffsData error: %v for %s id %d\n", role, id)
		}
		if empty {
			Printf("no identity affiliation data for %s id %+v\n", role, id)
			continue
		}
		for prop, value := range affsIdentity {
			data[prop] = value
		}
	}
	if idAuthor != nil && authorField != Author {
		affsIdentity, empty, e := IdentityAffsData(ctx, ds, nil, idAuthor, date, Author)
		if e != nil {
			Printf("AffsDataForRoles: IdentityAffsData error: %v\n")
		}
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
