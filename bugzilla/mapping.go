package bugzilla

var (

	// BugzillaRawMapping - bugzilla raw index mapping
	BugzillaRawMapping = []byte(`{"mappings": 
{"dynamic":true,
"properties":{
"metadata__updated_on":{"type":"date"},
"metadata__timestamp":{"type":"date"},
"creation_ts":{"type":"date"},
"changed_at":{"type":"date"},
"delta_ts":{"type":"date"},
"short_description":{"type":"text","index":true},
"backend_version":{"type":"keyword"},
"backend_name":{"type":"keyword"},
"bug_status":{"type":"keyword"},
  "priority":{"type":"keyword"},
  "severity":{"type":"keyword"}
}}}`)

	// BugzillaEnrichMapping - bugzilla enriched index mapping
	BugzillaEnrichMapping = []byte(`{"mappings":
{"properties":
{
  "metadata__updated_on":{"type":"date"},
  "metadata__timestamp":{"type":"date"},
  "metadata__enriched_on":{"type":"date"},
  "changed_date":{"type":"date"},
  "creation_date":{"type":"date"},
  "delta_ts":{"type":"date"},
  "main_description":{"type":"text","index":true},
  "main_description_analyzed":{"type":"text","index":true},
  "full_description_analyzed":{"type":"text","index":true},
  "uuid":{"type":"keyword"},
  "reporter_id":{"type":"keyword"},
  "reporter_uuid":{"type":"keyword"},
  "author_id":{"type":"keyword"},
  "author_uuid":{"type":"keyword"},
  "assigned_to_id":{"type":"keyword"},
  "assigned_to_uuid":{"type":"keyword"},
  "priority":{"type":"keyword"},
  "severity":{"type":"keyword"},
  "status":{"type":"keyword"},
  "component":{"type":"keyword"},
  "assigned_to_name":{"type":"keyword"},
  "reporter_org_name":{"type":"keyword"},
  "author_org_name":{"type":"keyword"},
  "reporter_name":{"type":"keyword"},
  "metadata__backend_name":{"type":"keyword"},
  "assigned_to_org_name":{"type":"keyword"},
  "author_name":{"type":"keyword"},
  "url":{"type":"keyword"}
  }}}`)
)
