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
	BugzillaEnrichMapping = []byte(`{"mappings":{"dynamic_templates":[{"notanalyzed":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"int_to_float":{"match":"*","match_mapping_type":"long","mapping":{"type":"float"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"format":"strict_date_optional_time||epoch_millis","type":"date"}}}]}}`)
)
