package bugzillarest

var (

	// BugzillaRestRawMapping - bugzilla raw index mapping
	BugzillaRestRawMapping = []byte(`{
  "mappings": 
{"dynamic":true,
"properties":{
"metadata__updated_on":{"type":"date"},
"metadata__timestamp":{"type":"date"},
"updated_on":{"type":"date"},
"timestamp":{"type":"date"},
"short_description":{"type":"text","index":true},
"backend_version":{"type":"keyword"},
"backend_name":{"type":"keyword"},
"status":{"type":"keyword"},
"priority":{"type":"keyword"},
"severity":{"type":"keyword"},
"uuid":{"type": "keyword"},
"origin":{"type":"keyword"},
"tag":{"type":"keyword"}
}}
}`)

	// BugzillaRestEnrichMapping - bugzilla rest enriched index mapping
	BugzillaRestEnrichMapping = []byte(`{"mappings":{"dynamic_templates":[{"notanalyzed":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"int_to_float":{"match":"*","match_mapping_type":"long","mapping":{"type":"float"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"format":"strict_date_optional_time||epoch_millis","type":"date"}}}]}}`)
)
