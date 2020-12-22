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
	BugzillaRestEnrichMapping = []byte(`{"mappings":
{"properties":
{
  "metadata__updated_on":{"type":"date"},
  "metadata__timestamp":{"type":"date"},
  "metadata__enriched_on":{"type":"date"},
  "metadata__backend_name":{"type":"date"},
  "creation_date":{"type":"date"},
  "creation_ts":{"type":"date"},
  "delta_ts":{"type":"date"},
  "main_description":{"type":"text","index":true},
  "main_description_analyzed":{"type":"text","index":true},
  "uuid":{"type":"keyword"},
  "creator_detail_id":{"type":"keyword"},
  "creator_detail_uuid":{"type":"keyword"},
  "author_id":{"type":"keyword"},
  "author_uuid":{"type":"keyword"},
  "assigned_to_detail_id":{"type":"keyword"},
  "assigned_to_detail_uuid":{"type":"keyword"},
  "assigned_to_id":{"type":"keyword"},
  "assigned_to_uuid":{"type":"keyword"},
  "priority":{"type":"keyword"},
  "severity":{"type":"keyword"},
  "status":{"type":"keyword"},
  "project":{"type":"keyword"},
  "product":{"type":"keyword"},
  "origin":{"type":"keyword"},
  "metadata__backend_version":{"type":"keyword"},
  "id": {"type":"keyword"}
}}}`)
)

