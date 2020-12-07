package bugzilla

var (

	// BugzillaRawMapping - bugzilla raw index mapping
	BugzillaRawMapping = []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},{"short_description":{"type":"text","index":true}}}}}`)
)

