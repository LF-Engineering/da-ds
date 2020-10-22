package dockerhub

var (
	// DockerhubSearchFields - extra search fields
	DockerhubSearchFields = map[string][]string{
		"name":      {"name"},
		"namespace": {"namespace"},
	}

	// DockerhubRawMapping - Dockerhub raw index mapping
	DockerhubRawMapping = []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}}`)

	// DockerhubRichMapping - Dockerhub rich index mapping
	DockerhubRichMapping = []byte(`{"mappings": {"properties":{"metadata__updated_on":{"type":"date"},"description":{"type":"text","index":true},"description_analyzed":{"type":"text","index":true},"full_description_analyzed":{"type":"text","index":true}}}}`)
)
