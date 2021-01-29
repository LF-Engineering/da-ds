package finosmeetings

var (

	// FinosmeetingsRawMapping - Dockerhub raw index mapping
	FinosmeetingsRawMapping = []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}}`)

	// FinosmeetingsRichMapping - Dockerhub rich index mapping
	FinosmeetingsRichMapping = []byte(`{"mappings": {"properties":{"metadata__updated_on":{"type":"date"},"description":{"type":"text","index":true},"description_analyzed":{"type":"text","index":true},"full_description_analyzed":{"type":"text","index":true}}}}`)
)
