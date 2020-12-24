package pipermail

var (
	// PipermailRawMapping - Pipeermail raw index mapping
	PipermailRawMapping = []byte(`{"mappings": {"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"description":{"type":"text","index":true},"full_description":{"type":"text","index":true}}}}}}`)
)
