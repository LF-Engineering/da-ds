package googlegroups

const (
	// GoogleGroups ...
	GoogleGroups = "GoogleGroups"
)

var (
	// GoogleGroupRichMapping ...
	GoogleGroupRichMapping = []byte(`{"mappings":{"properties":{"metadata__updated_on":{"type":"date"},"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}}`)
)
