package pipermail

// MessageSearchFields ...
type MessageSearchFields struct {
	Name   string `json:"name"`
	ItemID string `json:"item_id"`
}

// RawMessage represents piper mail raw message
type RawMessage struct {
	BackendVersion    string               `json:"backend_version"`
	Data              interface{}          `json:"data"`
	Tag               string               `json:"tag"`
	UUID              string               `json:"uuid"`
	SearchFields      *MessageSearchFields `json:"search_fields"`
	Origin            string               `json:"origin"`
	UpdatedOn         float64              `json:"updated_on"`
	MetadataUpdatedOn string               `json:"metadata__updated_on"`
	BackendName       string               `json:"backend_name"`
	MetadataTimestamp string               `json:"metadata__timestamp"`
	Timestamp         float64              `json:"timestamp"`
	Category          string               `json:"category"`
	ProjectSlug       string               `json:"project_slug"`
	GroupName         string               `json:"group_name"`
}
