package googlegroups

import "time"

// GoogleGroupMessages ...
type GoogleGroupMessages struct {
	Messages []*GoogleGroupMessageThread
}

// GoogleGroupMessageThread ...
type GoogleGroupMessageThread struct {
	Topic    string                `json:"topic"`
	ID       string                `json:"id"`
	Messages []*GoogleGroupMessage `json:"messages"`
}

// GoogleGroupMessage ...
type GoogleGroupMessage struct {
	ID      string `json:"id"`
	Author  string `json:"author"`
	Date    string `json:"date"`
	File    string `json:"file"`
	Message string `json:"message"`
}

// EnrichedMessage ...
type EnrichedMessage struct {
	From                 string    `json:"from"`
	Date                 time.Time `json:"date"`
	To                   string    `json:"to"`
	MessageID            string    `json:"message_id"`
	InReplyTo            string    `json:"in_reply_to"`
	References           string    `json:"references"`
	Subject              string    `json:"subject"`
	MessageBody          string    `json:"message_body"`
	TopicID              string    `json:"topic_id"`
	BackendVersion       string    `json:"backend_version"`
	UUID                 string    `json:"uuid"`
	Origin               string    `json:"origin"`
	UpdatedOn            float64   `json:"updated_on"`
	MetadataUpdatedOn    time.Time `json:"metadata__updated_on"`
	BackendName          string    `json:"backend_name"`
	MetadataTimestamp    time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn   time.Time `json:"metadata_enriched_on"`
	Timestamp            float64   `json:"timestamp"`
	ProjectSlug          string    `json:"project_slug"`
	GroupName            string    `json:"group_name"`
	Project              string    `json:"project"`
	Root                 bool      `json:"root"`
	FromBot              bool      `json:"from_bot"`
	ChangedAt            time.Time `json:"changed_at"`
	AuthorID             string    `json:"author_id"`
	AuthorUUID           string    `json:"author_uuid"`
	AuthorGender         string    `json:"author_gender"`
	AuthorOrgName        string    `json:"author_org_name"`
	AuthorUserName       string    `json:"author_user_name"`
	AuthorBot            bool      `json:"author_bot"`
	FromGenderAcc        int       `json:"from_gender_acc"`
	MboxAuthorDomain     string    `json:"mbox_author_domain"`
	IsGoogleGroupMessage int       `json:"is_google_group_message"`
	FromGender           string    `json:"from_gender"`
	FromMultipleOrgNames string    `json:"from_multiple_org_names"`
	FromOrgName          string    `json:"from_org_name"`
}

// RawMessage represents GoogleGroups raw message
type RawMessage struct {
	From              string    `json:"from"`
	Date              time.Time `json:"date"`
	To                string    `json:"to"`
	MessageID         string    `json:"message_id"`
	InReplyTo         string    `json:"in_reply_to"`
	References        string    `json:"references"`
	Subject           string    `json:"subject"`
	MessageBody       string    `json:"message_body"`
	TopicID           string    `json:"topic_id"`
	BackendVersion    string    `json:"backend_version"`
	UUID              string    `json:"uuid"`
	Origin            string    `json:"origin"`
	UpdatedOn         float64   `json:"updated_on"`
	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	BackendName       string    `json:"backend_name"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	Timestamp         float64   `json:"timestamp"`
	ProjectSlug       string    `json:"project_slug"`
	GroupName         string    `json:"group_name"`
	Project           string    `json:"project"`
	ChangedAt         time.Time `json:"changed_at"`
}

// RawHits result
type RawHits struct {
	Hits NHits `json:"hits"`
}

// NHits result
type NHits struct {
	Hits []NestedRawHits `json:"hits"`
}

// NestedRawHits is the actual hit data
type NestedRawHits struct {
	ID     string     `json:"_id"`
	Source RawMessage `json:"_source"`
}
