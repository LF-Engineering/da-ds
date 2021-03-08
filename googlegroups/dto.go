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
	To                   []string  `json:"to"`
	MessageID            string    `json:"message_id"`
	InReplyTo            string    `json:"in_reply_to"`
	References           string    `json:"references"`
	Subject              string    `json:"subject"`
	Topic                string    `json:"topic"`
	MessageBody          string    `json:"message_body"`
	TopicID              string    `json:"topic_id"`
	BackendVersion       string    `json:"backend_version"`
	UUID                 string    `json:"uuid"`
	Origin               string    `json:"origin"`
	MetadataUpdatedOn    time.Time `json:"metadata__updated_on"`
	BackendName          string    `json:"backend_name"`
	MetadataTimestamp    time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn   time.Time `json:"metadata__enriched_on"`
	ProjectSlug          string    `json:"project_slug"`
	GroupName            string    `json:"group_name"`
	Project              string    `json:"project"`
	Root                 bool      `json:"root"`
	FromBot              bool      `json:"from_bot"`
	ChangedAt            time.Time `json:"changed_at"`
	AuthorName           string    `json:"author_name"`
	AuthorID             string    `json:"author_id"`
	AuthorUUID           string    `json:"author_uuid"`
	AuthorOrgName        string    `json:"author_org_name"`
	AuthorUserName       string    `json:"author_user_name"`
	AuthorBot            bool      `json:"author_bot"`
	AuthorMultiOrgNames  []string  `json:"author_multi_org_names"`
	MboxAuthorDomain     string    `json:"mbox_author_domain"`
	IsGoogleGroupMessage int       `json:"is_google_group_message"`
	Timezone             int       `json:"timezone"`
}

// RawMessage represents GoogleGroups raw message
type RawMessage struct {
	From              string    `json:"from"`
	Date              time.Time `json:"date"`
	To                []string  `json:"to"`
	MessageID         string    `json:"message_id"`
	InReplyTo         string    `json:"in_reply_to"`
	References        string    `json:"references"`
	Subject           string    `json:"subject"`
	MessageBody       string    `json:"message_body"`
	TopicID           string    `json:"topic_id"`
	Topic             string    `json:"topic"`
	BackendVersion    string    `json:"backend_version"`
	UUID              string    `json:"uuid"`
	Origin            string    `json:"origin"`
	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	BackendName       string    `json:"backend_name"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	ProjectSlug       string    `json:"project_slug"`
	GroupName         string    `json:"group_name"`
	Project           string    `json:"project"`
	ChangedAt         time.Time `json:"changed_at"`
	Timezone          int       `json:"timezone"`
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

// HeadersData struct
type HeadersData struct {
	// Date is the date the message was originally sent
	Date string
	// MessageID is the message id
	MessageID string
	// InReplyTo is who the email was sent to. This can contain multiple
	// addresses if the email was forwarded.
	InReplyTo string
	// References
	References string
	// Sender is the entity that originally created and sent the message
	Sender string
	// From is the name - email address combo of the email author
	From string
	// Subject is the subject of the email
	Subject string
	// To is the email recipient.
	To []string
	// DeliveredTo is to whom the email was sent to. This can contain multiple
	// addresses if the email was forwarded.
	DeliveredTo []string
}
