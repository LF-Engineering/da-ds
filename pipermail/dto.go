package pipermail

import "time"

// MessageSearchFields ...
type MessageSearchFields struct {
	Name   string `json:"name"`
	ItemID string `json:"item_id"`
}

// RawMessage represents piper mail raw message
type RawMessage struct {
	BackendVersion    string               `json:"backend_version"`
	Data              *RawMessageData       `json:"data"`
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

type RawMessageData struct {
	ContentType     string `json:"content_type,Content-Type"`
	Date            string `json:"date,Date"`
	From            string `json:"from,From"`
	InReplyTo       string `json:"in_reply_to,In-Reply-To"`
	MboxByteLength  int64  `json:"mbox_byte_length,MBox-Bytes-Length"`
	MboxNBodies     int    `json:"mbox_n_bodies,MBox-N-Bodies"`
	MboxNLines      int64  `json:"mbox_n_lines,MBox-N-Lines"`
	MboxProjectName string `json:"mbox_project_name,MBox-Project-Name"`
	MboxValid       bool   `json:"mbox_valid,MBox-Valid"`
	MboxWarn        bool   `json:"mbox_warn,MBox-Warn"`
	MessageID       string `json:"message_id,Message-ID"`
	References      string `json:"references,References"`
	Subject         string `json:"subject,Subject"`
	Data            struct {
		Text struct {
			Plain []struct {
				Data string `json:"data"`
			}
		} `json:"text"`
	} `json:"data"`
	DateInTZ string `json:"date_in_tz"`
	DateTZ   int    `json:"date_tz"`
}

// EnrichMessage represents piper mail enriched message
type EnrichMessage struct {
	ID                   string    `json:"id"`
	ProjectTS            int64     `json:"project_ts"`
	FromUserName         string    `json:"from_user_name"`
	TZ                   int64     `json:"tz"`
	MessageID            string    `json:"Message-ID"`
	UUID                 string    `json:"uuid"`
	AuthorName           string    `json:"author_name"`
	Root                 bool      `json:"root"`
	FromUUID             string    `json:"from_uuid"`
	AuthorGenderACC      int64     `json:"author_gender_acc"`
	FromName             string    `json:"from_name"`
	AuthorOrgName        string    `json:"author_org_name"`
	AuthorUserName       string    `json:"author_user_name"`
	AuthorBot            bool      `json:"author_bot"`
	BodyExtract          string    `json:"body_extract"`
	AuthorID             string    `json:"author_id"`
	SubjectAnalyzed      string    `json:"subject_analyzed"`
	FromBot              bool      `json:"from_bot"`
	Project              string    `json:"project"`
	MboxAuthorDomain     string    `json:"mbox_author_domain"`
	Date                 string    `json:"date"`
	IsPipermailMessage   int       `json:"is_pipermail_message"`
	FromGender           string    `json:"from_gender"`
	FromMultipleOrgNames []string  `json:"from_multiple_org_names"`
	FromOrgName          string    `json:"from_org_name"`
	FromDomain           string    `json:"from_domain"`
	List                 string    `json:"list"`
	AuthorUUID           string    `json:"author_uuid"`
	AuthorMultiOrgNames  []string  `json:"author_multi_org_names"`
	Origin               string    `json:"origin"`
	Size                 string    `json:"size"`
	Tag                  string    `json:"tag"`
	Subject              string    `json:"subject"`
	FromID               string    `json:"from_id"`
	AuthorGender         string    `json:"author_gender"`
	FromGenderAcc        string    `json:"from_gender_acc"`
	EmailDate            string    `json:"email_date"`
	MetadataTimestamp    time.Time `json:"metadata__timestamp"`
	MetadataBackendName  string    `json:"metadata__backend_name"`
	MetadataUpdatedOn    time.Time `json:"metadata__updated_on"`
	BackendVersion       string    `json:"backend_version"`
}
