package googlegroups

import (
	"regexp"
	"time"
)

const (
	// GoogleGroups ...
	GoogleGroups = "googlegroups"
	// malformedMIMEHeaderLineErrorMessage
	malformedMIMEHeaderLineErrorMessage = "malformed MIME header line: "
	// Unknown ...
	Unknown = "Unknown"
	// CredentialsSSMParamName from ssm
	CredentialsSSMParamName = "insights_googlegroups_credentials"
	// TokenSSMParamName from ssm
	TokenSSMParamName = "insights_googlegroups_token"
	// MaxNumberOfMessages from gmail
	MaxNumberOfMessages = 10000000
)

var (
	// GoogleGroupRichMapping ...
	GoogleGroupRichMapping = []byte(`{"mappings":{"properties":{"author_bot":{"type":"boolean"},"author_id":{"type":"keyword"},"author_multi_org_names":{"type":"keyword"},"author_name":{"type":"keyword"},"author_org_name":{"type":"keyword"},"author_user_name":{"type":"keyword"},"author_uuid":{"type":"keyword"},"backend_name":{"type":"keyword"},"backend_version":{"type":"keyword"},"changed_at":{"type":"date"},"date":{"type":"date"},"from":{"type":"keyword"},"from_bot":{"type":"boolean"},"group_name":{"type":"keyword"},"in_reply_to":{"type":"keyword"},"is_google_group_message":{"type":"long"},"mbox_author_domain":{"type":"keyword"},"message_body":{"type":"keyword"},"message_id":{"type":"keyword"},"metadata__enriched_on":{"type":"date"},"metadata__timestamp":{"type":"date"},"metadata__updated_on":{"type":"date"},"origin":{"type":"keyword"},"project":{"type":"keyword"},"project_slug":{"type":"keyword"},"references":{"type":"keyword"},"root":{"type":"boolean"},"subject":{"type":"keyword"},"timezone":{"type":"long"},"to":{"type":"keyword"},"topic":{"type":"keyword"},"topic_id":{"type":"keyword"},"uuid":{"type":"keyword"}}}}`)
	// GoogleGroupRawMapping ...
	GoogleGroupRawMapping = []byte(`{"mappings":{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}}`)
	// base64RE ...
	base64RE = regexp.MustCompile("^([a-zA-Z0-9+/]+\\r\\n)+[a-zA-Z0-9+/]+={0,2}$")
	// DefaultDateTime ...
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)
