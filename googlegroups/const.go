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
	GoogleGroupRichMapping = []byte(`{"mappings":{"dynamic_templates":[{"notanalyzed":{"match":"*","match_mapping_type":"string","mapping":{"type":"keyword"}}},{"int_to_float":{"match":"*","match_mapping_type":"long","mapping":{"type":"float"}}},{"formatdate":{"match":"*","match_mapping_type":"date","mapping":{"format":"strict_date_optional_time||epoch_millis","type":"date"}}}]}}`)
	// GoogleGroupRawMapping ...
	GoogleGroupRawMapping = []byte(`{"mappings":{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}}`)
	// base64RE ...
	base64RE = regexp.MustCompile("^([a-zA-Z0-9+/]+\\r\\n)+[a-zA-Z0-9+/]+={0,2}$")
	// DefaultDateTime ...
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)
