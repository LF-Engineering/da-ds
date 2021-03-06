package googlegroups

import (
	"regexp"
	"time"
)

const (
	// GoogleGroups ...
	GoogleGroups = "GoogleGroups"
	// malformedMIMEHeaderLineErrorMessage
	malformedMIMEHeaderLineErrorMessage = "malformed MIME header line: "
	// archivesBasePath ...
	archivesBasePath = "/Users/code/da-ds/googlegroups/archives"
	// jsonFilesBasePath ...
	jsonFilesBasePath = "/Users/code/da-ds/googlegroups/jsonfiles/"
	// jsonExtension ...
	jsonExtension = ".json"
	// script ...
	script = "./googlegroups/cmd.sh"
	// Unknown ...
	Unknown = "Unknown"
)

var (
	// GoogleGroupRichMapping ...
	GoogleGroupRichMapping = []byte(`{"mappings":{"properties":{"metadata__updated_on":{"type":"date"},"Subject_analyzed":{"type":"text","fielddata":true,"index":true},"body":{"type":"text","index":true}}}}`)
	// GoogleGroupRawMapping ...
	GoogleGroupRawMapping = []byte(`{"mappings":{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}}`)
	// base64RE ...
	base64RE = regexp.MustCompile("^([a-zA-Z0-9+/]+\\r\\n)+[a-zA-Z0-9+/]+={0,2}$")
	// DefaultDateTime ...
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)
