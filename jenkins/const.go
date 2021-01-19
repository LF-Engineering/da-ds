package jenkins

import "time"

var (
	// Jenkins represents the name of data source
	Jenkins = "jenkins"
	// Depth is the attribute to be passed onto the /api/json
	Depth = 1
	// BuildCategory is the default category for jenkins build
	BuildCategory = "build"
	// DefaultDateTime is the default time used when no time is provided
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)
