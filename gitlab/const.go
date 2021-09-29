package gitlab

import "time"

const (
	GITLAB_API_VERSION = "v4"
	GITLAB_API_BASE    = "https://gitlab.com/api"
	DATASOURCE         = "gitlab"
	Gitlab             = "gitlab"
	Unknown            = "Unknown"
)

var (
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	// GitlabRawMapping
	GitlabRawMapping = []byte(`{"mappings":{"dynamic":true,"properties":{"metadata__updated_on":{"type":"date"},"data":{"properties":{"body":{"dynamic":false,"properties":{}}}}}}}`)
	// GitlabRichMapping ...
	GitlabRichMapping = []byte(`{
		"mappings": {
			"dynamic_templates": [
			  {
				"notanalyzed": {
				  "match": "*",
				  "match_mapping_type": "string",
				  "mapping": {
					"type": "keyword"
				  }
				}
			  },
			  {
				"formatdate": {
				  "match": "*",
				  "match_mapping_type": "date",
				  "mapping": {
					"format": "strict_date_optional_time||epoch_millis",
					"type": "date"
				  }
				}
			  }
			],
			"properties": { 
			  "metadata__updated_on": {
				"type": "date"
			  }
			}
		}
	  }`)
)
