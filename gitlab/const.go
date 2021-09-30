package gitlab

import "time"

const (
	// GitlabAPIVersion ...
	GitlabAPIVersion = "v4"
	//GitlabAPIBase ...
	GitlabAPIBase = "https://gitlab.com/api"
	//Gitlab datasource name
	Gitlab = "gitlab"
	//Unknown ...
	Unknown = "Unknown"
)

var (
	// DefaultDateTime ...
	DefaultDateTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	// GitlabRawMapping ...
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
