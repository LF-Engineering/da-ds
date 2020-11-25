package bugzilla

import (
	"time"
)

// BugResponse data model represents Bugzilla get bugsList results
type BugResponse struct {
	ID               int       `json:"id"`
	Product          string    `json:"product"`
	Component        string    `json:"component"`
	AssignedTo       string    `json:"assigned_to"`
	Status           string    `json:"status"`
	Resolution       string    `json:"resolution"`
	ShortDescription string    `json:"short_description"`
	ChangedDate      time.Time `json:"changed_date"`
}

// BugResponse data model represents Bugzilla get bugDetail results
type BugDetailResponse struct {
	Bug BugDetailXML `xml:"bug"`
}

// BugDetailXML ...
type BugDetailXML struct {
	ID                 int    `xml:"bug_id"`
	CreationDate       string `xml:"creation_ts"`
	ShortDescription   string `xml:"short_desc"`
	DeltaTS            string `xml:"delta_ts"`
	ReporterAccessible int    `xml:"reporter_accessible"`
	ClassificationID   int    `xml:"classification_id"`
	Classification     string `xml:"classification"`
	Product            string `xml:"product"`
	Component          string `xml:"component"`
	Version            string `xml:"version"`
	RepPlatform        string `xml:"rep_platform"`
	OpSys              string `xml:"op_sys"`
	BugStatus          string `xml:"bug_status"`
	Resolution         string `xml:"resolution"`
	BugFileLoc         string `xml:"bug_file_loc"`
	StatusWhiteboard  string `xml:"status_whiteboard"`
	Keywords           string `xml:"keywords"`
	Priority           string `xml:"priority"`
	BugSeverity       string `xml:"bug_severity"`
	TargetMilestone   string `xml:"target_milestone"`
	EverConfirmed      string `xml:"everconfirmed"`
	Reporter           string `xml:"reporter"`
	AssignedTo        string `xml:"assigned_to"`
	CC                 string `xml:"cc"`
	CfOs               string `xml:"cf_os"`
	CfRegressionType   string `xml:"cf_regression_type"`
	LongDescription    struct {
		CommentID int    `xml:"comment_id"`
		Who       string `xml:"who"`
		When      string `xml:"bug_when"`
		TheText   string `xml:"thetext"`
	} `xml:"long_desc"`
}

// SearchFields ...
type SearchFields struct {
	Component string `json:"component"`
	Product   string `json:"product"`
	ItemID    string `json:"item_id"`
}

// BugRaw data model represents es schema
type BugRaw struct {
	BackendVersion           string        `json:"backend_version"`
	BackendName              string        `json:"backend_name"`
	UUID                     string        `json:"uuid"`
	Origin                   string        `json:"origin"`
	Tag                      string        `json:"tag"`
	Product                  string        `json:"product"`
	Data                     *BugDetailXML  `json:"data"`
	UpdatedOn                int64         `json:"updated_on"`
	MetadataUpdatedOn        time.Time     `json:"metadata__updated_on"`
	MetadataTimestamp        time.Time     `json:"metadata__timestamp"`
	Timestamp                int64         `json:"timestamp"`
	Category                 string        `json:"category"`
	ClassifiedFieldsFiltered *string       `json:"classified_fields_filtered"`
	SearchFields             *SearchFields `json:"search_fields"`
}
