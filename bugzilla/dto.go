package bugzilla

import (
	"time"
)

type AssigneeResponse struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// BugResponse data model represents Bugzilla get bugsList results
type BugResponse struct {
	ID               int               `json:"id"`
	Product          string            `json:"product"`
	Component        string            `json:"component"`
	AssignedTo       *AssigneeResponse `json:"assigned_to"`
	ShortDescription string            `json:"short_description"`
	CreationTS       time.Time         `json:"creation_ts"`
	Priority         string            `json:"priority"`
	BugStatus        string            `json:"bug_status"`
	//Activity           []*BugActivityResponse `json:"activity"`
	Severity string `json:"bug_severity"`
	OpSys    string `json:"op_sys"`
}

// BugActivityResponse data model represents Bugzilla bugsActivity results
type BugActivityResponse struct {
	Added  string `json:"added"`
	What   string `json:"what"`
	Remove string `json:"remove"`
	Who    string `json:"who"`
	When   string `json:"when"`
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
	status_whiteboard  string `xml:"status_whiteboard"`
	keywords           string `xml:"keywords"`
	priority           string `xml:"priority"`
	bug_severity       string `xml:"bug_severity"`
	target_milestone   string `xml:"target_milestone"`
	everconfirmed      string `xml:"everconfirmed"`
	reporter           string `xml:"reporter"`
	assigned_to        string `xml:"assigned_to"`
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
	BackendVersion    string       `json:"backend_version"`
	BackendName       string       `json:"backend_name"`
	UUID              string       `json:"uuid"`
	Origin            string       `json:"origin"`
	Tag               string       `json:"tag"`
	Product           string       `json:"product"`
	Data              *BugResponse `json:"data"`
	MetadataUpdatedOn time.Time    `json:"metadata__updated_on"`
	MetadataTimestamp time.Time    `json:"metadata__timestamp"`
	Timestamp         float64      `json:"timestamp"`
	Category          string       `json:"category"`
	//SearchFields             *SearchFields `json:"search_fields"`
}
