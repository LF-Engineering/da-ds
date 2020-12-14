package bugzilla

import (
	"time"
)

// AssigneeResponse represents Assignee response model
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
	ChangedAt        string            `json:"changed_at"`
}

// BugDetailResponse data model represents Bugzilla get bugDetail results
type BugDetailResponse struct {
	Bug BugDetailXML `xml:"bug"`
}

// BugDetailXML ...
type BugDetailXML struct {
	ID               int       `xml:"bug_id"`
	CreationTS       string    `xml:"creation_ts"`
	DeltaTS          string    `xml:"delta_ts"`
	Priority         string    `xml:"priority"`
	Severity         string    `xml:"bug_severity"`
	OpSys            string    `xml:"op_sys"`
	RepPlatform      string    `xml:"rep_platform"`
	Keywords         []string  `xml:"keywords"`
	StatusWhiteboard string    `xml:"status_whiteboard"`
	Resolution       string    `xml:"resolution"`
	Reporter         string    `xml:"reporter"`
	AssignedTo       string    `xml:"assigned_to"`
	Summary          string    `xml:"summary"`
	LongDesc         []Comment `xml:"long_desc"`
}

// Comment of a bug
type Comment struct {
	Commentid int    `xml:"commentid"`
	Who       string `xml:"who"`
	BugWhen   string `xml:"bug_when"`
	Thetext   string `xml:"thetext"`
}

// SearchFields ...
type SearchFields struct {
	Component string `json:"component"`
	Product   string `json:"product"`
	ItemID    string `json:"item_id"`
}

// BugRaw data model represents es schema
type BugRaw struct {
	BackendVersion string `json:"backend_version"`
	BackendName    string `json:"backend_name"`
	UUID           string `json:"uuid"`
	BugID          int    `json:"bug_id"`
	Origin         string `json:"origin"`
	Tag            string `json:"tag"`
	Product        string `json:"product"`
	Component      string `json:"component"`
	Assignee       struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	ShortDescription  string        `json:"short_description"`
	LongDesc          []Comment     `json:"long_desc"`
	BugStatus         string        `json:"bug_status"`
	MetadataUpdatedOn time.Time     `json:"metadata__updated_on"`
	MetadataTimestamp time.Time     `json:"metadata__timestamp"`
	Timestamp         float64       `json:"timestamp"`
	Category          string        `json:"category"`
	CreationTS        time.Time     `json:"creation_ts"`
	Priority          string        `json:"priority"`
	Severity          string        `json:"severity"`
	OpSys             string        `json:"op_sys"`
	ChangedAt         time.Time     `json:"changed_at"`
	ActivityCount     int           `json:"activity_count"`
	SearchFields      *SearchFields `json:"search_fields"`
	DeltaTs           time.Time     `json:"delta_ts"`
	Keywords          []string      `json:"keywords"`
	RepPlatform       string        `json:"rep_platform"`
	StatusWhiteboard  string        `json:"status_whiteboard"`
	Resolution        string        `json:"resolution"`
	Reporter          string        `json:"reporter"`
	AssignedTo        string        `json:"assigned_to"`
	Summary           string        `json:"summary"`
}

// BugEnrich ...
type BugEnrich struct {
	UUID           string    `json:"uuid"`
	Project        string    `json:"project"`
	Labels         []string  `json:"labels"`
	Changes        int       `json:"changes"`
	Priority       string    `json:"priority"`
	Severity       string    `json:"severity"`
	OpSys          string    `json:"op_sys"`
	ChangedAt      string    `json:"changed_at"`
	Product        string    `json:"product"`
	Component      string    `json:"component"`
	Platform       string    `json:"platform"`
	BugID          int       `json:"bug_id"`
	Status         string    `json:"status"`
	TimeOpenDays   float64   `json:"timeopen_days"`
	Category       string    `json:"category"`
	ChangedDate    time.Time `json:"changed_date"`
	Tag            string    `json:"tag"`
	IsBugzillaBug  int       `json:"is_bugzilla_bug"`
	URL            string    `json:"url"`
	ResolutionDays float64   `json:"resolution_days"`
	CreationDate   time.Time `json:"creation_date"`
	DeltaTs        time.Time `json:"delta_ts"`
	Whiteboard     string    `json:"whiteboard"`
	Resolution     string    `json:"resolution"`
	Assigned       string    `json:"assigned"`

	ReporterID           string   `json:"reporter_id"`
	ReporterUUID         string   `json:"reporter_uuid"`
	ReporterName         string   `json:"reporter_name"`
	ReporterUserName     string   `json:"reporter_user_name"`
	ReporterDomain       string   `json:"reporter_domain"`
	ReporterGender       string   `json:"reporter_gender"`
	ReporterGenderACC    int      `json:"reporter_gender_acc"`
	ReporterOrgName      string   `json:"reporter_org_name"`
	ReporterMultiOrgName []string `json:"reporter_multi_org_names"`
	ReporterBot          bool     `json:"reporter_bot"`

	AuthorID           string   `json:"author_id"`
	AuthorUUID         string   `json:"author_uuid"`
	AuthorName         string   `json:"author_name"`
	AuthorUserName     string   `json:"author_user_name"`
	AuthorDomain       string   `json:"author_domain"`
	AuthorGender       string   `json:"author_gender"`
	AuthorGenderAcc    int      `json:"author_gender_acc"`
	AuthorOrgName      string   `json:"author_org_name"`
	AuthorMultiOrgName []string `json:"author_multi_org_names"`
	AuthorBot          bool     `json:"author_bot"`

	AssignedToID           string   `json:"assigned_to_id"`
	AssignedToUUID         string   `json:"assigned_to_uuid"`
	AssignedToName         string   `json:"assigned_to_name"`
	AssignedToUserName     string   `json:"assigned_to_user_name"`
	AssignedToDomain       string   `json:"assigned_to_domain"`
	AssignedToGender       string   `json:"assigned_to_gender"`
	AssignedToGenderAcc    int      `json:"assigned_to_gender_acc"`
	AssignedToOrgName      string   `json:"assigned_to_org_name"`
	AssignedToMultiOrgName []string `json:"assigned_to_multi_org_names"`
	AssignedToBot          bool     `json:"assigned_to_bot"`

	MainDescription         string    `json:"main_description"`
	MainDescriptionAnalyzed string    `json:"main_description_analyzed"`
	Summary                 string    `json:"summary"`
	SummaryAnalyzed         string    `json:"summary_analyzed"`
	Comments                int       `json:"comments"`
	RepositoryLabels        *[]string `json:"repository_labels"`

	MetadataUpdatedOn      time.Time `json:"metadata__updated_on"`
	MetadataTimestamp      time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn     time.Time `json:"metadata__enriched_on"`
	MetadataFilterRaw      *string   `json:"metadata__filter_raw"`
	MetadataBackendName    string    `json:"metadata__backend_name"`
	MetadataBackendVersion string    `json:"metadata__backend_version"`
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
	ID     string `json:"_id"`
	Source BugRaw `json:"_source"`
}
