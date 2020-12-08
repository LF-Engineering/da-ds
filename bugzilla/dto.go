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
	ID               int                    `json:"id"`
	Product          string                 `json:"product"`
	Component        string                 `json:"component"`
	AssignedTo       *AssigneeResponse      `json:"assigned_to"`
	ShortDescription string                 `json:"short_description"`
	CreationTS       time.Time              `json:"creation_ts"`
	Priority         string                 `json:"priority"`
	BugStatus        string                 `json:"bug_status"`
	ChangedAt        string                 `json:"changed_at"`
	Activity         []*BugActivityResponse `json:"activity"`
}

// todo: clean it if not used
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
	ID               int        `xml:"bug_id"`
	CreationTS       string     `xml:"creation_ts"`
	DeltaTS          string     `xml:"delta_ts"`
	Priority         string     `xml:"priority"`
	Severity         string     `xml:"bug_severity"`
	OpSys            string     `xml:"op_sys"`
	RepPlatform      string     `xml:"rep_platform"`
	Keywords         []string   `xml:"keywords"`
	StatusWhiteboard string     `xml:"status_whiteboard"`
	Resolution       string     `xml:"resolution"`
	Reporter         string     `xml:"reporter"`
	AssignedTo       string     `xml:"assigned_to"`
	Summary          string     `xml:"summary"`
	LongDesc         []Comments `xml:"long_desc"`
}

type Comments struct {
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
	ShortDescription  string     `json:"short_description"`
	LongDesc          []Comments `json:"long_desc"`
	BugStatus         string     `json:"bug_status"`
	MetadataUpdatedOn time.Time  `json:"metadata__updated_on"`
	MetadataTimestamp time.Time  `json:"metadata__timestamp"`
	Timestamp         float64    `json:"timestamp"`
	Category          string     `json:"category"`
	CreationTS        time.Time  `json:"creation_ts"`
	Priority          string     `json:"priority"`
	Severity          string     `json:"severity"`
	OpSys             string     `json:"op_sys"`
	ChangedAt         time.Time  `json:"changed_at"`
	ActivityCount     int        `json:"activity_count"`
	//SearchFields             *SearchFields `json:"search_fields"`
	DeltaTs          time.Time `json:"delta_ts"`
	Keywords         []string  `json:"keywords"`
	RepPlatform      string    `json:"rep_platform"`
	StatusWhiteboard string    `json:"status_whiteboard"`
	Resolution       string    `json:"resolution"`
	Reporter         string    `json:"reporter"`
	AssignedTo       string    `json:"assigned_to"`
	Summary          string    `json:"summary"`
}

// EnrichedItem ...
type EnrichedItem struct {
	UUID                    string    `json:"uuid"`
	Labels                  []string  `json:"labels"`
	Changes                 int       `json:"changes"`
	Priority                string    `json:"priority"`
	Severity                string    `json:"severity"`
	OpSys                   string    `json:"op_sys"`
	ChangedAt               string    `json:"changed_at"`
	Product                 string    `json:"product"`
	Component               string    `json:"component"`
	Platform                string    `json:"platform"`
	BugId                   int       `json:"bug_id"`
	Status                  string    `json:"status"`
	TimeOpenDays            float64   `json:"timeopen_days"`
	Category                string    `json:"category"`
	ChangedDate             time.Time `json:"changed_date"`
	Tag                     string    `json:"tag"`
	IsBugzillaBug           int       `json:"is_bugzilla_bug"`
	Url                     string    `json:"url"`
	ResolutionDays          float64   `json:"resolution_days"`
	CreationDate            time.Time `json:"creation_date"`
	DeltaTs                 time.Time `json:"delta_ts"`
	Whiteboard              string    `json:"whiteboard"`
	Resolution              string    `json:"resolution"`
	Assigned                string    `json:"assigned"`
	ReporterName            string    `json:"reporter_name"`
	AuthorName              string    `json:"author_name"`
	MainDescription         string    `json:"main_description"`
	MainDescriptionAnalyzed string    `json:"main_description_analyzed"`
	Summary                 string    `json:"summary"`
	SummaryAnalyzed         string    `json:"summary_analyzed"`
	Comments                int       `json:"comments"`
	LongDesc                int       `json:"long_desc"`

	MetadataUpdatedOn  time.Time `json:"metadata__updated_on"`
	MetadataTimestamp  time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn time.Time `json:"metadata__enriched_on"`
	MetadataFilterRaw  *string   `json:"metadata__filter_raw"`
	BackendName        string    `json:"metadata__backend_name"`
}
