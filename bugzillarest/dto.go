package bugzillarest

import "time"

type Attachment struct {
	Data           string
	Size           int
	CreationTime   time.Time
	LastChangeTime time.Time
	ID             int
	BugID          int
	FileName       string
	Summary        string
	ContentType    string
	IsPrivate      bool
	IsObsolete     bool
	IsPatch        bool
	Creator        string
	Flags          []string
}

type BugzillaRestRaw struct {
	Data                     BugData `json:"data"`
	UUID                     string `json:"uuid"`
	MetadataUpdatedOn        time.Time `json:"metadata__updated_on"`
	ClassifiedFieldsFiltered *string `json:"classified_fields_filtered"`
	UpdatedOn                float64 `json:"updated_on"`
	BackendName              string `json:"backend_name"`
	Category                 string `json:"category"`
	Origin                   string `json:"origin"`
	BackendVersion           string `json:"backend_version"`
	Tag                      string `json:"tag"`
	Timestamp                float64 `json:"timestamp"`
	MetadataTimestamp        time.Time `json:"metadata__timestamp"`
}

type FetchedBugs struct {
	Bugs []BugData
}

type BugData struct {
	History             *[]History `json:"history"`
	Resolution          string `json:"resolution"`
	Priority            string `json:"priority"`
	Keywords            []string `json:"keywords"`
	DependsOn           []string `json:"depends_on"`
	Alias               []string `json:"alias"`
	IsCcAccessible      bool `json:"is_cc_accessible"`
	Duplicates          []int `json:"duplicates"`
	SeeAlso             []string `json:"see_also"`
	LastChangeTime      time.Time `json:"last_change_time"`
	CreatorDetail       *PersonDetail `json:"creator_detail"`
	Blocks              []int `json:"blocks"`
	TargetMilestone     string `json:"target_milestone"`
	Deadline            *string `json:"deadline"`
	IsOpen              bool `json:"is_open"`
	RemainingTime       int `json:"remaining_time"`
	Flags               []string `json:"flags"`
	Groups              []string `json:"groups"`
	Component           string `json:"component"`
	Platform            string `json:"platform"`
	Comments            Comments `json:"comments"`
	EstimatedTime       int `json:"estimated_time"`
	OpSys               string `json:"op_sys"`
	Severity            string `json:"severity"`
	Url                 string `json:"url"`
	Cc                  []string `json:"cc"`
	IsConfirmed         bool `json:"is_confirmed"`
	IsCreatorAccessible bool `json:"is_creator_accessible"`
	ActualTime          int `json:"actual_time"`
	AssignedTo          string `json:"assigned_to"`
	DupeOf              *string`json:"dupe_of"`
	Attachments         []Attachment `json:"attachments"`
	Tags                []string `json:"tags"`
	CreationTime        time.Time`json:"creation_time"`
	Whiteboard          string `json:"whiteboard"`
	CcDetail            []PersonDetail `json:"cc_detail"`
	Status              string
	Summary             string
	Classification      string
	QaContact           string
	Product             string `json:"product"`
	ID                  int `json:"id"`
	Creator             string `json:"creator"`
	Version             string `json:"version"`
	AssignedToDetail    *PersonDetail `json:"assigned_to_detail"`
}

type PersonDetail struct {
	Name     string `json:"name"`
	RealName string `json:"real_name"`
	ID       int    `json:"id"`
}

// BugRestEnrich ...
type BugRestEnrich struct {
	UUID           string    `json:"uuid"`
	Project        string    `json:"project"`
	Changes        int       `json:"changes"`
	Product        string    `json:"product"`
	Component      string    `json:"component"`
	Status         string    `json:"status"`
	TimeOpenDays   float64   `json:"timeopen_days"`
	ChangedDate    time.Time `json:"changed_date"`
	Tag            string    `json:"tag"`
	URL            string    `json:"url"`
	CreationDate   time.Time `json:"creation_date"`
	DeltaTs        time.Time `json:"delta_ts"`
	Whiteboard     *string   `json:"whiteboard"`
	AssignedTo      string    `json:"assigned_to"`

	CreatorDetailID           string   `json:"creator_detail_id"`
	CreatorDetailUUID         string   `json:"creator_detail_uuid"`
	CreatorDetailName         string   `json:"creator_detail_name"`
	CreatorDetailUserName     string   `json:"creator_detail_user_name"`
	CreatorDetailDomain       string   `json:"creator_detail_domain"`
	CreatorDetailGender       string   `json:"creator_detail_gender"`
	CreatorDetailGenderACC    int      `json:"creator_detail_gender_acc"`
	CreatorDetailOrgName      string   `json:"creator_detail_org_name"`
	CreatorDetailMultiOrgName []string `json:"creator_detail_multi_org_names"`
	CreatorDetailBot          bool     `json:"creator_detail_bot"`

	AuthorID            string   `json:"author_id"`
	AuthorUUID          string   `json:"author_uuid"`
	AuthorName          string   `json:"author_name"`
	AuthorUserName      string   `json:"author_user_name"`
	AuthorDomain        string   `json:"author_domain"`
	AuthorGender        string   `json:"author_gender"`
	AuthorGenderAcc     int      `json:"author_gender_acc"`
	AuthorOrgName       string   `json:"author_org_name"`
	AuthorMultiOrgNames []string `json:"author_multi_org_names"`
	AuthorBot           bool     `json:"author_bot"`

	AssignedToUUID         string   `json:"assigned_to_uuid"`
	AssignedToOrgName      string   `json:"assigned_to_org_name"`

	AssignedToDetailID           string   `json:"assigned_to_detail_id"`
	AssignedToDetailUUID         string   `json:"assigned_to_detail_uuid"`
	AssignedToDetailName         string   `json:"assigned_to_detail_name"`
	AssignedToDetailUserName     string   `json:"assigned_to_detail_user_name"`
	AssignedToDetailDomain       string   `json:"assigned_to_detail_domain"`
	AssignedToDetailGender       string   `json:"assigned_to_detail_gender"`
	AssignedToDetailGenderAcc    int      `json:"assigned_to_detail_gender_acc"`
	AssignedToDetailOrgName      string   `json:"assigned_to_detail_org_name"`
	AssignedToDetailMultiOrgName []string `json:"assigned_to_detail_multi_org_names"`
	AssignedToDetailBot          bool     `json:"assigned_to_detail_bot"`

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

	ISBugzillarestBugrest int       `json:"is_bugzillarest_bugrest"`
	CreationTs            string `json:"creation_ts"`
	NumberOfComments      int       `json:"number_of_comments"`
	Origin                string    `json:"origin"`
	Offset                *string   `json:"offset"`
	ProjectTs             float64   `json:"project_ts"`
	Creator               string    `json:"creator"`
	ISOpen                bool      `json:"is_open"`
	ID                    int       `json:"id"`
}


type Comment struct {
	ID           int
	Creator      string
	Time         time.Time
	Count        int
	IsPrivate    bool
	CreationTime time.Time
	AttachmentID *int
	Tags         []string
}

type Comments []Comment

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
	Source BugzillaRestRaw `json:"_source"`
}