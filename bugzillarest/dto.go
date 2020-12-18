package bugzillarest

import "time"

// BugRestEnrich ...
type BugRestEnrich struct {
	UUID           string    `json:"uuid"`
	Project        string    `json:"project"`
	Changes        int       `json:"changes"`
	//OpSys          string    `json:"op_sys"`
	Product        string    `json:"product"`
	Component      string    `json:"component"`
	//BugID          int       `json:"bug_id"`
	Status         string    `json:"status"`
	TimeOpenDays   float64   `json:"timeopen_days"`
	ChangedDate    time.Time `json:"changed_date"`
	Tag            string    `json:"tag"`
	//IsBugzillaBug  int       `json:"is_bugzilla_bug"`
	URL            string    `json:"url"`
	//ResolutionDays float64   `json:"resolution_days"`
	CreationDate   time.Time `json:"creation_date"`
	DeltaTs        time.Time `json:"delta_ts"`
	Whiteboard     *string   `json:"whiteboard"`
	//Resolution     string    `json:"resolution"`
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
