package finosmeetings

import "time"

// // Permissions response
// type Permissions struct {
// 	Read  bool `json:"read"`
// 	Write bool `json:"write"`
// 	Admin bool `json:"admin"`
// }

// // RepositorySearchFields ...
// type RepositorySearchFields struct {
// 	Name      string `json:"name"`
// 	ItemID    string `json:"item_id"`
// 	Namespace string `json:"namespace"`
// }

// type MeetingsResponse struct {
// 	CMProgram            string      `json:"user"`
// 	CMTitle            string      `json:"name"`
// 	CMType       string      `json:"namespace"`
// 	RepositoryType  string      `json:"repository_type"`
// 	Status          int         `json:"status"`
// 	Description     string      `json:"description"`
// 	IsPrivate       bool        `json:"is_private"`
// 	IsAutomated     bool        `json:"is_automated"`
// 	CanEdit         bool        `json:"can_edit"`
// 	StarCount       int         `json:"star_count"`
// 	PullCount       int         `json:"pull_count"`
// 	LastUpdated     time.Time   `json:"last_updated"`
// 	IsMigrated      bool        `json:"is_migrated"`
// 	HasStarred      bool        `json:"has_starred"`
// 	FullDescription string      `json:"full_description"`
// 	Affiliation     string      `json:"affiliation"`
// 	Permissions     Permissions `json:"permissions"`
// 	FetchedOn       float64     `json:"fetched_on"`
// }

// FinosmeetingsRaw represents finosmeetings repository raw model
type FinosmeetingsRaw struct {
	BackendVersion           string           `json:"backend_version"`
	Data                     *FinosMeetingCSV `json:"data"`
	Tag                      string           `json:"tag"`
	UUID                     string           `json:"uuid"`
	Origin                   string           `json:"origin"`
	UpdatedOn                float64          `json:"updated_on"`
	BackendName              string           `json:"backend_name"`
	Timestamp                float64          `json:"timestamp"`
	Category                 string           `json:"category"`
	ClassifiedFieldsFiltered *string          `json:"classified_fields_filtered"`
	MetadataUpdatedOn        time.Time        `json:"metadata__updated_on"`
	MetadataTimestamp        time.Time        `json:"metadata__timestamp"`
}

// FinosmeetingsEnrich represents finosmeetings enriched model
type FinosmeetingsEnrich struct {
	UUID                string    `json:"uuid"`
	Project             string    `json:"project"`
	CMProgram           string    `json:"cm_program"`
	CMTitle             string    `json:"cm_title"`
	CMType              string    `json:"cm_type"`
	CSVOrg              string    `json:"csv_org"`
	Date                string    `json:"date"`
	DateIsoFormat       time.Time `json:"date_iso_format"`
	IsFinosMeetingEntry int       `json:"is_finos_meeting_entry"`
	Tag                 string    `json:"tag"`
	GithubID            string    `json:"githubid"`
	Name                string    `json:"name"`
	BackendVersion      string    `json:"backend_version"`
	CreationDate        time.Time `json:"creation_date"`

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

	Email              string   `json:"email"`
	EmailBot           bool     `json:"email_bot"`
	EmailDomain        string   `json:"email_domain"`
	EmailGender        string   `json:"email_gender"`
	EmailGenderAcc     int      `json:"email_gender_acc"`
	EmailID            string   `json:"email_id"`
	EmailMultiOrgNames []string `json:"email_multi_org_names"`
	EmailName          string   `json:"email_name"`
	EmailOrgName       string   `json:"email_org_name"`
	EmailUsername      string   `json:"email_user_name"`
	EmailUUID          string   `json:"email_uuid"`

	MetadataUpdatedOn      time.Time `json:"metadata__updated_on"`
	MetadataTimestamp      time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn     time.Time `json:"metadata__enriched_on"`
	MetadataFilterRaw      *string   `json:"metadata__filter_raw"`
	MetadataBackendName    string    `json:"metadata__backend_name"`
	MetadataBackendVersion string    `json:"metadata__backend_version"`

	Origin string  `json:"origin"`
	Offset *string `json:"offset"`
}

// FinosMeetingCSV - struct for data from csv file
type FinosMeetingCSV struct {
	CMProgram     string
	CMTitle       string
	CMType        string
	Date          string
	DateIsoFormat time.Time
	Email         string
	GithubID      string
	Name          string
	Org           string
	Timestamp     float64
	FetchedOn     float64
}

// // RepositoryEnrich represents dockerhub repository enriched model
// type RepositoryEnrich struct {
// 	ID             string `json:"id"`
// 	Project        string `json:"project"`
// 	Affiliation    string `json:"affiliation"`
// 	Description    string `json:"description"`
// 	IsPrivate      bool   `json:"is_private"`
// 	IsAutomated    bool   `json:"is_automated"`
// 	PullCount      int    `json:"pull_count"`
// 	RepositoryType string `json:"repository_type"`
// 	User           string `json:"user"`
// 	Status         int    `json:"status"`
// 	StarCount      int    `json:"star_count"`

// 	IsEvent                 int    `json:"is_event"`
// 	IsDockerImage           int    `json:"is_docker_image"`
// 	DescriptionAnalyzed     string `json:"description_analyzed"`
// 	FullDescriptionAnalyzed string `json:"full_description_analyzed"`

// 	CreationDate         time.Time `json:"creation_date"`
// 	IsDockerhubDockerhub int       `json:"is_dockerhub_dockerhub"`
// 	RepositoryLabels     *[]string `json:"repository_labels"`
// 	MetadataFilterRaw    *string   `json:"metadata__filter_raw"`

// 	LastUpdated        time.Time `json:"last_updated"`
// 	Offset             *string   `json:"offset"`
// 	MetadataEnrichedOn time.Time `json:"metadata__enriched_on"`

// 	BackendVersion      string    `json:"backend_version"`
// 	Tag                 string    `json:"tag"`
// 	UUID                string    `json:"uuid"`
// 	Origin              string    `json:"origin"`
// 	MetadataUpdatedOn   time.Time `json:"metadata__updated_on"`
// 	MetadataBackendName string    `json:"metadata__backend_name"`
// 	MetadataTimestamp   time.Time `json:"metadata__timestamp"`
// 	BuildOnCloud        *string   `json:"build_on_cloud"`
// 	ProjectTS           int64     `json:"project_ts"`
// }
