package dockerhub

import "time"

// RepositoryResponse data model represents dockerhub get repository results
type RepositoryResponse struct {
	User            string      `json:"user"`
	Name            string      `json:"name"`
	Namespace       string      `json:"namespace"`
	RepositoryType  string      `json:"repository_type"`
	Status          int         `json:"status"`
	Description     string      `json:"description"`
	IsPrivate       bool        `json:"is_private"`
	IsAutomated     bool        `json:"is_automated"`
	CanEdit         bool        `json:"can_edit"`
	StarCount       int         `json:"star_count"`
	PullCount       int         `json:"pull_count"`
	LastUpdated     time.Time   `json:"last_updated"`
	IsMigrated      bool        `json:"is_migrated"`
	HasStarred      bool        `json:"has_starred"`
	FullDescription string      `json:"full_description"`
	Affiliation     string      `json:"affiliation"`
	Permissions     Permissions `json:"permissions"`
	FetchedOn       float64       `json:"fetched_on"`
}

type Permissions struct {
	Read  bool `json:"read"`
	Write bool `json:"write"`
	Admin bool `json:"admin"`
}

// RepositorySearchFields ...
type RepositorySearchFields struct {
	Name      string `json:"name"`
	ItemID    string `json:"item_id"`
	Namespace string `json:"namespace"`
}

// RepositoryRaw data model represents es schema
type RepositoryRaw struct {
	BackendVersion           string                  `json:"backend_version"`
	Data                     *RepositoryResponse     `json:"data"`
	Tag                      string                  `json:"tag"`
	UUID                     string                  `json:"uuid"`
	SearchFields             *RepositorySearchFields `json:"search_fields"`
	Origin                   string                  `json:"origin"`
	UpdatedOn                float64                   `json:"updated_on"`
	MetadataUpdatedOn        time.Time               `json:"metadata__updated_on"`
	BackendName              string                  `json:"backend_name"`
	MetadataTimestamp        time.Time               `json:"metadata__timestamp"`
	Timestamp                float64                   `json:"timestamp"`
	Category                 string                  `json:"category"`
	ClassifiedFieldsFiltered *string                 `json:"classified_fields_filtered"`
}

type RepositoryEnrich struct {
	ID             string `json:"id"`
	Project        string `json:"project"`
	Affiliation    string `json:"affiliation"`
	Description    string `json:"description"`
	IsPrivate      bool   `json:"is_private"`
	IsAutomated    bool   `json:"is_automated"`
	PullCount      int    `json:"pull_count"`
	RepositoryType string `json:"repository_type"`
	User           string `json:"user"`
	Status         int    `json:"status"`
	StarCount      int    `json:"star_count"`

	IsEvent                 int    `json:"is_event"`
	IsDockerImage           int    `json:"is_docker_image"`
	DescriptionAnalyzed     string `json:"description_analyzed"`
	FullDescriptionAnalyzed string `json:"full_description_analyzed"`

	CreationDate         time.Time `json:"creation_date"`
	IsDockerhubDockerhub int       `json:"is_dockerhub_dockerhub"`
	RepositoryLabels     *[]string `json:"repository_labels"`
	MetadataFilterRaw    *string   `json:"metadata__filter_raw"`

	LastUpdated        time.Time `json:"last_updated"`
	Offset             *string   `json:"offset"`
	MetadataEnrichedOn time.Time `json:"metadata__enriched_on"`

	BackendVersion    string    `json:"backend_version"`
	Tag               string    `json:"tag"`
	UUID              string    `json:"uuid"`
	Origin            string    `json:"origin"`
	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	BackendName       string    `json:"metadata__backend_name"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	BuildOnCloud      *string   `json:"build_on_cloud"`
	ProjectTS         int64     `json:"project_ts"`
}

// LoginResponse from login dockerhub web API
type LoginResponse struct {
	Token string `json:"token"`
}
