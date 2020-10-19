package dockerhub

// todo: add repository response
type RepositoryResponse struct {
	User            string `json:"user"`
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	RepositoryType  string `json:"repository_type"`
	Status          int    `json:"status"`
	Description     string `json:"description"`
	IsPrivate       bool   `json:"is_private"`
	IsAutomated     bool   `json:"is_automated"`
	CanEdit         bool   `json:"can_edit"`
	StarCount       int    `json:"star_count"`
	PullCount       int    `json:"pull_count"`
	LastUpdated     string `json:"last_updated"`
	IsMigrated      bool   `json:"is_migrated"`
	HasStarred      bool   `json:"has_starred"`
	FullDescription string `json:"full_description"`
	Affiliation     string `json:"affiliation"`
	Permissions     struct {
		Read  bool `json:"read"`
		Write bool `json:"write"`
		Admin bool `json:"admin"`
	} `json:"permissions"`
}

type RepositoryRaw struct {
	BackendVersion string             `json:"backend_version"`
	Data           RepositoryResponse `json:"data"`
	Tag            string             `json:"tag"`
	UUID           string             `json:"uuid"`
	SearchFields   struct {
		Name      string `json:"name"`
		ItemID    string `json:"item_id"`
		Namespace string `json:"namespace"`
	} `json:"search_fields"`
	Origin                   string  `json:"origin"`
	UpdatedOn                string  `json:"updated_on"`
	MetadataUpdatedOn        string  `json:"metadata__updated_on"`
	BackendName              string  `json:"backend_name"`
	MetadataTimestamp        string  `json:"metadata__timestamp"`
	Timestamp                float32 `json:"timestamp"`
	Category                 string  `json:"category"`
	ClassifiedFieldsFiltered string  `json:"classified_fields_filtered"`
}

type LoginResponse struct {
	Token string `json:"token"`
}
