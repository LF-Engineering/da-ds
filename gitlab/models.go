package gitlab

import "time"

// IssueData ...
type IssueData struct {
	ID             int        `json:"id"`
	IssueID        int        `json:"iid"`
	Title          string     `json:"title"`
	Description    string     `json:"description"`
	State          string     `json:"state"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ClosedAt       *time.Time `json:"closed_at"`
	ClosedBy       *Author    `json:"closed_by"`
	Labels         []string   `json:"labels"`
	Assignees      []Author   `json:"assignees"`
	Author         Author     `json:"author"`
	Type           string     `json:"type"`
	UserNotesCount int        `json:"user_notes_count"`
	Upvotes        int        `json:"upvotes"`
	Downvotes      int        `json:"downvotes"`
	WebURL         string     `json:"web_url"`
	ProjectID      int        `json:"project_id"`
}

// MergeRequestData ...
type MergeRequestData struct {
	ID             int        `json:"id"`
	MergeRequestID int        `json:"iid"`
	Title          string     `json:"title"`
	Description    string     `json:"description"`
	State          string     `json:"state"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	ClosedAt       *time.Time `json:"closed_at"`
	ClosedBy       *Author    `json:"closed_by"`
	MergedBy       *Author    `json:"merged_by"`
	MergedAt       *time.Time `json:"merged_at"`
	TargetBranch   string     `json:"target_branch"`
	SourceBranch   string     `json:"source_branch"`
	Labels         []string   `json:"labels"`
	Assignees      []Author   `json:"assignees"`
	Reviewers      []Author   `json:"reviewers"`
	Author         Author     `json:"author"`
	UserNotesCount int        `json:"user_notes_count"`
	Upvotes        int        `json:"upvotes"`
	Downvotes      int        `json:"downvotes"`
	WebURL         string     `json:"web_url"`
	Type           string     `json:"type"`
}

// Author ...
type Author struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Username  string `json:"username"`
	State     string `json:"state"`
	AvatarURL string `json:"avatar_url"`
	WebURL    string `json:"web_url"`
}

// Project ...
type Project struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// IssueRaw ...
type IssueRaw struct {
	BackendName       string    `json:"backend_name"`
	BackendVersion    string    `json:"backend_version"`
	Timestamp         float64   `json:"timestamp"`
	Origin            string    `json:"origin"`
	UUID              string    `json:"uuid"`
	Project           string    `json:"project"`
	ProjectSlug       string    `json:"project_slug"`
	MetadataUpdatedOn time.Time `json:"metadata__updated_on"`
	MetadataTimestamp time.Time `json:"metadata__timestamp"`
	Data              IssueData `json:"data"`
	Repo              string    `json:"repo"`
}

// MergeRequestRaw ...
type MergeRequestRaw struct {
	BackendName       string           `json:"backend_name"`
	BackendVersion    string           `json:"backend_version"`
	Timestamp         float64          `json:"timestamp"`
	Origin            string           `json:"origin"`
	UUID              string           `json:"uuid"`
	Project           string           `json:"project"`
	ProjectSlug       string           `json:"project_slug"`
	MetadataUpdatedOn time.Time        `json:"metadata__updated_on"`
	MetadataTimestamp time.Time        `json:"metadata__timestamp"`
	Data              MergeRequestData `json:"data"`
	Repo              string           `json:"repo"`
}

// IssueEnrich ...
type IssueEnrich struct {
	AuthorName            string     `json:"author_name"`
	AuthorAvatarURL       string     `json:"author_avatar_url"`
	AuthorID              string     `json:"author_id"`
	AuthorUUID            string     `json:"author_uuid"`
	AuthorOrgName         string     `json:"author_org_name"`
	AuthorUserName        string     `json:"author_user_name"`
	AuthorBot             bool       `json:"author_bot"`
	AuthorDomain          string     `json:"author_domain"`
	AuthorLogin           string     `json:"author_login"`
	AuthorMultiOrgNames   []string   `json:"author_multi_org_names"`
	BackendName           string     `json:"backend_name"`
	BackendVersion        string     `json:"backend_version"`
	Title                 string     `json:"title"`
	Type                  string     `json:"type"`
	CreatedAt             time.Time  `json:"created_at"`
	ClosedAt              *time.Time `json:"closed_at"`
	UpdatedAt             time.Time  `json:"updated_at"`
	URL                   string     `json:"url"`
	URLID                 string     `json:"url_id"`
	Repository            string     `json:"repository"`
	State                 string     `json:"state"`
	Tag                   string     `json:"tag"`
	Category              string     `json:"category"`
	Body                  string     `json:"body"`
	BodyAnalyzed          string     `json:"body_analyzed"`
	UUID                  string     `json:"uuid"`
	NoOfAssignees         int        `json:"n_assignees"`
	NoOfComments          int        `json:"n_comments"`
	NoOfReactions         int        `json:"n_reactions"`
	NoOfTotalComments     int        `json:"n_total_comments"`
	Origin                string     `json:"origin"`
	Project               string     `json:"project"`
	ProjectSlug           string     `json:"project_slug"`
	Labels                []string   `json:"labels"`
	ItemType              string     `json:"item_type"`
	IssueID               int        `json:"issue_id"`
	IsGitlabIssue         int        `json:"is_gitlab_issue"`
	IDInRepo              int        `json:"id_in_repo"`
	ID                    string     `json:"id"`
	GitlabRepo            string     `json:"gitlab_repo"`
	Reponame              string     `json:"repo_name"`
	RepoID                string     `json:"repo_id"`
	RepoShortname         string     `json:"repo_short_name"`
	MetadataTimestamp     time.Time  `json:"metadata__timestamp"`
	MetadataEnrichedOn    time.Time  `json:"metadata__enriched_on"`
	MetadataUpdatedOn     time.Time  `json:"metadata__updated_on"`
	UserAvatarURL         string     `json:"user_avatar_url"`
	UserDataBot           bool       `json:"user_data_bot"`
	UserDataDomain        string     `json:"user_data_domain"`
	UserDataID            string     `json:"user_data_id"`
	UserDataMultiOrgNames []string   `json:"user_data_multi_org_names"`
	UserDataName          string     `json:"user_data_name"`
	UserDataOrgName       string     `json:"user_data_org_name"`
	UserDataUsername      string     `json:"user_data_user_name"`
	UserDataUUID          string     `json:"user_data_uuid"`
	UserDomain            string     `json:"user_domain"`
	UserLocation          string     `json:"user_location"`
	UserLogin             string     `json:"user_login"`
	Username              string     `json:"user_name"`
	UserOrg               string     `json:"user_org"`
}

// MergeReqestEnrich ...
type MergeReqestEnrich struct {
	AuthorName             string     `json:"author_name"`
	AuthorAvatarURL        string     `json:"author_avatar_url"`
	AuthorID               string     `json:"author_id"`
	AuthorUUID             string     `json:"author_uuid"`
	AuthorOrgName          string     `json:"author_org_name"`
	AuthorUserName         string     `json:"author_user_name"`
	AuthorBot              bool       `json:"author_bot"`
	AuthorDomain           string     `json:"author_domain"`
	AuthorLogin            string     `json:"author_login"`
	AuthorMultiOrgNames    []string   `json:"author_multi_org_names"`
	BackendName            string     `json:"backend_name"`
	BackendVersion         string     `json:"backend_version"`
	Title                  string     `json:"title"`
	Type                   string     `json:"type"`
	CreatedAt              time.Time  `json:"created_at"`
	ClosedAt               *time.Time `json:"closed_at"`
	MergedAt               time.Time  `json:"merged_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	Merged                 bool       `json:"merged"`
	URL                    string     `json:"url"`
	URLID                  string     `json:"url_id"`
	Repository             string     `json:"repository"`
	State                  string     `json:"state"`
	Tag                    string     `json:"tag"`
	Category               string     `json:"category"`
	Body                   string     `json:"body"`
	BodyAnalyzed           string     `json:"body_analyzed"`
	UUID                   string     `json:"uuid"`
	NoOfAssignees          int        `json:"n_assignees"`
	NoOfComments           int        `json:"n_comments"`
	NoOfReactions          int        `json:"n_reactions"`
	NoOfTotalComments      int        `json:"n_total_comments"`
	NoOfRequestedReviewers int        `json:"n_requested_reviewers"`
	Origin                 string     `json:"origin"`
	Project                string     `json:"project"`
	ProjectSlug            string     `json:"project_slug"`
	Labels                 []string   `json:"labels"`
	ItemType               string     `json:"item_type"`
	MergeRequestID         int        `json:"merge_request_id"`
	IsGitlabMergeRequest   int        `json:"is_gitlab_merge_request"`
	MergeRequest           bool       `json:"merge_request"`
	IDInRepo               int        `json:"id_in_repo"`
	ID                     string     `json:"id"`
	GitlabRepo             string     `json:"gitlab_repo"`
	Reponame               string     `json:"repo_name"`
	RepoID                 string     `json:"repo_id"`
	RepoShortname          string     `json:"repo_short_name"`
	MetadataTimestamp      time.Time  `json:"metadata__timestamp"`
	MetadataEnrichedOn     time.Time  `json:"metadata__enriched_on"`
	MetadataUpdatedOn      time.Time  `json:"metadata__updated_on"`
	UserAvatarURL          string     `json:"user_avatar_url"`
	UserDataBot            bool       `json:"user_data_bot"`
	UserDataDomain         string     `json:"user_data_domain"`
	UserDataID             string     `json:"user_data_id"`
	UserDataMultiOrgNames  []string   `json:"user_data_multi_org_names"`
	UserDataName           string     `json:"user_data_name"`
	UserDataOrgName        string     `json:"user_data_org_name"`
	UserDataUsername       string     `json:"user_data_user_name"`
	UserDataUUID           string     `json:"user_data_uuid"`
	UserDomain             string     `json:"user_domain"`
	UserLocation           string     `json:"user_location"`
	UserLogin              string     `json:"user_login"`
	Username               string     `json:"user_name"`
	UserOrg                string     `json:"user_org"`
}
