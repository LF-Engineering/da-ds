package circleci

import "time"

// Pipeline circle ci raw pipeline data
type Pipeline struct {
	PageToken string         `json:"next_page_token"`
	Items     []PipelineItem `json:"items"`
}

type Workflow struct {
	PageToken string         `json:"next_page_token"`
	Items     []WorkflowItem `json:"items"`
}

type WorkflowJobs struct {
	PageToken string    `json:"next_page_token"`
	Items     []JobItem `json:"items"`
}

type PipelineItem struct {
	ID              string    `json:"id"`
	ProjectSlug     string    `json:"project_slug"`
	Number          int       `json:"number"`
	State           string    `json:"state"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	PipelineTrigger Trigger   `json:"trigger"`
	SourceVCS       VCS       `json:"vcs"`
}

type Trigger struct {
	ReceivedAt time.Time `json:"received_at"`
	Type       string    `json:"type"`
	Actor      User      `json:"actor"`
}

type User struct {
	Login     string `json:"login"`
	AvatarURL string `json:"avatar_url"`
}

type VCS struct {
	OriginURL string `json:"origin_repository_url"`
	TargetURL string `json:"target_repository_url"`
	Revision  string `json:"revision"`
	Provider  string `json:"provider_name"`
	VCSCommit Commit `json:"commit"`
	Tag       string `json:"tag"`
	Branch    string `json:"branch"`
}

type Commit struct {
	Body    string `json:"body"`
	Subject string `json:"subjec"`
}

type WorkflowItem struct {
	ID             string    `json:"id"`
	PipelineID     string    `json:"pipeline_id"`
	ProjectSlug    string    `json:"project_slug"`
	PipelineNumber int       `json:"pipeline_number"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	StartedBy      string    `json:"started_by"`
	CreatedAt      time.Time `json:"created_at"`
	StoppedAt      time.Time `json:"stopped_at"`
}

type JobItem struct {
	ID                string    `json:"id"`
	ProjectSlug       string    `json:"project_slug"`
	PipelineNumber    int       `json:"pipeline_number"`
	Dependencies      []string  `json:"dependencies"`
	Name              string    `json:"name"`
	Status            string    `json:"status"`
	StartedBy         string    `json:"started_by"`
	StartedAt         time.Time `json:"started_at"`
	StoppedAt         time.Time `json:"stopped_at"`
	ApprovedBy        string    `json:"approved_by"`
	Type              string    `json:"type"`
	ApprovedRequestID string    `json:"approval_request_id"`
	JobNumber         int       `json:"job_number"`
}

type UserDetails struct {
	Name  string `json:"name"`
	Login string `json:"login"`
	ID    string `json:"id"`
}

type JobDetails struct {
	WebURL       string         `json:"web_url"`
	JProject     Project        `json:"project"`
	ParallelRuns []ParallelRun  `json:"parallel_runs"`
	StartedAt    time.Time      `json:"started_at"`
	LWorkflow    LatestWorkflow `json:"latest_workflow"`
	Name         string         `json:"name"`
	Executor     Executor       `json:"executor"`
	Parallelism  int            `json:"parallelism"`
	Status       string         `json:"status"`
	Number       int            `json:"number"`
	Pipeline     PipelineItem   `json:"pipeline"`
	Duration     int            `json:"duration"`
	CreatedAt    time.Time      `json:"created_at"`
	QueuedAt     time.Time      `json:"queued_at"`
	StoppedAt    time.Time      `json:"stopped_at"`
	Messages     []Message      `json:"messages"`
	Contexts     []Generic      `json:"contexts"`
	Organization Generic        `json:"organization"`
}

type Project struct {
	ExternalURL string `json:"external_url"`
	Slug        string `json:"slug"`
	Name        string `json:"name"`
}

type ParallelRun struct {
	Index  int    `json:"index"`
	Status string `json:"status"`
}

type LatestWorkflow struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type Executor struct {
	Name string `json:"resource_class"`
	Type string `json:"type"`
}

type Message struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

type Generic struct {
	Name string `json:"name"`
}

type CircleCIData struct {
	UUID string `json:"uuid"`
	//Project     string `json:"project"`
	ProjectSlug string `json:"project_slug"`

	PipelineID          string    `json:"pipeline_id"`
	PipelineNumber      int       `json:"pipeline_number"`
	PipelineCreatedAt   time.Time `json:"pipeline_created_at"`
	PipelineUpdatedAt   time.Time `json:"pipeline_updated_at"`
	PipelineState       string    `json:"pipeline_state"`
	PipelineTriggerType string    `json:"pipeline_trigger_type"`
	PipelineTriggerDate time.Time `json:"pipeline_trigger_date"`

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

	WorkflowCreatorID            string   `json:"workflow_creator_id"`
	WorkflowCreatorUUID          string   `json:"workflow_creator_uuid"`
	WorkflowCreatorName          string   `json:"workflow_creator_name"`
	WorkflowCreatorUserName      string   `json:"workflow_creator_user_name"`
	WorkflowCreatorDomain        string   `json:"workflow_creator_domain"`
	WorkflowCreatorGender        string   `json:"workflow_creator_gender"`
	WorkflowCreatorGenderAcc     int      `json:"workflow_creator_gender_acc"`
	WorkflowCreatorOrgName       string   `json:"workflow_creator_org_name"`
	WorkflowCreatorMultiOrgNames []string `json:"workflow_creator_multi_org_names"`
	WorkflowCreatorBot           bool     `json:"workflow_creator_bot"`

	ISApproval                    bool     `json:"is_approval"`
	WorkflowApprovalID            string   `json:"workflow_approval_id"`
	WorkflowApprovalUUID          string   `json:"workflow_approval_uuid"`
	WorkflowApprovalName          string   `json:"workflow_approval_name"`
	WorkflowApprovalUserName      string   `json:"workflow_approval_user_name"`
	WorkflowApprovalDomain        string   `json:"workflow_approval_domain"`
	WorkflowApprovalGender        string   `json:"workflow_approval_gender"`
	WorkflowApprovalGenderAcc     int      `json:"workflow_approval_gender_acc"`
	WorkflowApprovalOrgName       string   `json:"workflow_approval_org_name"`
	WorkflowApprovalMultiOrgNames []string `json:"workflow_approval_multi_org_names"`
	WorkflowApprovalBot           bool     `json:"workflow_approval_bot"`

	OriginalRepositoryURL string `json:"original_repository_url"`
	TargetRepositoryURL   string `json:"target_repository_url"`
	Revision              string `json:"revision"`
	Provider              string `json:"provider"`

	ISRelease bool   `json:"is_release"`
	Tag       string `json:"tag"`

	IsCommit      bool   `json:"is_commit"`
	CommitBody    string `json:"commit_body"`
	CommitSubject string `json:"commit_subject"`
	CommitBranch  string `json:"commit_branch"`

	WorkflowID        string    `json:"workflow_id"`
	WorkflowName      string    `json:"workflow_name"`
	WorkflowStatus    string    `json:"workflow_status"`
	WorkflowCreatedAt time.Time `json:"workflow_created_at"`
	WorkflowStoppedAt time.Time `json:"workflow_stopped_at"`

	WorkflowApprovalRequestID string `json:"workflow_approval_request_id"`

	WorkflowJobType         string    `json:"workflow_job_type"`
	WorkflowJobID           string    `json:"workflow_job_id"`
	WorkflowJobStartedAt    time.Time `json:"workflow_job_started_at"`
	WorkflowJobStoppedAt    time.Time `json:"workflow_job_stopped_at"`
	WorkflowJobStatus       string    `json:"workflow_job_status"`
	WorkflowJobName         string    `json:"workflow_job_name"`
	WorkflowJobDependencies []string  `json:"workflow_job_dependencies"`

	JobNumber                int       `json:"job_number"`
	JobStartedAt             time.Time `json:"job_started_at"`
	JobQueuedAt              time.Time `json:"job_queued_at"`
	JobStoppedAt             time.Time `json:"job_stopped_at"`
	JobStatus                string    `json:"job_status"`
	JobParallelism           int       `json:"job_parallelism"`
	JobDuration              int       `json:"job_duration"`
	JobExecutorType          string    `json:"job_executor_type"`
	JobExecutorResourceClass string    `json:"job_executor_resource_class"`

	MetadataUpdatedOn      time.Time `json:"metadata__updated_on"`
	MetadataTimestamp      time.Time `json:"metadata__timestamp"`
	MetadataEnrichedOn     time.Time `json:"metadata__enriched_on"`
	MetadataFilterRaw      *string   `json:"metadata__filter_raw"`
	MetadataBackendName    string    `json:"metadata__backend_name"`
	MetadataBackendVersion string    `json:"metadata__backend_version"`
}
