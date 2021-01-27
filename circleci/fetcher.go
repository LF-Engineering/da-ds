package circleci

import (
	"fmt"
	"time"

	libAffiliations "github.com/LF-Engineering/dev-analytics-libraries/affiliation"
	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
)

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// Fetcher contains fetch functionalities
type Fetcher struct {
	dSName                     string
	HTTPClientProvider         HTTPClientProvider
	ElasticSearchProvider      ESClientProvider
	BackendVersion             string
	Endpoint                   string
	BackendName                string
	affiliationsClientProvider *libAffiliations.Affiliation
	cache                      map[string]libAffiliations.AffIdentity
	userCache                  map[string]string
}

// Params required parameters for bugzilla fetcher
type Params struct {
	Name           string
	Endpoint       string
	FromDate       time.Time
	Order          string
	Project        string
	BackendVersion string
	BackendName    string
}

// NewFetcher initiates a new circleci fetcher
func NewFetcher(params Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider, affiliationsClientProvider *libAffiliations.Affiliation, cache map[string]libAffiliations.AffIdentity, userCache map[string]string) *Fetcher {
	return &Fetcher{
		HTTPClientProvider:         httpClientProvider,
		ElasticSearchProvider:      esClientProvider,
		BackendVersion:             params.BackendVersion,
		Endpoint:                   params.Endpoint,
		dSName:                     "CircleCI",
		affiliationsClientProvider: affiliationsClientProvider,
		cache:                      cache,
		userCache:                  userCache,
	}
}

func (f *Fetcher) FetchAll(origin string, project string, token string, lastFetch *time.Time, now time.Time, fromStr string) ([]CircleCIData, *time.Time, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", APIURL, APIVersion, APIProject, origin, APIPipeline)

	var pipelines []PipelineItem
	var result Pipeline
	// fetch first  page of circlci pipeline result
	header := map[string]string{"Circle-Token": token}
	_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, nil, err
	}

	pipelines = append(pipelines, result.Items...)

	if result.PageToken != "" {
		for {
			params := make(map[string]string, 0)
			params["page-token"] = result.PageToken
			_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, params)
			if err != nil {
				return nil, nil, err
			}

			err = jsoniter.Unmarshal(res, &result)

			if err != nil {
				return nil, nil, err
			}

			if result.PageToken == "" {
				pipelines = append(pipelines, result.Items...)
				break
			}

			pipelines = append(pipelines, result.Items...)
		}
	}
	fmt.Printf("Total number of Pipelines fetched: %d\n", len(pipelines))

	data := make([]CircleCIData, 0)

	for _, pipeline := range pipelines {
		circleRaw, err := f.FetchWorkflows(pipeline, token, lastFetch, now, project)

		if err != nil {
			return nil, nil, err
		}
		data = append(data, circleRaw...)
	}

	fmt.Println("Done fetching data from origin")
	return data, nil, nil
}

func (f *Fetcher) FetchWorkflows(pipeline PipelineItem, token string, lastFetch *time.Time, now time.Time, project string) ([]CircleCIData, error) {

	// Get Workflows
	workflows, err := f.fetchworkflow(pipeline.ID, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	data := make([]CircleCIData, 0)
	for _, workflow := range workflows {
		if lastFetch == nil || lastFetch.Before(workflow.CreatedAt) {
			result, err := f.FetchJobs(workflow, pipeline, token, now, project)
			if err != nil {
				fmt.Println(err)
				return nil, err
			}
			data = append(data, result...)
		} else {
			continue
		}

	}

	return data, nil
}

func (f *Fetcher) FetchJobs(workflow WorkflowItem, pipeline PipelineItem, token string, now time.Time, project string) ([]CircleCIData, error) {
	// Get Jobs
	jobs, err := f.fetchworkflowjobs(workflow.ID, token)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	workflowDuration := f.calculateWorkflowDuration(jobs, token)

	data := make([]CircleCIData, 0)
	for _, job := range jobs {
		result, err := f.FetchItem(workflow, pipeline, job, token, now, project, workflowDuration)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		data = append(data, *result)
	}

	return data, nil
}

func (f *Fetcher) FetchItem(workflow WorkflowItem, pipeline PipelineItem, job JobItem, token string, now time.Time, project string, workflowDuration float64) (*CircleCIData, error) {
	var circleCI CircleCIData
	var err error
	var jobDetails *JobDetails
	// Get Job Details
	if job.Type != "approval" && job.Status != "blocked" && job.JobNumber > 0 {
		jobDetails, err = f.fetchjobdetails(job.ProjectSlug, job.JobNumber, token)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
	}

	// generate UUID
	uid, err := uuid.Generate(job.ID, pipeline.ID, workflow.ID)
	if err != nil {
		return nil, err
	}

	circleCI.UUID = uid
	circleCI.ProjectName = project
	circleCI.ProjectSlug = pipeline.ProjectSlug
	circleCI.PipelineID = pipeline.ID
	circleCI.PipelineNumber = pipeline.Number
	circleCI.PipelineCreatedAt = pipeline.CreatedAt
	circleCI.PipelineUpdatedAt = pipeline.UpdatedAt
	circleCI.PipelineState = pipeline.State
	circleCI.PipelineTriggerType = pipeline.PipelineTrigger.Type
	circleCI.PipelineTriggerDate = pipeline.PipelineTrigger.ReceivedAt

	if pipeline.PipelineTrigger.Type == "schedule" {
		circleCI.AuthorUserName = pipeline.PipelineTrigger.Actor.Login
		circleCI.AuthorBot = true
		circleCI.AuthorUUID = UNKNOWN
		circleCI.AuthorID = UNKNOWN
		circleCI.AuthorName = pipeline.PipelineTrigger.Actor.Login
		circleCI.AuthorDomain = UNKNOWN
		circleCI.AuthorGender = UNKNOWN
		circleCI.AuthorOrgName = UNKNOWN
		circleCI.AuthorMultiOrgNames = make([]string, 0)
		circleCI.AuthorGenderAcc = 0
		circleCI.WorkflowCreatorBot = true
		circleCI.WorkflowCreatorUUID = UNKNOWN
		circleCI.WorkflowCreatorID = UNKNOWN
		circleCI.WorkflowCreatorName = pipeline.PipelineTrigger.Actor.Login
		circleCI.WorkflowCreatorName = UNKNOWN
		circleCI.WorkflowCreatorDomain = UNKNOWN
		circleCI.WorkflowCreatorGender = UNKNOWN
		circleCI.WorkflowCreatorOrgName = UNKNOWN
		circleCI.WorkflowCreatorMultiOrgNames = make([]string, 0)
		circleCI.WorkflowCreatorGenderAcc = 0
		circleCI.WorkflowCreatorUserName = pipeline.PipelineTrigger.Actor.Login
	} else {
		var author *libAffiliations.AffIdentity
		circleCI.AuthorUserName = pipeline.PipelineTrigger.Actor.Login
		if val, ok := f.cache[circleCI.AuthorUserName]; ok {
			author = &val
			circleCI.AuthorName = author.Name
			circleCI.AuthorID = *author.ID
			circleCI.AuthorUUID = *author.UUID
			circleCI.AuthorBot = false
			circleCI.AuthorDomain = author.Domain
			circleCI.AuthorGender = *author.Gender
			circleCI.AuthorOrgName = *author.OrgName
			circleCI.AuthorMultiOrgNames = author.MultiOrgNames
			circleCI.AuthorGenderAcc = *author.GenderACC
		} else {
			author, err = f.affiliationsClientProvider.GetProfileByUsername(circleCI.AuthorUserName, project)
			if err != nil {
				circleCI.AuthorUUID = UNKNOWN
				circleCI.AuthorID = UNKNOWN
				circleCI.AuthorName = circleCI.AuthorUserName
				circleCI.AuthorDomain = UNKNOWN
				circleCI.AuthorGender = UNKNOWN
				circleCI.AuthorOrgName = UNKNOWN
				circleCI.AuthorMultiOrgNames = make([]string, 0)
				circleCI.AuthorGenderAcc = 0
			} else {
				circleCI.AuthorName = author.Name
				circleCI.AuthorID = *author.ID
				circleCI.AuthorUUID = *author.UUID
				circleCI.AuthorBot = false
				circleCI.AuthorDomain = author.Domain
				circleCI.AuthorGender = *author.Gender
				circleCI.AuthorOrgName = *author.OrgName
				circleCI.AuthorMultiOrgNames = author.MultiOrgNames
				circleCI.AuthorGenderAcc = *author.GenderACC
				f.cache[circleCI.AuthorUserName] = *author
			}
		}
		var workflowstartedby string
		if val, ok := f.userCache[workflow.StartedBy]; ok {
			workflowstartedby = val
		} else {
			workflowstartedby, err = f.fetchuser(workflow.StartedBy, token)
		}
		if err != nil {
			circleCI.WorkflowCreatorUserName = pipeline.PipelineTrigger.Actor.Login
			circleCI.WorkflowCreatorBot = true
			circleCI.WorkflowCreatorUUID = UNKNOWN
			circleCI.WorkflowCreatorID = UNKNOWN
			circleCI.WorkflowCreatorName = pipeline.PipelineTrigger.Actor.Login
			circleCI.WorkflowCreatorName = UNKNOWN
			circleCI.WorkflowCreatorDomain = UNKNOWN
			circleCI.WorkflowCreatorGender = UNKNOWN
			circleCI.WorkflowCreatorOrgName = UNKNOWN
			circleCI.WorkflowCreatorMultiOrgNames = make([]string, 0)
			circleCI.WorkflowCreatorGenderAcc = 0
		} else {
			var workflowauthor *libAffiliations.AffIdentity
			circleCI.WorkflowCreatorUserName = workflowstartedby
			if workflowval, ok := f.cache[workflowstartedby]; ok {
				workflowauthor = &workflowval
				circleCI.WorkflowCreatorName = workflowauthor.Name
				circleCI.WorkflowCreatorID = *workflowauthor.ID
				circleCI.WorkflowCreatorUUID = *workflowauthor.UUID
				circleCI.WorkflowCreatorBot = false
				circleCI.WorkflowCreatorDomain = workflowauthor.Domain
				circleCI.WorkflowCreatorGender = *workflowauthor.Gender
				circleCI.WorkflowCreatorOrgName = *workflowauthor.OrgName
				circleCI.WorkflowCreatorMultiOrgNames = workflowauthor.MultiOrgNames
				circleCI.WorkflowCreatorGenderAcc = *workflowauthor.GenderACC
			} else {
				workflowauthor, err = f.affiliationsClientProvider.GetProfileByUsername(workflowstartedby, project)
				if err != nil {
					circleCI.WorkflowCreatorUUID = UNKNOWN
					circleCI.WorkflowCreatorID = UNKNOWN
					circleCI.WorkflowCreatorName = workflowstartedby
					circleCI.WorkflowCreatorDomain = UNKNOWN
					circleCI.WorkflowCreatorGender = UNKNOWN
					circleCI.WorkflowCreatorOrgName = UNKNOWN
					circleCI.WorkflowCreatorMultiOrgNames = make([]string, 0)
					circleCI.WorkflowCreatorGenderAcc = 0
				} else {
					circleCI.WorkflowCreatorName = workflowauthor.Name
					circleCI.WorkflowCreatorID = *workflowauthor.ID
					circleCI.WorkflowCreatorUUID = *workflowauthor.UUID
					circleCI.WorkflowCreatorBot = false
					circleCI.WorkflowCreatorDomain = workflowauthor.Domain
					circleCI.WorkflowCreatorGender = *workflowauthor.Gender
					circleCI.WorkflowCreatorOrgName = *workflowauthor.OrgName
					circleCI.WorkflowCreatorMultiOrgNames = workflowauthor.MultiOrgNames
					circleCI.WorkflowCreatorGenderAcc = *workflowauthor.GenderACC
					f.cache[workflowstartedby] = *workflowauthor
				}
			}
		}
		if job.Type == "approval" {
			var approvalname string
			if val, ok := f.userCache[job.ApprovedBy]; ok {
				approvalname = val
			} else {
				approvalname, err = f.fetchuser(job.ApprovedBy, token)
				if err != nil {
					return nil, err
				}
			}
			circleCI.WorkflowApprovalUserName = approvalname
			var approvalauthor *libAffiliations.AffIdentity
			if approvalval, ok := f.cache[approvalname]; ok {
				approvalauthor = &approvalval
				circleCI.WorkflowApprovalName = approvalauthor.Name
				circleCI.WorkflowApprovalID = *approvalauthor.ID
				circleCI.WorkflowApprovalUUID = *approvalauthor.UUID
				circleCI.WorkflowApprovalBot = false
				circleCI.WorkflowApprovalDomain = approvalauthor.Domain
				circleCI.WorkflowApprovalGender = *approvalauthor.Gender
				circleCI.WorkflowApprovalOrgName = *approvalauthor.OrgName
				circleCI.WorkflowApprovalMultiOrgNames = approvalauthor.MultiOrgNames
				circleCI.WorkflowApprovalGenderAcc = *approvalauthor.GenderACC
			} else {
				approvalauthor, err = f.affiliationsClientProvider.GetProfileByUsername(workflowstartedby, project)
				if err != nil {
					circleCI.WorkflowApprovalUUID = UNKNOWN
					circleCI.WorkflowApprovalID = UNKNOWN
					circleCI.WorkflowApprovalName = approvalname
					circleCI.WorkflowApprovalDomain = UNKNOWN
					circleCI.WorkflowApprovalGender = UNKNOWN
					circleCI.WorkflowApprovalOrgName = UNKNOWN
					circleCI.WorkflowApprovalMultiOrgNames = make([]string, 0)
					circleCI.WorkflowApprovalGenderAcc = 0
				} else {
					circleCI.WorkflowApprovalName = approvalauthor.Name
					circleCI.WorkflowApprovalID = *approvalauthor.ID
					circleCI.WorkflowApprovalUUID = *approvalauthor.UUID
					circleCI.WorkflowApprovalBot = false
					circleCI.WorkflowApprovalDomain = approvalauthor.Domain
					circleCI.WorkflowApprovalGender = *approvalauthor.Gender
					circleCI.WorkflowApprovalOrgName = *approvalauthor.OrgName
					circleCI.WorkflowApprovalMultiOrgNames = approvalauthor.MultiOrgNames
					circleCI.WorkflowApprovalGenderAcc = *approvalauthor.GenderACC
					f.cache[workflowstartedby] = *approvalauthor
				}
			}
			circleCI.WorkflowApprovalRequestID = job.ApprovedRequestID
			circleCI.ISApproval = 1
			circleCI.WorkflowJobType = job.Type
			circleCI.WorkflowJobID = job.ID
			circleCI.WorkflowJobStatus = job.Status
			circleCI.WorkflowJobName = job.Name
			circleCI.WorkflowJobDependencies = job.Dependencies
		} else if job.Status == "blocked" {
			circleCI.WorkflowJobDependencies = job.Dependencies
			circleCI.WorkflowJobType = job.Type
			circleCI.WorkflowJobID = job.ID
			circleCI.WorkflowJobStatus = job.Status
			circleCI.WorkflowJobName = job.Name
		} else {
			circleCI.WorkflowJobDependencies = job.Dependencies
			circleCI.WorkflowJobType = job.Type
			circleCI.WorkflowJobID = job.ID
			circleCI.WorkflowJobStartedAt = job.StartedAt
			circleCI.WorkflowJobStoppedAt = job.StoppedAt
			circleCI.WorkflowJobStatus = job.Status
			circleCI.WorkflowJobName = job.Name
		}
	}

	circleCI.OriginalRepositoryURL = pipeline.SourceVCS.OriginURL
	circleCI.TargetRepositoryURL = pipeline.SourceVCS.TargetURL
	circleCI.Revision = pipeline.SourceVCS.Revision
	circleCI.Provider = pipeline.SourceVCS.Provider

	if pipeline.SourceVCS.Tag != "" {
		circleCI.Tag = pipeline.SourceVCS.Tag
		circleCI.ISRelease = 1
	}

	if pipeline.SourceVCS.Branch != "" {
		circleCI.IsCommit = 1
		circleCI.CommitBranch = pipeline.SourceVCS.Branch
		circleCI.CommitBody = pipeline.SourceVCS.VCSCommit.Body
		circleCI.CommitSubject = pipeline.SourceVCS.VCSCommit.Subject
	}

	circleCI.WorkflowID = workflow.ID
	circleCI.WorkflowName = workflow.Name
	circleCI.WorkflowStatus = workflow.Status
	circleCI.WorkflowCreatedAt = workflow.CreatedAt
	circleCI.WorkflowStoppedAt = workflow.StoppedAt
	circleCI.WorkflowDuration = workflowDuration
	circleCI.WorkflowDurationSecond = workflowDuration / 1000
	circleCI.WorkflowDurationMinute = circleCI.WorkflowDurationSecond / 60
	circleCI.WorkflowDurationHour = circleCI.WorkflowDurationMinute / 60

	if job.Type != "approval" && job.Status != "blocked" && job.JobNumber > 0 {
		circleCI.JobNumber = jobDetails.Number
		circleCI.JobStartedAt = jobDetails.StartedAt
		circleCI.JobQueuedAt = jobDetails.QueuedAt
		circleCI.JobStoppedAt = jobDetails.StoppedAt
		circleCI.JobStatus = jobDetails.Status
		circleCI.JobParallelism = jobDetails.Parallelism
		circleCI.JobDuration = jobDetails.Duration
		circleCI.JobExecutorType = jobDetails.Executor.Type
		circleCI.JobExecutorResourceClass = jobDetails.Executor.Name
	}
	circleCI.MetadataTimestamp = now.UTC()
	return &circleCI, nil
}

func (f *Fetcher) fetchworkflow(id string, token string) ([]WorkflowItem, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", APIURL, APIVersion, APIPipeline, id, APIWorkflow)

	header := map[string]string{"Circle-Token": token}
	_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, nil)

	if err != nil {
		return nil, err
	}

	var result Workflow
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func (f *Fetcher) fetchworkflowjobs(id string, token string) ([]JobItem, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s", APIURL, APIVersion, APIWorkflow, id, APIJob)

	header := map[string]string{"Circle-Token": token}
	_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, nil)

	if err != nil {
		return nil, err
	}

	var result WorkflowJobs
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func (f *Fetcher) fetchjobdetails(project string, jobNumber int, token string) (*JobDetails, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s/%d", APIURL, APIVersion, APIProject, project, APIJob, jobNumber)
	header := map[string]string{"Circle-Token": token}

	_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, nil)

	if err != nil {
		return nil, err
	}

	var result JobDetails
	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (f *Fetcher) fetchuser(id string, token string) (string, error) {
	url := fmt.Sprintf("%s/%s/%s/%s", APIURL, APIVersion, APIUser, id)

	header := map[string]string{"Circle-Token": token}

	_, res, err := f.HTTPClientProvider.Request(url, "GET", header, nil, nil)

	if err != nil {
		return "", err
	}

	var result UserDetails

	err = jsoniter.Unmarshal(res, &result)
	if err != nil {
		return "", err
	}
	return result.Login, nil
}

func (f *Fetcher) calculateWorkflowDuration(jobs []JobItem, token string) float64 {
	var total float64

	for _, job := range jobs {
		if job.Type != "approval" && job.Status != "blocked" {
			jobDetails, err := f.fetchjobdetails(job.ProjectSlug, job.JobNumber, token)
			if err != nil {
				continue
			} else {
				total += float64(jobDetails.Duration)
			}
		}
	}

	return total
}
