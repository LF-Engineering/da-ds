package circleci

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/LF-Engineering/dev-analytics-libraries/uuid"
	jsoniter "github.com/json-iterator/go"
)

// HTTPClientProvider used in connecting to remote http server
type HTTPClientProvider interface {
	Request(url string, method string, header map[string]string, body []byte, params map[string]string) (statusCode int, resBody []byte, err error)
}

// Fetcher contains fetch functionalities
type Fetcher struct {
	dSName                string
	HTTPClientProvider    HTTPClientProvider
	ElasticSearchProvider ESClientProvider
	BackendVersion        string
	Endpoint              string
	BackendName           string
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
func NewFetcher(params Params, httpClientProvider HTTPClientProvider, esClientProvider ESClientProvider) *Fetcher {
	return &Fetcher{
		HTTPClientProvider:    httpClientProvider,
		ElasticSearchProvider: esClientProvider,
		BackendVersion:        params.BackendVersion,
		Endpoint:              params.Endpoint,
		dSName:                "CircleCI",
	}
}

func (f *Fetcher) FetchAll(origin string, token string, lastFetch *time.Time, now time.Time, fromStr string) ([]CircleCIData, *time.Time, error) {
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
			res, err := makeHttpRequest(url, nil, "GET", result.PageToken, token)
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

	data := make([]CircleCIData, 0)

	for i := len(pipelines) - 1; i >= 0; i-- {
		circleRaw, err := f.FetchWorkflows(pipelines[i], token, lastFetch, now)

		if err != nil {
			return nil, nil, err
		}
		data = append(data, circleRaw...)
	}

	return data, nil, nil
}

func (f *Fetcher) FetchWorkflows(pipeline PipelineItem, token string, lastFetch *time.Time, now time.Time) ([]CircleCIData, error) {

	// Get Workflows
	workflows, err := f.fetchworkflow(pipeline.ID, token)
	if err != nil {
		return nil, err
	}

	data := make([]CircleCIData, 0)
	for _, workflow := range workflows {
		if lastFetch == nil || lastFetch.Before(workflow.CreatedAt) {
			result, err := f.FetchJobs(workflow, pipeline, token, now)
			if err != nil {
				return nil, err
			}
			data = append(data, result...)
		} else {
			continue
		}

	}

	return data, nil
}

func (f *Fetcher) FetchJobs(workflow WorkflowItem, pipeline PipelineItem, token string, now time.Time) ([]CircleCIData, error) {
	// Get Jobs
	jobs, err := f.fetchworkflowjobs(workflow.ID, token)
	if err != nil {
		return nil, err
	}

	data := make([]CircleCIData, 0)
	for _, job := range jobs {
		result, err := f.FetchItem(workflow, pipeline, job, token, now)
		if err != nil {
			return nil, err
		}
		data = append(data, *result)
	}

	return data, nil
}

func (f *Fetcher) FetchItem(workflow WorkflowItem, pipeline PipelineItem, job JobItem, token string, now time.Time) (*CircleCIData, error) {
	var circleCI CircleCIData
	var err error
	var jobDetails *JobDetails
	// Get Job Details
	if job.Type != "approval" && job.Status != "blocked" {
		jobDetails, err = f.fetchjobdetails(job.ProjectSlug, job.JobNumber, token)
		if err != nil {
			return nil, err
		}
	}

	// generate UUID
	uid, err := uuid.Generate(job.ID, pipeline.ID, workflow.ID)
	if err != nil {
		return nil, err
	}

	circleCI.UUID = uid
	circleCI.ProjectSlug = pipeline.ProjectSlug
	circleCI.PipelineID = pipeline.ID
	circleCI.PipelineNumber = pipeline.Number
	circleCI.PipelineCreatedAt = pipeline.CreatedAt
	circleCI.PipelineUpdatedAt = pipeline.UpdatedAt
	circleCI.PipelineState = pipeline.State
	circleCI.PipelineTriggerType = pipeline.PipelineTrigger.Type
	circleCI.PipelineTriggerDate = pipeline.PipelineTrigger.ReceivedAt

	circleCI.AuthorUserName = pipeline.PipelineTrigger.Actor.Login

	workflowstartedby, err := f.fetchuser(workflow.StartedBy, token)
	if err != nil {
		circleCI.WorkflowCreatorUserName = pipeline.PipelineTrigger.Actor.Login
		circleCI.WorkflowCreatorBot = true
	} else {
		circleCI.WorkflowCreatorUserName = workflowstartedby
	}

	if job.Type == "approval" {
		approvalname, err := f.fetchuser(job.ApprovedBy, token)
		if err != nil {
			return nil, err
		}
		circleCI.WorkflowApprovalUserName = approvalname
		circleCI.WorkflowApprovalRequestID = job.ApprovedRequestID
		circleCI.ISApproval = true
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

	circleCI.OriginalRepositoryURL = pipeline.SourceVCS.OriginURL
	circleCI.TargetRepositoryURL = pipeline.SourceVCS.TargetURL
	circleCI.Revision = pipeline.SourceVCS.Revision
	circleCI.Provider = pipeline.SourceVCS.Provider

	if pipeline.SourceVCS.Tag != "" {
		circleCI.Tag = pipeline.SourceVCS.Tag
		circleCI.ISRelease = true
	}

	if pipeline.SourceVCS.Branch != "" {
		circleCI.IsCommit = true
		circleCI.CommitBranch = pipeline.SourceVCS.Branch
		circleCI.CommitBody = pipeline.SourceVCS.VCSCommit.Body
		circleCI.CommitSubject = pipeline.SourceVCS.VCSCommit.Subject
	}

	circleCI.WorkflowID = workflow.ID
	circleCI.WorkflowName = workflow.Name
	circleCI.WorkflowStatus = workflow.Status
	circleCI.WorkflowCreatedAt = workflow.CreatedAt
	circleCI.WorkflowStoppedAt = workflow.StoppedAt

	if job.Type != "approval" && job.Status != "blocked" {
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

func makeHttpRequest(url string, payload []byte, method string, pageToken string, token string) ([]byte, error) {
	var response *http.Response
	var err error
	client := &http.Client{}

	url = fmt.Sprintf("%s?page-token=%s", url, pageToken)
	request, err := http.NewRequest(method, url, bytes.NewBuffer(payload))
	request.Header.Set("Content-type", "application/json")
	request.Header.Set("Circle-Token", token)

	response, err = client.Do(request)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}
