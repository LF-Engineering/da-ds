# Circle CI Instrumentation

## Onboarding
* To onboard a project, you need the project_slug which is a combination of the vcs, org name (same as Github Org name if using Github) and project name i.e vcs/org_name/project_name e.g `github/LF-Engineering/da-ds`

* The project also needs to be public else a Token by a user who is a member of the project org will be required to pull metrics from that project.

## Instrumentation 
### Configuration
* The fixture file configuration will take the form below

```
native:
  slug: StackStorm
data_sources:
- slug: circleci
  projects:
  - name: StackStorm
    endpoints:
    - name: github/StackStorm/stackstorm-ha
  config:
    - name: Circle-Token
      value: <user-generated token>
```

### Data Gathering
* The first step to instrumenting Circle CI for a given project is pulling all pipelines Incrementally. The Endpoint for pulling pipelines is `https://circleci.com/api/v2/project/<project_slug>/pipeline`
e.g `https://circleci.com/api/v2/project/gh/LF-Engineering/da-ds/pipeline`

This will return data in this form.
```
{
  "next_page_token" : "AARLwwWYCXtgaRy9oukzlxti_r-78HgZD223Cke0LMjnGlP6LEy_GDsUMrtL41rfJdhIy-dGJrJku8MSfU1r2SbuPRA4zEUNmHS9vsOH-eyx8az1VnFuNdDykYnOhPRdXKZlo3W-inRI",
  "items" : [ {
    "id" : "98b1071e-c364-4b39-91b9-820b8588e2ae",
    "errors" : [ ],
    "project_slug" : "gh/LF-Engineering/da-ds",
    "updated_at" : "2020-12-24T12:59:23.906Z",
    "number" : 154,
    "state" : "created",
    "created_at" : "2020-12-24T12:59:23.906Z",
    "trigger" : {
      "received_at" : "2020-12-24T12:59:23.721Z",
      "type" : "webhook",
      "actor" : {
        "login" : "lukaszgryglicki",
        "avatar_url" : "https://avatars2.githubusercontent.com/u/2469783?v=4"
      }
    },
    "vcs" : {
      "origin_repository_url" : "https://github.com/LF-Engineering/da-ds",
      "target_repository_url" : "https://github.com/LF-Engineering/da-ds",
      "revision" : "db4e4736effbf25d0b48f9027dd81660fc114cb4",
      "provider_name" : "GitHub",
      "commit" : {
        "body" : "Signed-off-by: Łukasz Gryglicki <lukaszgryglicki@o2.pl>",
        "subject" : "Confirmed - no more needed"
      },
      "branch" : "master"
    }
  }
  ]
}
```
The reponse provides a token to move to move to th next page.
Below are the steps taken to process pipeline data.
1. Check if we have pulled data before for the project (DA Project). If yes, get the last_pull_date.
2. For every circle ci endpoint provided in the fixure file, use the last_pull_date to determine the point in which data gathring will start.
    - Each output provides a pipeline ID which can be used to get the pipeline's workflow. To get the workflows for a given pipeline you need the pipeline's id. See pipeline response above. 

    Worflows endpoint: `https://circleci.com/api/v2/pipeline/98b1071e-c364-4b39-91b9-820b8588e2ae/workflow`. This will return the following
    ```
        {
        "next_page_token" : null,
        "items" : [ {
            "pipeline_id" : "98b1071e-c364-4b39-91b9-820b8588e2ae",
            "id" : "9552b8fe-f5b7-4f7b-92a3-8bedc0ee35ca",
            "name" : "workflow",
            "project_slug" : "gh/LF-Engineering/da-ds",
            "status" : "success",
            "started_by" : "fb72a952-6ec3-49ae-97df-b3f7e12f6093",
            "pipeline_number" : 154,
            "created_at" : "2020-12-24T12:59:23Z",
            "stopped_at" : "2020-12-24T13:00:08Z"
        } ]
        }
     ```

    - With the workflow id, you can then get the job number. 
     Endpoint: `https://circleci.com/api/v2/workflow/1a912544-8530-47d3-81c2-64c9f02dd254/job`

     Response:
     ```
       {
        "next_page_token" : null,
        "items" : [ {
            "dependencies" : [ ],
            "job_number" : 155,
            "id" : "8aa5022e-ad9c-49c4-a45d-9ce04def8b00",
            "started_at" : "2020-12-24T12:59:27Z",
            "name" : "build",
            "project_slug" : "gh/LF-Engineering/da-ds",
            "status" : "success",
            "type" : "build",
            "stopped_at" : "2020-12-24T13:00:08Z"
        } ]
        }
     ```

     There's a corner case where the job requires approval. There response would look like this.
     Endpoint: `https://circleci.com/api/v2/workflow/716a0d67-e379-420a-b84c-7cd3a2787c7e/job`

     Response:
     ```
        {
            "next_page_token" : null,
            "items" : [ {
                "dependencies" : [ ],
                "id" : "949f0edf-83c6-4a0e-a34e-c376f8fc7499",
                "started_at" : null,
                "name" : "approve_prod",
                "approved_by" : "382334b0-ea3e-40f6-8dbb-130e7cb3816c",
                "project_slug" : "gh/LF-Engineering/dev-analytics-ui",
                "status" : "success",
                "type" : "approval",
                "approval_request_id" : "949f0edf-83c6-4a0e-a34e-c376f8fc7499"
            }, {
                "dependencies" : [ "949f0edf-83c6-4a0e-a34e-c376f8fc7499" ],
                "job_number" : 1008,
                "id" : "48ed6602-87ca-40f7-9762-54f73f1b058e",
                "started_at" : "2020-12-18T16:18:02Z",
                "name" : "build_and_deploy_prod",
                "project_slug" : "gh/LF-Engineering/dev-analytics-ui",
                "status" : "success",
                "type" : "build",
                "stopped_at" : "2020-12-18T16:26:18Z"
            } ]
            }
     ```
    We can then get the approval's name using the `approved_by` id and the endpoint below
    Endpoint: `https://circleci.com/api/v2/user/382334b0-ea3e-40f6-8dbb-130e7cb3816c`

    Response: 
    ```
        {
            "name" : "Fayaz",
            "login" : "fayazg",
            "id" : "382334b0-ea3e-40f6-8dbb-130e7cb3816c"
        }
    ```

    - From the reponse above you can get the job_number. which will be used to get the job details

    Endpoint: `https://circleci.com/api/v2/project/github/LF-Engineering/da-ds/job/155`

    Response:
    ```
    {
        "web_url" : "https://circleci.com/gh/LF-Engineering/da-ds/155",
        "project" : {
            "external_url" : "https://github.com/LF-Engineering/da-ds",
            "slug" : "gh/LF-Engineering/da-ds",
            "name" : "da-ds"
        },
        "parallel_runs" : [ {
            "index" : 0,
            "status" : "success"
        } ],
        "started_at" : "2020-12-24T12:59:27.236Z",
        "latest_workflow" : {
            "name" : "workflow",
            "id" : "9552b8fe-f5b7-4f7b-92a3-8bedc0ee35ca"
        },
        "name" : "build",
        "executor" : {
            "resource_class" : "medium",
            "type" : "docker"
        },
        "parallelism" : 1,
        "status" : "success",
        "number" : 155,
        "pipeline" : {
            "id" : "98b1071e-c364-4b39-91b9-820b8588e2ae"
        },
        "duration" : 41658,
        "created_at" : "2020-12-24T12:59:24.233Z",
        "messages" : [ ],
        "contexts" : [ ],
        "organization" : {
            "name" : "LF-Engineering"
        },
        "queued_at" : "2020-12-24T12:59:24.256Z",
        "stopped_at" : "2020-12-24T13:00:08.894Z"
        }
    ```
3. Use the next_page_token to navigate to the next page where necessary.

4. Sample Elasticsearch Document can be seen in the table below

Document Key | Value | Description | Type | Required | Default 
-------------|-------|-------------|------|----------|--------
pipeline_id | 36bf400b-b767-416b-8e8d-a6bfce3de473 | Pipeline ID | string | `true` | n/a
pipeline_number | 4185 | Pipeline Number | int | `true` | n/a
project_slug | github.com/LF-Engineering/da-ds | Circle CI project slug | string | `true` | n/a
pipeline_created_at | 2020-12-11T20:24:47.003Z | Time pipeline was created | Datetime | `true` | n/a
pipeline_updated_at | 2020-12-11T20:24:47.003Z | Time pipeline was updated | Datetime | `true` | n/a
<<<<<<< HEAD
pipeline_state | created | Pipeline state | string | `true` | n/a
pipeline_trigger_type | webhook | How pipeline was triggered | string | `true` | n/a
pipeline_trigger_date | 2020-12-11T20:24:47.003Z | Time pipeline was triggered | Datetime | `true` | n/a
creator_name | Foo Bar | Name of actor who triggered the pipeline | string | `false` | Unknown
creator_username | foobar | Username of actor who triggered the pipeline | string | `true` | n/a
creator_org_name | Linux Foundation | Creator's current enrollmnent | string | `false` | Unknown
creator_bot | `false` | Pipeline creator is a bot. | boolean | `false` | `false`
creator_id | 36bf400bb767416b8e8da6bfce3de473 | Creator ID from Affiliations Database | string | `true`| n/a 
creator_multi_org_names | [LF, ATT] | List of creator's enrollments | keyword | `false` | Unknown
creator_uuid | 36bf400bb767416b8e8da6bfce3de473 | Creator UUID from Affiliations Database | string | `true` | n/a
original_repository_url | https://github.com/LF-Engineering/dev-analytics-api | Repository URL | string | `true` | n/a
target_repository_url | https://github.com/LF-Engineering/dev-analytics-api | Repository URL | string | `true` | n/a
revision | 6a1a9ae892e42f563dc89992ccfc726761275c24 | Commit hash | string | `true` | n/a
provider | GitHub | VCS | string | `true` | n/a
commit_body | Build | Message body of the commit | string | `false` | n/a
commit_subject | Update the token | Subject of the commit | string | `false` | n/a
commit_branch | main | Branch that triggered the pipeline | string | `true` | n/a
workflow_id | 716a0d67-e379-420a-b84c-7cd3a2787c7e | Workflow ID | string | `true` | n/a
workflow_name | build_and_deploy | Workflow name | string | `true` | n/a
workflow_status | success | Workflow status | string | `true` | n/a
workflow_creator_name | Foo Bar | Name of actor who triggered the pipeline | string | `false` | Unknown
workflow_creator_username | foobar | Username of actor who triggered the pipeline | string | `true` | n/a
workflow_creator_org_name | Linux Foundation | Creator's current enrollmnent | string | `false` | Unknown
workflow_creator_bot | `false` | Pipeline creator is a bot. | boolean | `false` | `false`
workflow_creator_id | 36bf400bb767416b8e8da6bfce3de473 | Creator ID from Affiliations Database | string | `true`| n/a 
workflow_creator_multi_org_names | [LF, ATT] | List of creator's enrollments | keyword | `false` | Unknown
workflow_creator_uuid | 36bf400bb767416b8e8da6bfce3de473 | Creator UUID from Affiliations Database | string | `true` | n/a
workflow_created_at | 2020-12-11T20:24:47.003Z | Time workflow was created | Datetime | `true` | n/a
workflow_stopped_at | 2020-12-11T20:24:47.003Z | Time workflow was stopped | Datetime | `true` | n/a
workflow_job_type | approval | Workflow job type | string | `true` | n/a
workflow_approval_request_id | 36bf400bb767416b8e8da6bfce3de473 | Workflow approval request id | string | `true`| n/a 
workflow_job_id | 716a0d67-e379-420a-b84c-7cd3a2787c7e | Workflow job ID | string | `true` | n/a
workflow_approval_name | Foo Bar | Name of actor who triggered the pipeline | string | `false` | Unknown
workflow_approval_username | foobar | Username of actor who triggered the pipeline | string | `true` | n/a
workflow_approval_org_name | Linux Foundation | Creator's current enrollmnent | string | `false` | Unknown
workflow_approval_bot | `false` | Pipeline creator is a bot. | boolean | `false` | `false`
workflow_approval_id | 36bf400bb767416b8e8da6bfce3de473 | Creator ID from Affiliations Database | string | `true`| n/a 
workflow_approval_multi_org_names | [LF, ATT] | List of creator's enrollments | keyword | `false` | Unknown
workflow_approval_uuid | 36bf400bb767416b8e8da6bfce3de473 | Creator UUID from Affiliations Database | string | `true` | n/a
workflow_job_started_at | 2020-12-11T20:24:47.003Z | Time workflow job was started | Datetime | `false` | n/a
workflow_job_stopped_at | 2020-12-11T20:24:47.003Z | Time workflow job was stopped | Datetime | `false` | n/a
workflow_job_status | success | Workflow job status | string | `true` | n/a
workflow_job_name | approve_prod | Workflow job name | string | `true` | n/a
job_number | 111 | Job number | int | `false` | n/a
job_created_at | 2020-12-11T20:24:47.003Z | Time job was created | Datetime | `false` | n/a
job_queued_at | 2020-12-11T20:24:47.003Z | Time job was queued | Datetime | `false` | n/a
job_stopped_at | 2020-12-11T20:24:47.003Z | Time job was stopped | Datetime | `false` | n/a
job_status | success | job status | string | `true` | n/a
job_parallelism | 1 | Job parallelism | int | `true` | n/a
job_duration | 186872 | Total time taken in seconds | int | `true` | n/a
job_executor_resource_class | large | Job executor class | string | `true` | n/a
job_executor_type | docker | Job executor class | string | `true` | n/a
=======
creator_name | Foo Bar | Name of actor who triggered the pipeline | string | `false` | Unknown
creator_username | foobar | Username of actor who triggered the pipeline | string | `true` | n/a

>>>>>>> Populating Elasticsearch sample document table
