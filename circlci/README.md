# Circle CI Instrumentation
=========

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
e.g `https://circleci.com/api/v2/project/gh/LF-Engineering/dev-analytics-ui/pipeline`

This will return data in this form.
```
{
  "next_page_token": "AARLwwV5GuP_eZfqQKSifvJEpDJCxl1PbYpFnPVtSa4ZG9eZTdzmm3TmKNZU6w1ORiutZd_vKxfYvE0kj0KqC2FBS4RQJoBu6svS1BQkChXEBXtUPHTGeIsW7vs6N_kUkvN4QvrB1u1G",
  "items": [
    {
      "id": "f03c1064-7ec0-4b38-94e3-e674421a0ab7",
      "errors": [],
      "project_slug": "gh/LF-Engineering/dev-analytics-ui",
      "updated_at": "2020-12-23T11:04:49.393Z",
      "number": 1704,
      "state": "created",
      "created_at": "2020-12-23T11:04:49.393Z",
      "trigger": {
        "received_at": "2020-12-23T11:04:49.013Z",
        "type": "webhook",
        "actor": {
          "login": "NahlaEssam",
          "avatar_url": "https://avatars0.githubusercontent.com/u/7237940?v=4"
        }
      },
      "vcs": {
        "origin_repository_url": "https://github.com/LF-Engineering/dev-analytics-ui",
        "target_repository_url": "https://github.com/LF-Engineering/dev-analytics-ui",
        "revision": "002135ef7b9504f6c3073806a1cb4e41d40b8da8",
        "provider_name": "GitHub",
        "commit": {
          "body": "",
          "subject": "fix DA-3240: Mobile View - Affiliation views"
        },
        "branch": "DA-3205"
      }
    },
    {
      "id": "0eb0c7a5-a05b-444f-8db5-008e2baa7bce",
      "errors": [],
      "project_slug": "gh/LF-Engineering/dev-analytics-ui",
      "updated_at": "2020-12-22T15:18:47.536Z",
      "number": 1703,
      "state": "created",
      "created_at": "2020-12-22T15:18:47.536Z",
      "trigger": {
        "received_at": "2020-12-22T15:18:47.059Z",
        "type": "webhook",
        "actor": {
          "login": "NahlaEssam",
          "avatar_url": "https://avatars0.githubusercontent.com/u/7237940?v=4"
        }
      },
      "vcs": {
        "origin_repository_url": "https://github.com/LF-Engineering/dev-analytics-ui",
        "target_repository_url": "https://github.com/LF-Engineering/dev-analytics-ui",
        "revision": "9ff2a47d6030a5b6773768f47e58b6475e21f0f1",
        "provider_name": "GitHub",
        "commit": {
          "body": "",
          "subject": "fix DA-3208: Mobile View - Project dashboard"
        },
        "branch": "DA-3205"
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

    Worflows endpoint: `https://circleci.com/api/v2/pipeline/3f0abeea-1c0a-4922-bf63-ad87c469b668/workflow`. This will return the following
    ```
        {
    "next_page_token" : null,
    "items" : [ {
        "pipeline_id" : "3f0abeea-1c0a-4922-bf63-ad87c469b668",
        "id" : "1a912544-8530-47d3-81c2-64c9f02dd254",
        "name" : "build_and_deploy",
        "project_slug" : "gh/LF-Engineering/dev-analytics-api",
        "status" : "success",
        "started_by" : "a68db03b-ff65-4156-bcbb-7d386ee0c692",
        "pipeline_number" : 4151,
        "created_at" : "2020-12-18T20:27:44Z",
        "stopped_at" : "2020-12-18T20:29:31Z"
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
            "job_number" : 4189,
            "id" : "c3055f24-ffde-4e51-a0b9-28959010188d",
            "started_at" : "2020-12-18T20:27:50Z",
            "name" : "build_and_test",
            "project_slug" : "gh/LF-Engineering/dev-analytics-api",
            "status" : "success",
            "type" : "build",
            "stopped_at" : "2020-12-18T20:29:31Z"
        } ]
        }
     ```

    - From the reponse above you can get the job_number. which will be used to get the job details

    Endpoint: `https://circleci.com/api/v2/project/gh/LF-Engineering/dev-analytics-api/job/4189`

    Response:
    ```
    {
        "web_url" : "https://circleci.com/gh/LF-Engineering/dev-analytics-api/4189",
        "project" : {
            "external_url" : "https://github.com/LF-Engineering/dev-analytics-api",
            "slug" : "gh/LF-Engineering/dev-analytics-api",
            "name" : "dev-analytics-api"
        },
        "parallel_runs" : [ {
            "index" : 0,
            "status" : "success"
        } ],
        "started_at" : "2020-12-18T20:27:50.097Z",
        "latest_workflow" : {
            "name" : "build_and_deploy",
            "id" : "1a912544-8530-47d3-81c2-64c9f02dd254"
        },
        "name" : "build_and_test",
        "executor" : {
            "resource_class" : "large",
            "type" : "docker"
        },
        "parallelism" : 1,
        "status" : "success",
        "number" : 4189,
        "pipeline" : {
            "id" : "3f0abeea-1c0a-4922-bf63-ad87c469b668"
        },
        "duration" : 101446,
        "created_at" : "2020-12-18T20:27:44.816Z",
        "messages" : [ ],
        "contexts" : [ ],
        "organization" : {
            "name" : "LF-Engineering"
        },
        "queued_at" : "2020-12-18T20:27:44.844Z",
        "stopped_at" : "2020-12-18T20:29:31.543Z"
        }
    ```
3. Use the next_page_token to navigate to the next page where necessary.

