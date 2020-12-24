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

