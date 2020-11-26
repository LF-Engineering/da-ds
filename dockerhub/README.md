Dockerhub Datasource
=========

Dockerhub datasource is a package to fetch data 
from dockerhub API and save it into Elasticsearch
 and Enrich saved data.


### Docker Running instructions

To run dockerhub datasource from dads you 
must set proper environment variables to 
select dockerhub as an engine and other 
parameters that determine the intended behavior.

These are the needed environment variables to run dockerhub:
- DA_DOCKERHUB_ENRICH={1,0}
    - To decide whether will do enrichment step or not.
- DA_DOCKERHUB_ES_URL=http://{ES_USERNAME}:{ES_PASSWORD}@{URL}:{PORT}
    - Elasticsearch url included username, password, host and port
- DA_DOCKERHUB_NO_INCREMENTAL={1,0} 
    - Starts from the beginning if 1 is selected and will not use date to continue enriching 
- DA_DOCKERHUB_USERNAME=''
    - Optional, for dockerhub repository credentials
- DA_DOCKERHUB_PASSWORD='' 
    - Optional, for dockerhub repository credentials
- DA_DOCKERHUB_PROJECT_SLUG='{SLUG}'
    - Slug name e.g. yocto
- DA_DOCKERHUB_REPOSITORIES_JSON='[{"Owner":'{OWNER}',"Repository":"{REPOSITORY}","ESIndex":"{INDEX_NAME}"}]'
    - JSON  e.g. '[{"Owner":"crops","Repository":"yocto-eol","ESIndex":"sds-yocto-dockerhub"}]'
- DA_DS='{DATASOURCE}' 
    - Datasource name should be 'dockerhub'
- DA_DOCKERHUB_HTTP_TIMEOUT=60s 
    - HTTP timeout duration.
    
Example of running dads at 
`./scripts/dockerhub.sh`
on the main directory.



