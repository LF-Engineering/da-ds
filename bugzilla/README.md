Bugzilla Datasource
=========

Bugzilla datasource is a package to fetch data 
from dockerhub API and save it into Elasticsearch
 and Enrich saved data.


### Bugzilla Running instructions

To run Bugzilla datasource from dads you 
must set proper environment variables to 
select Bugzilla as an engine and other 
parameters that determine the intended behavior.

These are the needed environment variables to run Bugzilla DA-DS:
- DA_BUGZILLA_ENDPOINT={}
    - bugzilla origin url
- DA_BUGZILLA_AFFILIATION_CONN_STRING={}
    - Affiliation database connection string
- DA_BUGZILLA_FETCHER_BACKEND_VERSION={}
    - Fetcher version
- DA_BUGZILLA_ENRICHER_BACKEND_VERSION={}
    - Enricher version
- DA_BUGZILLA_FETCH={1,0}
    - To decide whether will fetch raw data or not.
- DA_BUGZILLA_ENRICH={1,0}
    - To decide whether will do enrich raw data or not.
- DA_BUGZILLA_ES_URL=''
    - Elastic search url.
- DA_BUGZILLA_ES_USERNAME=''
    - Elastic search credentials
- DA_BUGZILLA_ES_PASSWORD=''
    - Elastic search credentials
- DA_BUGZILLA_ES_INDEX=''
    - Elastic search index name .
- DA_BUGZILLA_FROM_DATE=''
    - Optional, date to start syncing from.
- DA_BUGZILLA_PROJECT=''
    - Slug name of a project e.g. yocto.
- DA_BUGZILLA_FETCH_SIZE=25
    - total number of fetched items per request.
- DA_BUGZILLA_ENRICH_SIZE=25
    - total number of enriched items per request
