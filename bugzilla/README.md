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
    - bugzilla url
- DA_BUGZILLA_SHCONNSTRING={}
    - Sorting hat connection string
- DA_BUGZILLA_FETCHERBACKENDVERSION={}
    - Fetcher version
- DA_BUGZILLA_ENRICHERBACKENDVERSION={}
    - Enricher version
- DA_BUGZILLA_FETCH={1,0}
    - To decide whether will do fetch or not.
- DA_BUGZILLA_ENRICH={1,0}
    - To decide whether will do enrichment step or not.
- DA_BUGZILLA_ESURL=''
    - Elastic search url .
- DA_BUGZILLA_USERNAME=''
    - Elastic search credentials
- DA_BUGZILLA_PASSWORD=''
    - Elastic search credentials
- DA_BUGZILLA_ESINDEX=''
    - Elastic search index name .
- DA_BUGZILLA_FROMDATE=''
    - Optional, date to start sync from .
- DA_BUGZILLA_PROJECT=''
    - Slug name e.g. yocto.
- DA_BUGZILLA_FETCHSIZE=25
    - Page size for pagination purpose
- DA_BUGZILLA_ENRICHSIZE=25
    - Page size for pagination purpose
- DA_DS='{DATASOURCE}'
    - Datasource name should be 'Bugzilla'