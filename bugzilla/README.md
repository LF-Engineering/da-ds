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

These are the needed environment variables to run Bugzilla:
- DA_BUGZILLA_ENDPOINT={}
    - bugzilla url
