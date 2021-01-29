#!/bin/bash
DA_GOOGLEGROUPS_ENRICH=0 \
DA_GOOGLEGROUPS_ES_URL="" \
DA_GOOGLEGROUPS_NO_INCREMENTAL=1 \
DA_DS=GoogleGroups \
DA_GOOGLEGROUPS_HTTP_TIMEOUT=60s \
DA_GOOGLEGROUPS_DB_CONN="" \
DA_GOOGLEGROUPS_AFFILIATIONS_API_BASE_URL="" \
DA_GOOGLEGROUPS_ES_CACHE_URL="" \
DA_GOOGLEGROUPS_ES_CACHE_USERNAME="" \
DA_GOOGLEGROUPS_ES_CACHE_PASSWORD="" \
DA_GOOGLEGROUPS_AUTH0_GRANT_TYPE="" \
DA_GOOGLEGROUPS_AUTH0_CLIENT_ID="" \
DA_GOOGLEGROUPS_AUTH0_CLIENT_SECRET="" \
DA_GOOGLEGROUPS_AUTH0_AUDIENCE="" \
DA_GOOGLEGROUPS_AUTH0_BASE_URL="" \
DA_GOOGLEGROUPS_ENVIRONMENT="" \
./dads --googlegroups-project=project1 --googlegroups-slug=project1 --googlegroups-groupname=finos.org/legend \
 --googlegroups-do-fetch=true --googlegroups-do-enrich=true --googlegroups-fetch-size=1000 \
 --googlegroups-enrich-size=1000 --googlegroups-es-index=sds-project1-dads-googlegroups
