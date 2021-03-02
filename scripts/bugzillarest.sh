#!/bin/bash
DA_BUGZILLAREST_ES_URL=http://elastic:changeme@127.0.0.1:9200 \
DA_BUGZILLAREST_USERNAME="" \
DA_BUGZILLAREST_PASSWORD="" \
DA_DS=bugzillarest \
DA_BUGZILLAREST_GAP_URL=localhost:80000 \
DA_BUGZILLAREST_AFFILIATION_API_URL=$1 \
DA_BUGZILLAREST_ES_CACHE_URL=$2 \
DA_BUGZILLAREST_ES_CACHE_USERNAME=$3 \
DA_BUGZILLAREST_ES_CACHE_PASSWORD=$4 \
DA_BUGZILLAREST_AUTH0_GRANT_TYPE=$5 \
DA_BUGZILLAREST_AUTH0_CLIENT_ID=$6 \
DA_BUGZILLAREST_AUTH0_CLIENT_SECRET=$7 \
DA_BUGZILLAREST_AUTH0_AUDIENCE=$8 \
DA_BUGZILLAREST_AUTH0_URL=$9 \
DA_BUGZILLAREST_BRANCH=${10} \
RAW_INDEX=sds-test-dpdk \
./dads --bugzillarest-origin=https://bugs.dpdk.org/ \
--bugzillarest-project=dpdk \
 --bugzillarest-do-fetch=true --bugzillarest-do-enrich=true --bugzillarest-fetch-size=25 \
 --bugzillarest-enrich-size=25
