#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: you need to specify env: test|prod"
  exit 1
fi
export PROJECT_SLUG='lg'
export DA_DS=git
export DA_GIT_NO_AFFILIATION=1
export DA_GIT_DB_HOST="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/SH_HOST.$1.secret`" 
export DA_GIT_DB_NAME="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/SH_DB.$1.secret`"
export DA_GIT_DB_PASS="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/SH_PASS.$1.secret`"
export DA_GIT_DB_PORT="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/SH_PORT.$1.secret`"
export DA_GIT_DB_USER="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/SH_USER.$1.secret`"
export DA_GIT_ES_URL="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/ES_URL.$1.secret`"
export DA_GIT_LEGACY_UUID=''
export DA_GIT_PROJECT_SLUG='lg'
export DA_GIT_RAW_INDEX=lg-test-raw
export DA_GIT_RICH_INDEX=lg-test
export DA_GIT_DROP_RAW=1
export DA_GIT_DROP_RICH=1
export DA_GIT_URL='https://github.com/lukaszgryglicki/test-api'
export DA_GIT_PAIR_PROGRAMMING=''
export DA_GIT_ENRICH=1
export DA_GIT_DEBUG=''
./dads 2>&1 | tee run.log
