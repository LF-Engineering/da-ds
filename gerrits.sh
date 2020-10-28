#!/bin/bash
for d in `cat gerrits.dajeff.secret`
do
  ary=(${d//;/ })
  DA_DS=gerrit DA_GERRIT_ENRICH='' DA_GERRIT_ES_URL="${ES_URL}" DA_GERRIT_RAW_INDEX=dads-gerrit-raw DA_GERRIT_RICH_INDEX=dads-gerrit DA_GERRIT_DEBUG=1 DA_GERRIT_DB_PORT=13306 DA_GERRIT_DB_NAME=sortinghat DA_GERRIT_DB_USER=sortinghat DA_GERRIT_DB_PASS=pwd DA_GERRIT_NO_SSL_VERIFY=1 DA_GERRIT_DISABLE_HOST_KEY_CHECK=1 DA_GERRIT_MAX_REVIEWS='' DA_GERRIT_URL="${ary[0]}" DA_GERRIT_USER="${ary[1]}" DA_GERRIT_SSH_KEY_PATH="${ary[2]}" ./dads
done
