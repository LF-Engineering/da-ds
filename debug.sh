#!/bin/bash
if [ -z "${ES_URL}" ]
then
  echo "$0: you must set ES_URL"
  exit 1
fi
echo "Example breakpoint: break github.com/LF-Engineering/da-ds.DSJira.AffsItems"
DA_DS=jira DA_JIRA_ENRICH=1 DA_JIRA_ES_URL="${ES_URL}" DA_JIRA_RAW_INDEX=dads-test-raw2 DA_JIRA_RICH_INDEX=dads-test DA_JIRA_DEBUG=1 DA_JIRA_DB_PORT=13306 DA_JIRA_DB_NAME=sortinghat DA_JIRA_DB_USER=sortinghat DA_JIRA_DB_PASS=pwd DA_JIRA_URL=https://jira.opendaylight.org dlv debug github.com/LF-Engineering/da-ds/cmd/dads
