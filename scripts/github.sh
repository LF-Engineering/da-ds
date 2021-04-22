#!/bin/bash
# dev-analytics-import-sh-json/README.md: PASS=rootpwd ./mariadb_local_docker.sh
# dev-analytics-import-sh-json/README.md: USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_init.sh
# dev-analytics-import-bitergia-indexes/README.md: ./es_local_docker.sh
# dev-analytics-affiliation: ./sh/psql.sh docker, then ./sh/psql.sh
# dev-analytics-affiliation: ./sh/local_api.sh
# Example: DA_GITHUB_RETRY=1 ORGREPO2='LF-Engineering/da-ds' CLEAN=1 REPOSITORY='' ISSUE=1 PULLREQUEST='' CURL=1 REFRESH='' ./scripts/github.sh
# DA_GITHUB_NO_AFFILIATION=1
if [ -z "$ORGREPO" ]
then
  ORGREPO='cncf/devstats'
fi
ary=(${ORGREPO//\// })
ORG="${ary[0]}"
REPO="${ary[1]}"
export AUTH0_DATA="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/AUTH0_DATA.prod.secret`"
export DA_DS=github
export DA_GITHUB_AFFILIATION_API_URL='http://127.0.0.1:8080'
export DA_GITHUB_DB_HOST=127.0.0.1 
export DA_GITHUB_DB_NAME=shdb
export DA_GITHUB_DB_USER=shusername
export DA_GITHUB_DB_PASS=shpwd
export DA_GITHUB_DB_PORT=13306
export DA_GITHUB_ES_URL='http://127.0.0.1:19200' 
export DA_GITHUB_TOKENS="`cat /etc/github/oauths`" 
export DA_GITHUB_ORG="$ORG"
export DA_GITHUB_REPO="$REPO"
export DA_GITHUB_ENRICH=1
export DA_GITHUB_DEBUG=1 
export PROJECT_SLUG="$ORGREPO" 
if [ ! -z "$REFRESH" ]
then
  export DA_GITHUB_NO_RAW=1
  export DA_GITHUB_REFRESH_AFFS=1
  export DA_GITHUB_FORCE_FULL=1
fi
if [ ! -z "$CLEAN" ]
then
  echo "delete from uidentities" | mysql -h127.0.0.1 -P13306 -prootpwd -uroot shdb || exit 1
  curl -s -XDELETE 'http://127.0.0.1:19200/*' || exit 1
  echo ''
fi
echo 'da-ds github'
if [ ! -z "$REPOSITORY" ]
then
  DA_GITHUB_RAW_INDEX=sds-da-ds-gh-api-github-repository-raw DA_GITHUB_RICH_INDEX=sds-da-ds-gh-api-github-repository DA_GITHUB_CATEGORY=repository ./dads 2>&1 | tee run-repository.log
  if [ ! -z "$CURL"]
  then
    curl -s 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-repository-raw/_search?size=10000' | jq -S '.hits.hits[]._source' > github-repository-raw.json
    curl -s 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-repository/_search?size=10000' | jq -S '.hits.hits[]._source' > github-repository-rich.json
  fi
fi
if [ ! -z "$ISSUE" ]
then
  #DA_GITHUB_DATE_FROM=2021-01-01 DA_GITHUB_RAW_INDEX=sds-da-ds-gh-api-github-issue-raw DA_GITHUB_RICH_INDEX=sds-da-ds-gh-api-github-issue DA_GITHUB_CATEGORY=issue ./dads 2>&1 | tee run-issue.log
  DA_GITHUB_RAW_INDEX=sds-da-ds-gh-api-github-issue-raw DA_GITHUB_RICH_INDEX=sds-da-ds-gh-api-github-issue DA_GITHUB_CATEGORY=issue ./dads 2>&1 | tee run-issue.log
  if [ ! -z "$CURL" ]
  then
    curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-issue-raw/_search?size=10000' -d '{"query":{"term":{"is_github_issue":1}}}' | jq -S '.hits.hits[]._source' > github-issue-raw.json
    curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-issue/_search?size=10000' -d '{"query":{"term":{"is_github_issue":1}}}' | jq -S '.hits.hits[]._source' > github-issue-rich.json
  fi
fi
if [ ! -z "$PULLREQUEST" ]
then
  DA_GITHUB_RAW_INDEX=sds-da-ds-gh-api-github-issue-raw DA_GITHUB_RICH_INDEX=sds-da-ds-gh-api-github-issue DA_GITHUB_CATEGORY=pull_request ./dads 2>&1 | tee run-pull-request.log
  if [ ! -z "$CURL"]
  then
    curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-issue-raw/_search?size=10000' -d '{"query":{"term":{"is_github_pull_request":1}}}' | jq -S '.hits.hits[]._source' > github-pull-request-raw.json
    curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-da-ds-gh-api-github-issue/_search?size=10000' -d '{"query":{"term":{"is_github_pull_request":1}}}' | jq -S '.hits.hits[]._source' > github-pull-request-rich.json
  fi
fi
