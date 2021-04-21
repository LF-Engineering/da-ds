#!/bin/bash
# dev-analytics-import-sh-json/README.md: PASS=rootpwd ./mariadb_local_docker.sh
# dev-analytics-import-sh-json/README.md: USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_init.sh
# dev-analytics-import-bitergia-indexes/README.md: ./es_local_docker.sh
if [ ! -z "$CLEAN" ]
then
  echo "delete from uidentities" | mysql -h127.0.0.1 -P13306 -prootpwd -uroot shdb || exit 1
  curl -s -XDELETE 'http://127.0.0.1:19200/*' || exit 1
  echo ''
fi
echo 'da-ds github'
if [ ! -z "$REPOSITORY" ]
then
  PROJECT_SLUG='cncf/devstats' DA_DS=github DA_GITHUB_NO_AFFILIATION=1 DA_GITHUB_DB_HOST=127.0.0.1 DA_GITHUB_DB_NAME=shdb DA_GITHUB_DB_PASS=shpwd DA_GITHUB_DB_PORT=13306 DA_GITHUB_DB_USER=shusername DA_GITHUB_ES_URL='http://127.0.0.1:19200' DA_GITHUB_PROJECT_SLUG='cncf/devstats' DA_GITHUB_RAW_INDEX=sds-cncf-devstats-github-repository-raw DA_GITHUB_RICH_INDEX=sds-cncf-devstats-github-repository DA_GITHUB_ORG=cncf DA_GITHUB_REPO=devstats DA_GITHUB_CATEGORY=repository DA_GITHUB_TOKENS="`cat /etc/github/oauths`" DA_GITHUB_ENRICH=1 DA_GITHUB_DEBUG=1 ./dads 2>&1 | tee run-repository.log
  curl -s 'http://127.0.0.1:19200/sds-cncf-devstats-github-repository-raw/_search?size=1000' | jq -S '.hits.hits[]._source' | tee github-repository-raw.json
  curl -s 'http://127.0.0.1:19200/sds-cncf-devstats-github-repository/_search?size=1000' | jq -S '.hits.hits[]._source' | tee github-repository-rich.json
fi
if [ ! -z "$ISSUE" ]
then
  #PROJECT_SLUG='cncf/devstats' DA_DS=github DA_GITHUB_DATE_FROM=2021-01-01 DA_GITHUB_NO_AFFILIATION=1 DA_GITHUB_DB_HOST=127.0.0.1 DA_GITHUB_DB_NAME=shdb DA_GITHUB_DB_PASS=shpwd DA_GITHUB_DB_PORT=13306 DA_GITHUB_DB_USER=shusername DA_GITHUB_ES_URL='http://127.0.0.1:19200' DA_GITHUB_PROJECT_SLUG='cncf/devstats' DA_GITHUB_RAW_INDEX=sds-cncf-devstats-github-issue-raw DA_GITHUB_RICH_INDEX=sds-cncf-devstats-github-issue DA_GITHUB_ORG=cncf DA_GITHUB_REPO=devstats DA_GITHUB_CATEGORY=issue DA_GITHUB_TOKENS="`cat /etc/github/oauths`" DA_GITHUB_ENRICH=1 DA_GITHUB_DEBUG=1 ./dads 2>&1 | tee run-issue.log
  PROJECT_SLUG='cncf/devstats' DA_DS=github DA_GITHUB_NO_AFFILIATION=1 DA_GITHUB_DB_HOST=127.0.0.1 DA_GITHUB_DB_NAME=shdb DA_GITHUB_DB_PASS=shpwd DA_GITHUB_DB_PORT=13306 DA_GITHUB_DB_USER=shusername DA_GITHUB_ES_URL='http://127.0.0.1:19200' DA_GITHUB_PROJECT_SLUG='cncf/devstats' DA_GITHUB_RAW_INDEX=sds-cncf-devstats-github-issue-raw DA_GITHUB_RICH_INDEX=sds-cncf-devstats-github-issue DA_GITHUB_ORG=cncf DA_GITHUB_REPO=devstats DA_GITHUB_CATEGORY=issue DA_GITHUB_TOKENS="`cat /etc/github/oauths`" DA_GITHUB_ENRICH=1 DA_GITHUB_DEBUG=1 ./dads 2>&1 | tee run-issue.log
  curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-cncf-devstats-github-issue-raw/_search?size=1000' -d '{"query":{"term":{"is_github_issue":1}}}' | jq -S '.hits.hits[]._source' | tee github-issue-raw.json
  curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-cncf-devstats-github-issue/_search?size=1000' -d '{"query":{"term":{"is_github_issue":1}}}' | jq -S '.hits.hits[]._source' | tee github-issue-rich.json
fi
if [ ! -z "$PULLREQUEST" ]
then
  PROJECT_SLUG='cncf/devstats' DA_DS=github DA_GITHUB_NO_AFFILIATION=1 DA_GITHUB_DB_HOST=127.0.0.1 DA_GITHUB_DB_NAME=shdb DA_GITHUB_DB_PASS=shpwd DA_GITHUB_DB_PORT=13306 DA_GITHUB_DB_USER=shusername DA_GITHUB_ES_URL='http://127.0.0.1:19200' DA_GITHUB_PROJECT_SLUG='cncf/devstats' DA_GITHUB_RAW_INDEX=sds-cncf-devstats-github-issue-raw DA_GITHUB_RICH_INDEX=sds-cncf-devstats-github-issue DA_GITHUB_ORG=cncf DA_GITHUB_REPO=devstats DA_GITHUB_CATEGORY=pull_request DA_GITHUB_TOKENS="`cat /etc/github/oauths`" DA_GITHUB_ENRICH=1 DA_GITHUB_DEBUG=1 ./dads 2>&1 | tee run-pull-request.log
  curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-cncf-devstats-github-issue-raw/_search?size=1000' -d '{"query":{"term":{"is_github_pull_request":1}}}' | jq -S '.hits.hits[]._source' | tee github-pull-request-raw.json
  curl -s -XPOST -H 'Content-Type: application/json' 'http://127.0.0.1:19200/sds-cncf-devstats-github-issue/_search?size=1000' -d '{"query":{"term":{"is_github_pull_request":1}}}' | jq -S '.hits.hits[]._source' | tee github-pull-request-rich.json
fi
