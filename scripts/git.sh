#!/bin/bash
# dev-analytics-import-sh-json/README.md: PASS=rootpwd ./mariadb_local_docker.sh
# dev-analytics-import-sh-json/README.md: USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_init.sh
# dev-analytics-import-bitergia-indexes/README.md: ./es_local_docker.sh
echo "delete from uidentities" | mysql -h127.0.0.1 -P13306 -prootpwd -uroot shdb || exit 1
curl -s -XDELETE 'http://127.0.0.1:19200/*' || exit 1
echo 'da-ds git'
PROJECT_SLUG='lg' DA_DS=git DA_GIT_NO_AFFILIATION='' DA_GIT_DB_HOST=127.0.0.1 DA_GIT_DB_NAME=shdb DA_GIT_DB_PASS=shpwd DA_GIT_DB_PORT=13306 DA_GIT_DB_USER=shusername DA_GIT_ES_URL='http://127.0.0.1:19200' DA_GIT_LEGACY_UUID='' DA_GIT_PROJECT_SLUG='lg' DA_GIT_RAW_INDEX=da-ds-git-raw DA_GIT_RICH_INDEX=da-ds-git DA_GIT_URL='https://github.com/lukaszgryglicki/trailers-test' DA_GIT_PAIR_PROGRAMMING='' DA_GIT_ENRICH=1 DA_GIT_DEBUG=2 ./dads 2>&1 | tee run.log
curl -s 'http://127.0.0.1:19200/da-ds-git-raw/_search' | jq '.hits.hits[]._source' | tee git-raw.json
curl -s 'http://127.0.0.1:19200/da-ds-git/_search' | jq '.hits.hits[]._source' | tee git-rich.json
