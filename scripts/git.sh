#!/bin/bash
echo "delete from uidentities" | mysql -h127.0.0.1 -P13306 -prootpwd -uroot sortinghat
curl -s -XDELETE 'http://127.0.0.1:19200/*'
PROJECT_SLUG='act/spdx' DA_DS=git DA_GIT_NO_AFFILIATION=1 DA_GIT_DB_HOST=127.0.0.1 DA_GIT_DB_NAME=sortinghat DA_GIT_DB_PASS=pwd DA_GIT_DB_PORT=13306 DA_GIT_DB_USER=sortinghat DA_GIT_ES_URL='http://127.0.0.1:19200' DA_GIT_LEGACY_UUID='' DA_GIT_PROJECT_SLUG='act/spdx' DA_GIT_RAW_INDEX=da-ds-git-raw DA_GIT_RICH_INDEX=dads-git DA_GIT_URL='https://github.com/SPDX/tools-python' DA_GIT_PAIR_PROGRAMMING='' DA_GIT_ENRICH=1 DA_GIT_DEBUG=1 ./dads 2>&1 | tee run.log
