#!/bin/bash

DA_DS=jenkins \
DA_JENKINS_DADS=true \
DA_JENKINS_DB_HOST=db_endpoint \
DA_JENKINS_DB_NAME=db_name \
DA_JENKINS_DB_PASS=password \
DA_JENKINS_DB_PORT=3306 \
DA_JENKINS_DB_USER=user \
DA_JENKINS_ENRICH=1 \
DA_JENKINS_ES_BULK_SIZE=500 \
DA_JENKINS_ES_SCROLL_SIZE=500 \
DA_JENKINS_ES_SCROLL_WAIT=2700s \
DA_JENKINS_ES_URL=https://user:password@url \
DA_JENKINS_HTTP_TIMEOUT=60s \
DA_JENKINS_JENKINS_JSON='[{"url":"https://www.jenkins_url.com","project":"ProjectName","index":"sds-ProjectName-"}]' \
DA_JENKINS_NO_INCREMENTAL=1 \
./dads 
