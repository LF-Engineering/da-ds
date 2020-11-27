#!/bin/bash
DA_DOCKERHUB_ENRICH=0 \
DA_DOCKERHUB_ES_URL=http://elastic:changeme@127.0.0.1:9200 \
DA_DOCKERHUB_NO_INCREMENTAL=1 \
DA_DOCKERHUB_USERNAME="" \
DA_DOCKERHUB_PASSWORD="" \
DA_DOCKERHUB_REPOSITORIES_JSON='[{"Owner":"crops","Repository":"yocto-eol","ESIndex":"sds-yocto-dockerhub"}]' \
DA_DS=dockerhub \
DA_DOCKERHUB_HTTP_TIMEOUT=60s \
DA_DOCKERHUB_RAW_INDEX="sds-yocto-dockerhub-raw" \
DA_DOCKERHUB_RICH_INDEX="sds-yocto-dockerhub" \
./dads

