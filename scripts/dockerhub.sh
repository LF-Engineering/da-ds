#!/bin/bash
DA_DOCKERHUB_ENRICH=1 \
DA_DOCKERHUB_ES_URL=http://elastic:changeme@127.0.0.1:9200 \
DA_DOCKERHUB_NO_INCREMENTAL=1 \
DA_DOCKERHUB_USERNAME="" \
DA_DOCKERHUB_PASSWORD='' \
DA_DOCKERHUB_REPOSITORIES_JSON='[{"Owner":"crops","Repository":"yocto-eol","ESIndex":"sds-yocto-dockerhub"}]' \
DA_DS=dockerhub \
DA_DOCKERHUB_HTTP_TIMEOUT=60s \
./dads

