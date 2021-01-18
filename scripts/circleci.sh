#!/bin/bash
DA_CIRCLECI_ENRICH=0 \
DA_CIRCLECI_ES_URL=http://localhost:9200 \
DA_CIRCLECI_NO_INCREMENTAL=1 \
DA_DS=circleci \
DA_CIRCLECI_HTTP_TIMEOUT=60s \
DA_CIRCLECI_RAW_INDEX="sds-lf-dads-circleci-raw" \
DA_CIRCLECI_RICH_INDEX="sds-lf-dads-circle" \
RAW_INDEX=sds-test-yocto \
DA_CIRCLECI_DB_CONN="" \
./dads --circleci-origin="gh/LF-Engineering/da-ds" \
 --circleci-project="lf" --circleci-slug="lf" \
 --circleci-do-fetch=true --circleci-fetch-size=100 --circleci-es-index=sds-lf-dads-circleci \
 --circleci-token=11111122222233333eeeeee