#!/bin/bash
DA_PIPERMAIL_ENRICH=0 \
DA_PIPERMAIL_ES_URL=http://elastic:changeme@127.0.0.1:9200 \
DA_PIPERMAIL_NO_INCREMENTAL=1 \
DA_PIPERMAIL_LINKS_JSON='[{"ProjectSlug":"test-yocto-pipermail","GroupName":"openembedded-architecture","Link":"https://www.openembedded.org/pipermail/openembedded-architecture/","ESIndex":"sds-yocto-dads-pipermail"}]' \
DA_DS=pipermail \
DA_PIPERMAIL_HTTP_TIMEOUT=60s \
DA_PIPERMAIL_RAW_INDEX="sds-yocto-dads-pipermail-raw" \
DA_PIPERMAIL_RICH_INDEX="sds-yocto-dads-pipermail" \
./dads
