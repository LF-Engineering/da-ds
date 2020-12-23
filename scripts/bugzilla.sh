#!/bin/bash
DA_BUGZILLA_ES_URL=http://elastic:changeme@127.0.0.1:9200 \
DA_BUGZILLA_USERNAME="" \
DA_BUGZILLA_PASSWORD="" \
DA_DS=bugzilla \
RAW_INDEX=sds-test-yocto \
./dads --bugzilla-origin=https://bugzilla.yoctoproject.org \
--bugzilla-project=yocto \
 --bugzilla-do-fetch=true --bugzilla-do-enrich=true --bugzilla-fetch-size=25 \
 --bugzilla-enrich-size=25


