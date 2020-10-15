#!/bin/bash
if [ -z "${ES_URL}" ]
then
  echo "$0: you must set ES_URL"
  exit 1
fi
if [ -z "${_ID}" ] 
then
  echo "$0: you must set _ID"
  exit 2
fi
curl -s -H 'Content-Type: application/json' "${ES_URL}/dads-test/_search" -d "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > dads.json
curl -s -H 'Content-Type: application/json' "${ES_URL}/sds-lfn-odl-jira/_search" -d  "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > p2o.json
cat p2o.json | sort -r | uniq > tmp && mv tmp p2o.txt
cat dads.json | sort -r | uniq > tmp && mv tmp dads.txt
