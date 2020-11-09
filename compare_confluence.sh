#!/bin/bash
# ES_URL=...
# _ID=ba6f08b6d2527009f4f2f5b359336073cd680edf
# curl -s "${ES_URL}/dads-confluence/_search" | jq '.hits.hits[]._source.uuid'
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
curl -s -H 'Content-Type: application/json' "${ES_URL}/dads-confluence/_search" -d "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > dads.json
curl -s -H 'Content-Type: application/json' "${ES_URL}/sds-lfn-shared-confluence/_search" -d  "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > p2o.json
cat p2o.json | sort -r | uniq > tmp && mv tmp p2o.txt
cat dads.json | sort -r | uniq > tmp && mv tmp dads.txt
echo "da-ds:" > report.txt
echo '-------------------------------------------' >> report.txt
cat dads.txt >> report.txt
echo '-------------------------------------------' >> report.txt
echo "p2o:" >> report.txt
echo '-------------------------------------------' >> report.txt
cat p2o.txt >> report.txt
echo '-------------------------------------------' >> report.txt
