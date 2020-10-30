#!/bin/bash
# ES_URL=... _ID=914eae314d14f071f873dd669b60569a9702471e
# _ID=4a0e886ac66fede1e5d362292f93182ac7510126_changeset_5754
# _ID=3dce4c3739f027c096ff921d172b9f9174bef90d_changeset_6141_comment_1576597948.0
# curl -s "${ES_URL}/dads-gerrit/_search" | jq '.hits.hits[]._source.id'
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
curl -s -H 'Content-Type: application/json' "${ES_URL}/dads-gerrit/_search" -d "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > dads.json
curl -s -H 'Content-Type: application/json' "${ES_URL}/sds-lfai-acumos-gerrit/_search" -d  "{\"query\":{\"term\":{\"_id\":\"${_ID}\"}}}" | jq '.' > p2o.json
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
