#!/bin/bash
# ES1_URL='http://127.0.0.1:19200'
# ES2_URL="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/ES_URL.prod.secret`"
# _ID=kubernetes-client/gen/issues/1
# curl -s "${ES1_URL}/sds-da-ds-gh-api-github-issue/_search" | jq '.hits.hits[]._source.url_id'
# curl -s "${ES2_URL}/sds-cncf-k8s-github-issue/_search" | jq '.hits.hits[]._source.url_id'
# ES1_URL='http://127.0.0.1:19200' ES2_URL="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/ES_URL.prod.secret`" _ID=kubernetes-client/gen/issues/1 ./scripts/compare_github.sh
if [ -z "${ES1_URL}" ]
then
  echo "$0: you must set ES1_URL"
  exit 1
fi
if [ -z "${ES2_URL}" ]
then
  echo "$0: you must set ES2_URL"
  exit 2
fi
if [ -z "${_ID}" ] 
then
  echo "$0: you must set _ID"
  exit 3
fi
curl -s -H 'Content-Type: application/json' "${ES1_URL}/sds-da-ds-gh-api-github-issue/_search" -d "{\"query\":{\"term\":{\"url_id\":\"${_ID}\"}}}" | jq '.' > dads.json
curl -s -H 'Content-Type: application/json' "${ES2_URL}/sds-cncf-k8s-github-issue/_search" -d  "{\"query\":{\"term\":{\"url_id\":\"${_ID}\"}}}" | jq '.' > p2o.json
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
