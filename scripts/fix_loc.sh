#!/bin/bash
ES="`cat ../sync-data-sources/helm-charts/sds-helm/sds-helm/secrets/ES_URL.$1.secret`"
indices=`curl -s "${ES}/_cat/indices?format=json" | jq -rS '.[].index' | grep 'sds-' | grep -v bitergia | grep -v github | grep -v raw | grep git | uniq | sort`
for i in $indices
do
  # TODO: need to fetch distinct origins there, and then for each non-empty origin
  data=$(curl -s -XPOST -H 'Content-Type: application/json' "${ES}/_sql?format=json" -d"{\"query\":\"select min(total_lines_of_code), max(total_lines_of_code) from \\\"${i}\\\"\"}" | jq --compact-output -r ".rows[0]")
  mi=$(echo "$data" | jq --compact-output -r '.[0]')
  ma=$(echo "$data" | jq --compact-output -r '.[1]')
  # FIXME: temporary debug
  echo "$i: ($mi,$ma)"
  continue
  if ( [ "$mi" = "0" ] && [ ! "$ma" = "0" ] && [ ! -z "$ma" ] )
  then
    result=$(curl -s -XPOST -H 'Content-Type: application/json' "${ES}/${i}/_update_by_query?pretty" -d"{\"script\":{\"inline\":\"ctx._source.total_lines_of_code=\\\"${ma}\\\";\"},\"query\":{\"term\":{\"total_lines_of_code\":\"0\"}}}" | jq -rS --compact-output '.updated')
    echo "$i set $ma LOC result: $result"
  else
    echo "$i $mi - $ma skipped"
  fi
done
