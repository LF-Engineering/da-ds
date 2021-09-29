#!/bin/bash
# start=1
start=1
e=0
end=$(curl -s 'https://wiki.anuket.io/rest/api/content/search?cql=lastModified%3E%3D%272000-01-01+00%3A00%27+order+by+lastModified&limit=1&start=1' | jq '.totalSize')
while true
do
  code=$(curl -s "https://wiki.anuket.io/rest/api/content/search?cql=lastModified%3E%3D%272000-01-01+00%3A00%27+order+by+lastModified&limit=1&start=${start}&expand=ancestors%2Cversion" | jq -rS '.statusCode')
  if [ "$code" = "500" ]
  then
    json=$(curl -s "https://wiki.anuket.io/rest/api/content/search?cql=lastModified%3E%3D%272000-01-01+00%3A00%27+order+by+lastModified&limit=1&start=${start}" | jq -rS '.results[0]')
    id=$(echo "$json" | jq -rS '.id')
    title=$(echo "$json" | jq -rS '.title')
    link=$(echo "$json" | jq -rS '._links.self')
    e=$((e+1))
    echo "$e) index=$start, id=$id: \"$title\": $link"
  fi
  start=$((start+1))
  if [ "$start" = "$end" ]
  then
    break
  fi
done
echo "$e error pages"
