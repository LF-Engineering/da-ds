#!/bin/bash
if [ -z "$USR" ]
then
  echo "$0: you need to specify USR=user-name"
  exit 1
fi
if [ -z "$KEY" ]
then
  echo "$0: you need to specify KEY=/path/to/gerrit/ssh-key"
  exit 2
fi
if [ -z "$GERRIT" ]
then
  echo "$0: you need to specify GERRIT=git.opendaylight.org"
  exit 3
fi
from=0
page=1000
to=''
if [ ! -z "$PAGE" ]
then
  page=$PAGE
fi
if [ ! -z "$FROM" ]
then
  from=$FROM
fi
if [ ! -z "$TO" ]
then
  to=$TO
fi
fn=gerrit.secret
if [ ! -z "${FN}" ]
then
  fn="${FN}"
fi
> "${fn}"
while true
do
  echo "from:$from page:$page"
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "${KEY}" -p 29418 "${USR}@${GERRIT}" gerrit query after:"1970-01-01 00:00:00" "limit:$page" '(status:open OR status:closed)' --all-approvals --all-reviewers --comments --format=JSON --start="$from" 1>./out 2>/dev/null
  rows=$(cat ./out | grep '"rowCount"' | jq -rS '.rowCount')
  cat ./out >> "${fn}"
  if ( [ "$rows" = "0" ] || [ -z "$rows" ] )
  then
    echo "finished, rows: $rows"
    break
  fi
  from=$(($from + $page))
  if [ "$from" -ge "$to" ]
  then
    echo "$from >= $to, finished"
    break
  fi
done
