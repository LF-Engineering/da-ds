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
page=500
to=''
if [ ! -z "$PAGE" ]
then
  page=$PAGE
  if [ "$page" -ge "500" ]
  then
    echo"setting page size to 500, it cannot be any bigger"
    page=500
  fi
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
  echo -n "from:$from page:$page "
  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i "${KEY}" -p 29418 "${USR}@${GERRIT}" gerrit query after:"1970-01-01 00:00:00" "limit:$page" '(status:open OR status:closed)' --all-approvals --all-reviewers --comments --format=JSON --start="$from" 1>./out 2>/dev/null
  rows=$(cat ./out | grep '"rowCount"' | jq -rS '.rowCount')
  echo "rows:$rows"
  cat ./out >> "${fn}"
  if ( [ "$rows" = "0" ] || [ -z "$rows" ] )
  then
    echo "finished, rows: $rows"
    break
  fi
  from=$(($from + $page))
  if ( [ ! -z "$to" ] && [ "$from" -ge "$to" ] )
  then
    echo "$from >= $to, finished"
    break
  fi
done
