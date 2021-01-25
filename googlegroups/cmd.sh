#!/bin/bash

setUp(){

venv=/Users/code/da-ds/googlegroups/venv
workDir=/Users/code/da-ds/googlegroups/archives
jsonFilesDir=/Users/code/da-ds/googlegroups/jsonfiles
mboxScript=/Users/code/da-ds/googlegroups/ggmbox.py
logsDir=/Users/code/da-ds/googlegroups/logs/

cd $workDir || exit

jsonFile=${jsonFilesDir}"/""$1".json
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
logfile=${logsDir}"$1".${current_time}.txt

# delete existing json file
rm -rf "${jsonFile}"

if [ -e "$venv" ]; then
  source $venv/bin/activate
  scrapy runspider -a name="$1" -o "${jsonFile}" -t json ${mboxScript} > "${logfile}" 2>&1
else
  virtualenv --python=python3 $venv
  source $venv/bin/activate
  pip install scrapy
  scrapy runspider -a name="$1" -o "${jsonFile}" -t json ${mboxScript} > "${logfile}" 2>&1
fi
}

main(){
    setUp "$1"
}

main "$@"
