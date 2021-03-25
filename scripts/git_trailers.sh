#!/bin/bash
if [ ! -f "git.log" ]
then
  ./scripts/git_log.sh > git.log || exit 1
fi
grep -E "^[[:space:]]+[a-zA-z0-9-]+:.+[[:space:]]+<.+>[[:space:]]*$" git.log | sort | uniq
