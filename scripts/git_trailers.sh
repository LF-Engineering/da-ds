#!/bin/bash
if [ ! -f "git.log" ]
then
  ./scripts/git_log.sh > git.log || exit 1
fi
grep -E "^[[:space:]]{2,}[a-zA-z0-9\-]+:.+<.+>.*$" git.log | sort | uniq
