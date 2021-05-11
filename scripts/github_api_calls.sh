#!/bin/bash
#echo 'PR:'
#curl -s -u "lukaszgryglicki:`cat /etc/github/oauth`" https://api.github.com/repos/lukaszgryglicki/csqconv/pulls/3
echo 'comments:'
curl -s -u "lukaszgryglicki:`cat /etc/github/oauth`" https://api.github.com/repos/lukaszgryglicki/csqconv/pulls/3/comments | jq ".[]\(.body)"
echo 'reviews:'
curl -s -u "lukaszgryglicki:`cat /etc/github/oauth`" https://api.github.com/repos/lukaszgryglicki/csqconv/pulls/3/reviews | jq ".[]\(.body)"
