#!/bin/bash
git log --reverse --topo-order --branches --tags --remotes=origin --raw --numstat --pretty=fuller --decorate=full --parents -M -C -c
