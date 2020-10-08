#!/bin/bash
if [ -z "$DOCKER_USER" ]
then
  DOCKER_USER=`docker info 2>/dev/null | grep User | awk '{print $2}'`
fi
if [ -z "$DOCKER_USER" ]
then
  echo "$0: cannot detect your docker user, specify one with DOCKER_USER=..."
  exit 1
fi
echo "Building as $DOCKER_USER"
docker build -f ./Dockerfile -t "${DOCKER_USER}/cython" .
