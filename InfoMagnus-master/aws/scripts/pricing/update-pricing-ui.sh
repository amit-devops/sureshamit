#!/usr/bin/env bash
set -e

if [[ -z "$1" ]]; then
echo usage: "$0 version profile"
echo "where version is the container tag version"
echo "      profile is the kubernetes context to use"
exit 1
fi

VERSION=$1
ENVNAME=$2

kubectl config use-context $ENVNAME
if [[ ! -z $REMOTE_DOCKER_REPO ]]; then
  kubectl set image deployment smartorder-server smartorder-ui=$REMOTE_DOCKER_REPO/smartorder/web-ui:$ENVNAME-$VERSION
else
  kubectl set image deployment smartorder-server smartorder-ui=smartorder/web-ui:$ENVNAME-$VERSION
fi
