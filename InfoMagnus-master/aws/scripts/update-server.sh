#!/usr/bin/env bash
set -e

if [[ -z "$2" ]]; then
echo "usage: $0 version profile"
echo "where version is the container tag version"
echo "      profile is the kubernetes context to use"
exit 1
fi

VERSION=$1
ENVNAME=$2

kubectl config use-context $ENVNAME
if [[ ! -z $REMOTE_DOCKER_REPO ]]; then
  kubectl set image deployment smartorder-server controller=$REMOTE_DOCKER_REPO/smartorder/server:$ENVNAME-$VERSION
  kubectl set image deployment smartorder-server denormalizer=$REMOTE_DOCKER_REPO/smartorder/server:$ENVNAME-$VERSION
  kubectl set image deployment smartorder-server prediction=$REMOTE_DOCKER_REPO/smartorder/server:$ENVNAME-$VERSION
fi
