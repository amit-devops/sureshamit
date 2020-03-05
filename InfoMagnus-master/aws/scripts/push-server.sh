#!/usr/bin/env bash
set -e

if [[ -z "$2" ]]; then
echo "usage: $0 version profile [push version]"
echo "where version is the version tag to assign to the repository item"
echo "      profile is the AWS profile to use for connection to the repository"
echo "      push version is the version that should be tagged on the remote repo"
exit 1
fi

VERSION=$1
ENVNAME=$2
PUSH_VERSION=$3
SCRIPTS=`dirname "$(stat --format=%n "$0")"`

if [[ -z $PUSH_VERSION ]]; then
  PUSH_VERSION=$VERSION
fi

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

if [[ ! -z $REMOTE_DOCKER_REPO ]]; then
  if [[ ! -z $ENVNAME ]]; then
  eval `aws --profile $ENVNAME ecr get-login --no-include-email`
  else
  eval `aws ecr get-login --no-include-email`
  fi
  docker tag smartorder/server:$VERSION $REMOTE_DOCKER_REPO/smartorder/server:$PUSH_VERSION
  docker push $REMOTE_DOCKER_REPO/smartorder/server:$PUSH_VERSION
fi
