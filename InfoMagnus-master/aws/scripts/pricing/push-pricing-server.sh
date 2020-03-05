#!/usr/bin/env bash
set -e

if [[ -z "$1" ]]; then
echo "usage: $0 version profile"
echo "where version is the version tag to assign to the repository item"
echo "      profile is the AWS profile to use for connection to the repository"
exit 1
fi

VERSION=$1
ENVNAME=$2

if [[ -z $SCRIPTS ]]; then
  echo "SCRIPTS environment variable not set"
  echo "SCRIPTS should point to the folder where this script is executing from"
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
  docker tag smartorder/server:$ENVNAME-$VERSION $REMOTE_DOCKER_REPO/smartorder/server:$ENVNAME-$VERSION
  docker push $REMOTE_DOCKER_REPO/smartorder/server:$ENVNAME-$VERSION
fi
