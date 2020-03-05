#!/usr/bin/env bash
set -e

if [[ -z $1 ]]; then
echo "usage: $0 version profile"
echo "where version is the version tag to assign to the repository item"
echo "      profile is the AWS profile to use for connection to the repository"
exit 1
fi

VERSION=$1
ENVNAME=$2
SCRIPTS=`dirname "$(stat --format=%n "$0")"`

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
  docker tag retail/web-ui:$ENVNAME-$VERSION $REMOTE_DOCKER_REPO/smartorder/web-ui:$ENVNAME-$VERSION
  docker push $REMOTE_DOCKER_REPO/smartorder/web-ui:$ENVNAME-$VERSION
fi
