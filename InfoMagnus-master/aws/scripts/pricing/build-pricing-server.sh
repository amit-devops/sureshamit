#!/usr/bin/env bash
set -e

if [[ -z "$2" ]]; then
echo usage: "$0 version profile"
echo "where version is the container version"
echo "      profile is the kubectl profile to use"
exit 1
fi

VERSION=$1
ENVNAME=$2
DEST=$(mktemp -d -t docker-XXXXXXXXXX)
if [[ -z "$SCRIPTS" ]]; then
  SCRIPTS=`dirname "$(stat -f "$0")"`
fi

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

if [[ -z $SOURCE ]]; then
  echo "SOURCE environment variable is not set"
  echo "SOURCE should point to the server application source folder"
  exit 1
fi

mkdir -p $DEST
cp -r $SOURCE/* $DEST
cd $DEST
docker build --no-cache -t pricingportal/server:$ENVNAME-$VERSION .
rm -rf $DEST
