#!/usr/bin/env bash
set -e

if [[ -z "$3" ]]; then
echo usage: "$0 version profile source_dir"
echo "where version is the container version"
echo "      profile is the kubectl profile to use"
echo "      source_dir is the location of the flask application source"
exit 1
fi

VERSION=$1
ENVNAME=$2
SOURCE=$3
DEST=$(mktemp -d -t docker-XXXXXXXXXX)
SCRIPTS=`dirname "$(stat --format=%n "$0")"`

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

mkdir -p $DEST
cp -r $SOURCE/* $DEST
cd $DEST
docker build --no-cache -t smartorder/server:$ENVNAME-$VERSION .
rm -rf $DEST
