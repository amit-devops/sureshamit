#!/usr/bin/env bash
set -e

if [[ -z $2 ]]; then
echo "usage: $0 version profile"
echo "where version is the container version"
echo "      profile is the kubectl profile to use"
echo "must be run from the client dist folder"
exit 1
fi

VERSION=$1
ENVNAME=$2
DEST=$(mktemp -d -t docker-XXXXXXXXXX)
SCRIPTS=`dirname "$(stat --format=%n "$0")"`

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

mkdir -p $DEST/nginx
mkdir -p $DEST/www
cp -r * $DEST/www
cp -r $SCRIPTS/../nginx $DEST/
docker build --no-cache -f $DEST/nginx/Dockerfile -t smartorder/web-ui:$ENVNAME-$VERSION $DEST
rm -rf $DEST
