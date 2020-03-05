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
if [[ -z "$SCRIPTS" ]]; then
  SCRIPTS=`dirname "$(stat -f "$0")"`
fi

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

mkdir -p $DEST/nginx
mkdir -p $DEST/www
cp -r * $DEST/www
cp -r $SCRIPTS/../nginx $DEST/
docker build --no-cache -f $DEST/nginx/Dockerfile -t pricingportal/web-ui:$ENVNAME-$VERSION $DEST
rm -rf $DEST
