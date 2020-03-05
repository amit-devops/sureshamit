#!/bin/bash
set -e
if [[ -z "$1" ]]; then
echo usage: "$0 profile"
exit 1
fi

ENVNAME=$1
KEYFILE=$(mktemp -t keyfile-XXXXXXXXXX)

if [[ ! -z "$SECRETS_PASSWORD" ]]; then
  echo $SECRETS_PASSWORD > $KEYFILE
else
  rm $KEYFILE
fi
if [[ -f $KEYFILE ]]; then
  mcrypt -d -f $KEYFILE ~/${ENVNAME}.secrets.nc
else
  mcrypt -d ~/${ENVNAME}.secrets.nc
fi
if [[ ! $? -eq 0 ]]; then
  exit 1
fi
for d in $(cat ~/${ENVNAME}.secrets); do
  export $d
done
if [[ -f ~/${ENVNAME}.secrets.nc ]]; then
  rm -f ~/${ENVNAME}.secrets
fi
if [[ -f $KEYFILE ]]; then
  rm $KEYFILE
fi
