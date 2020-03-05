#!/bin/bash
set -e
if [[ -z "$3" ]]; then
echo usage: "$0 hostname profile source_dir"
exit 1
fi

HOSTNAME=$1
ENVNAME=$2
SOURCE=$3
DEST=$(mktemp -d -t config-XXXXXXXXXX)
CONFIG=/etc/config/
SCRIPTS=`dirname "$(stat --format=%n "$0")"`

source $SCRIPTS/access-secrets.sh $ENVNAME
if [[ ! $? -eq 0 ]]; then
  echo Unable to access $ENVNAME.secrets file!
  exit 1
fi

# process contents of secrets file to perform substitution
# of environment variables.
touch $DEST/secrets.properties
while read -r LINE; do
    eval "echo \"$LINE\"" >> $DEST/secrets.properties
done < $SOURCE/config/deployed/secrets.properties
chmod 660 $DEST/secrets.properties
cp $SOURCE/config/deployed/app.properties $DEST/app.properties
cp $SOURCE/uwsgi.ini $DEST

cat > $DEST/env.sh <<INPUT
#!/usr/bin/env sh
export DEPLOYMENT=$DEPLOYMENT
export HOSTNAME=$HOSTNAME
export CONFIG_PATH=$CONFIG
export FLASK_CONFIG=$DEPLOYMENT
INPUT

chmod 770 $DEST/env.sh

kubectl config use-context $ENVNAME
kubectl delete configmap smartorder-config
kubectl create configmap smartorder-config \
        --from-file=$DEST/app.properties \
        --from-file=$DEST/env.sh \
        --from-file=$DEST/uwsgi.ini
kubectl delete secret smartorder-secrets
kubectl create secret generic smartorder-secrets \
        --from-file=$DEST/secrets.properties \

rm -rf $DEST
