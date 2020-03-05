#!/bin/sh
if [ -f /etc/config/env.sh ]; then
  . /etc/config/env.sh
fi

# Checks the APP_MODE and sets the correct celery queue
if [ "$APP_MODE" = "CONTROLLER" ]; then
  celery_queue="default"
elif [ $APP_MODE = "DENORMALIZER" ]; then
  celery_queue="feeder"
elif [ $APP_MODE = "PREDICTION" ]; then
  celery_queue="prediction"
else
  echo "Unknown APP_MODE of $APP_MODE found"
  exit 1
fi

# Creates Celery Worker
celery -A app.celery_worker.celery worker -Q "$celery_queue" --detach -f celery.log -l INFO -n $APP_MODE@%h

# Starts Flask App
uwsgi --ini /etc/uwsgi.ini:$APP_MODE --chdir /var/www/dsdp

