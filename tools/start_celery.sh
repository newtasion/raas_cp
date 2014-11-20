#!/bin/bash

# run this to start in background mode
# screen -dmS celery ./start_celery.sh

# test rabbitmq server
echo "Testing to see if rabbitmq server is up"
nc -z localhost 5672
if [[ $? != '0' ]]
then
  echo "Starting rabbitmq in the background using 'screen'"
  screen -dmS rabbitmq-server rabbitmq-server
fi
project_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
echo "Starting celery in the background using 'screen'"
screen -dmS celery env PYTHONPATH=$project_root:$project_root/server:$project_root/server/raas celery --app=raas worker -l info
screen -ls
