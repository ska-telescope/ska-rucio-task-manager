#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "You must pass the script location (relative to $RUCIO_TASK_MANAGER_ROOT) as an argument to this script."
  exit
fi

echo "Running task at $1"

docker run --rm -it \
	-e RUCIO_CFG_AUTH_TYPE=$RUCIO_CFG_AUTH_TYPE \
	-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
	-e OIDC_ACCESS_TOKEN="$OIDC_ACCESS_TOKEN" \
	-e TASK_FILE_PATH=$1 \
        -v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
        --name=rucio-task-manager rucio-task-manager:`cat $RUCIO_TASK_MANAGER_ROOT/BASE_RUCIO_CLIENT_TAG`

