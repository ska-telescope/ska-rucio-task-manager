#!/bin/bash

# Setup envvars
RUCIO_TASK_MANAGER_ROOT=`python3 -c "import os,sys; print(os.path.realpath(sys.argv[1]))" ../../`
read -e -p "Path to Rucio task manager root (default: $RUCIO_TASK_MANAGER_ROOT): " input
RUCIO_TASK_MANAGER_ROOT=${input:-$RUCIO_TASK_MANAGER_ROOT} 
export RUCIO_TASK_MANAGER_ROOT=$RUCIO_TASK_MANAGER_ROOT

echo "RUCIO_TASK_MANAGER_ROOT set to \"$RUCIO_TASK_MANAGER_ROOT\""

export RUCIO_CFG_AUTH_TYPE=oidc

read -e -p "Rucio account name (default: $RUCIO_CFG_ACCOUNT): " input
RUCIO_CFG_ACCOUNT=${input:-$RUCIO_CFG_ACCOUNT}
export RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT

echo "RUCIO_CFG_ACCOUNT set to \"$RUCIO_CFG_ACCOUNT\""

# Get a token
docker run -itd --name ska-rucio-client --rm -e PYTHONWARNINGS="ignore:Unverified HTTPS request" -e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT -e RUCIO_CFG_AUTH_TYPE=oidc registry.gitlab.com/ska-telescope/src/ska-rucio-client:release-1.29.0

docker exec -it ska-rucio-client rucio whoami && export BEARER_TOKEN=`docker exec -it ska-rucio-client cat /tmp/user/.rucio_user/auth_token_for_account_$RUCIO_CFG_ACCOUNT`
docker stop ska-rucio-client

export OIDC_ACCESS_TOKEN=$BEARER_TOKEN

# Build the task manager image
(cd $RUCIO_TASK_MANAGER_ROOT; make skao)

# Define a quick entrypoint
function run-task () {
  if [ "$#" -ne 1 ]; then
    echo "You must pass the script location (relative to $RUCIO_TASK_MANAGER_ROOT) as an argument to this script."
    return 1
  fi

  echo "Running task at $1"

  docker run --rm -it \
  -e RUCIO_CFG_AUTH_TYPE=$RUCIO_CFG_AUTH_TYPE \
  -e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
  -e OIDC_ACCESS_TOKEN="$OIDC_ACCESS_TOKEN" \
  -e TASK_FILE_PATH=$1 \
  -v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
  --name=rucio-task-manager rucio-task-manager:`cat $RUCIO_TASK_MANAGER_ROOT/BASE_RUCIO_CLIENT_TAG`
}

