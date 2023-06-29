#!/bin/bash

export RUCIO_TASK_MANAGER_ROOT=`python -c "import os,sys; print(os.path.realpath(sys.argv[1]))" ../../`
read -e -p "Path to Rucio task manager root: " -i "$RUCIO_TASK_MANAGER_ROOT" RUCIO_TASK_MANAGER_ROOT

echo "RUCIO_TASK_MANAGER_ROOT set to \"$RUCIO_TASK_MANAGER_ROOT"

export RUCIO_CFG_AUTH_TYPE=oidc

if [ -v RUCIO_CFG_ACCOUNT ]; then
  echo "RUCIO_CFG_ACCOUNT already set to \"$RUCIO_CFG_ACCOUNT\""
else 
  read -p "Enter Rucio account name: " account
  export RUCIO_CFG_ACCOUNT=$account
  echo "RUCIO_CFG_ACCOUNT set to \"$RUCIO_CFG_ACCOUNT\""
fi

docker run -itd --name ska-rucio-client --rm -e PYTHONWARNINGS="ignore:Unverified HTTPS request" -e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT -e RUCIO_CFG_AUTH_TYPE=oidc registry.gitlab.com/ska-telescope/src/ska-rucio-client:release-1.29.0

docker exec -it ska-rucio-client rucio whoami && export BEARER_TOKEN=`docker exec -it ska-rucio-client cat /tmp/user/.rucio_user/auth_token_for_account_$RUCIO_CFG_ACCOUNT`

docker stop ska-rucio-client

export OIDC_ACCESS_TOKEN=$BEARER_TOKEN

(cd $RUCIO_TASK_MANAGER_ROOT; make skao)

