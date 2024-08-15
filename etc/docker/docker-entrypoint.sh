#!/bin/bash

export PYTHONWARNINGS='ignore:Unverified HTTPS request'

# Generate Rucio configuration file from template
echo "initialising Rucio"
/etc/profile.d/rucio_init.sh
echo

# Pass through kube config if set
if [ -v KUBE_CONFIG_VALUE ]
then
  echo "$KUBE_CONFIG_VALUE" > ~/.kube/config
fi

# Set up authentication
if [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'userpass' ]
then
  if [ -v RUCIO_CFG_USERNAME ] && [ -v RUCIO_CFG_PASSWORD ]
  then
    echo "proceeding with userpass authentication..."
  else
    echo "requested userpass auth but one or more of \$RUCIO_CFG_USERNAME or \$RUCIO_CFG_PASSWORD are not set"
    echo "quitting"
    exit
  fi
elif [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'x509' ]
then
  if [ -v RUCIO_CFG_CLIENT_CERT_VALUE ] && [ -v RUCIO_CFG_CLIENT_KEY_VALUE ] && [ -v VOMS ] # if certificate/key are being passed in as values (e.g. from a k8s secret)
  then
    echo "proceeding with X509 authentication via passed key/certificate values..."
    # copy in credentials
    echo "$RUCIO_CFG_CLIENT_CERT_VALUE" > "/opt/rucio/etc/usercert.pem"
    echo "$RUCIO_CFG_CLIENT_KEY_VALUE" > "/opt/rucio/etc/userkey.pem"
    chmod 600 "/opt/rucio/etc/usercert.pem"
    chmod 400 "/opt/rucio/etc/userkey.pem"
    # export Rucio X509 client credentials
    export RUCIO_CFG_CLIENT_CERT="/opt/rucio/etc/usercert.pem"
    export RUCIO_CFG_CLIENT_KEY="/opt/rucio/etc/userkey.pem"
  elif [ -v RUCIO_CFG_CLIENT_CERT ] && [ -v RUCIO_CFG_CLIENT_KEY ] && [ -v VOMS ] # if certificate/key are being passed in as paths (e.g. from volume binds)
  then
    echo "proceeding with X509 authentication via passed key/certificate paths..."
    echo "! make sure that that you have volume bound your certificate/key at the locations specified by RUCIO_CFG_CLIENT_CERT and RUCIO_CFG_CLIENT_KEY respectively !"
    echo
  else
    echo "requested X509 auth but one or more of \$RUCIO_CFG_CLIENT_CERT_*, \$RUCIO_CFG_CLIENT_KEY_* or \$VOMS are not set"
    exit
  fi
  # set X509 credentials for FTS client
  export X509_USER_KEY=$RUCIO_CFG_CLIENT_KEY
  export X509_USER_CERT=$RUCIO_CFG_CLIENT_CERT
  # create X509 user proxy
  voms-proxy-init --cert "$RUCIO_CFG_CLIENT_CERT" --key "$RUCIO_CFG_CLIENT_KEY" --voms "$VOMS"
elif [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'oidc' ]
then
  if [ -f "/tmp/access_token" ]                                                                                           # if access token token has been volume mounted
  then
    echo "proceeding with oidc authentication using existing mounted token..."
    cp "/tmp/access_token" "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  elif [ -v OIDC_CLIENT_ID ] && [ -v OIDC_CLIENT_SECRET ] && [ -v OIDC_TOKEN_ENDPOINT ] && [ -v RUCIO_CFG_OIDC_SCOPE ] && \
    [ -v RUCIO_CFG_OIDC_AUDIENCE ] && [ -v RUCIO_CFG_ACCOUNT ]                                                            # if IAM client's credentials have been passed in (i.e. using a service client w/ client_credentials flow)
  then
    echo "proceeding with oidc authentication via passed client credentials..."
    curl -s -u "$OIDC_CLIENT_ID:$OIDC_CLIENT_SECRET" -d grant_type=client_credentials -d scope="$RUCIO_CFG_OIDC_SCOPE" \
      -d audience="$RUCIO_CFG_OIDC_AUDIENCE" $OIDC_TOKEN_ENDPOINT \
      | jq -j '.access_token' > "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  elif [ -v OIDC_AGENT_AUTH_CLIENT_CFG_VALUE ] && [ -v OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD ] && [ -v RUCIO_CFG_ACCOUNT ]  # if oidc-agent config is being passed in as a value (e.g. from a k8s secret)
  then
    echo "proceeding with oidc authentication via passed oidc-agent values..."
    # initialise oidc-agent
    # n.b. this assumes that the configuration has a refresh token attached to it with infinite lifetime
    eval "$(oidc-agent-service use)"
    mkdir -p ~/.oidc-agent
    # copy across the auth client configuration (-e to interpolate newline characters)
    echo -e "$OIDC_AGENT_AUTH_CLIENT_CFG_VALUE" > ~/.oidc-agent/rucio-auth
    # add configuration to oidc-agent
    oidc-add --pw-env=OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD rucio-auth
    # get client name (can be different to short name used by oidc-agent)
    export OIDC_CLIENT_NAME=$(oidc-gen --pw-env=OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD -p rucio-auth | jq -r .name)
    # retrieve token from oidc-agent; place in file for Rucio client and set env BEARER_TOKEN (for Gfal2 client)
    export BEARER_TOKEN=`oidc-token --scope "$RUCIO_CFG_OIDC_SCOPE" --aud "$RUCIO_CFG_OIDC_AUDIENCE" $OIDC_CLIENT_NAME`
    echo $BEARER_TOKEN > "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  elif [ -v OIDC_ACCESS_TOKEN ] && [ -v RUCIO_CFG_ACCOUNT ]                                                               # if access token is being passed in directly
  then
    echo "proceeding with oidc authentication using an access token..."
    echo "$OIDC_ACCESS_TOKEN" > "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
    # Gfal client expects BEARER_TOKEN env var
    export BEARER_TOKEN=$OIDC_ACCESS_TOKEN
  else
    echo "requested oidc auth but couldn't find the necessary environment variables to instigate"
    env
    exit 1
  fi
  tr -d '\n' < "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT" > "/tmp/auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  # move this token to the location expected by Rucio
  mkdir -p /tmp/user/.rucio_user/
  mv "/tmp/auth_token_for_account_$RUCIO_CFG_ACCOUNT" /tmp/user/.rucio_user/
fi

echo
rucio whoami
echo

# if task has came in as yaml, do template substitution & pipe the result to a file
if [ -v TASK_FILE_YAML ]
then
  echo "$TASK_FILE_YAML" > /tmp/task.yaml
  j2 /tmp/task.yaml > /tmp/task.yaml.j2
  export TASK_FILE_PATH=/tmp/task.yaml.j2
fi

export PYTHONIOENCODING=utf8

python3 src/run.py -vt "$TASK_FILE_PATH"
