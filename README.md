# rucio-task-manager

A modular and extensible framework for performing tasks on a Rucio datalake.

## Quickstart development (On the SKAO prototype datalake using OIDC)

1. Review the [architecture](#architecture) section
2. Build the image: `make skao`
3. [Get a datalake access token](#using-the-rucio-client)
4. Export the required environment variables for OIDC authentication using an access token: 
   - `RUCIO_CFG_ACCOUNT=<account>`, 
   - `RUCIO_CFG_AUTH_TYPE=oidc`,
   - `RUCIO_TASK_MANAGER_ROOT=/path/to/task/manager/root`,
   - `OIDC_ACCESS_TOKEN=<token>`
5. [Run it](#by-passing-in-an-access-token)

# Architecture

Fundamentally, this framework is a task scheduler with Rucio authentication built in. A **task** is defined as any 
operation or sequence of operations that can be performed on the datalake.

Within this framework, a task comprises two parts: _logic_ and _definition_. Task logic is code and should be sufficiently 
abstracted & parameterised to allow for easy re-use and chaining of tasks.

The source for task logic is kept in `src`:

```
  └── src
      |── common
      │  └── rucio
      └── tasks
         └── probes
         └── reports
         └── sync
         └── tests
```

where functionality that is expected to be common across modules is stored in `src/common`. The structure of 
`src/tasks` takes the following format: `<task_type>/<task_name>.yml` where, for consistency, `<task_type>` should be 
one of:

- probes (cluster health, uptime etc.)
- reports (slack integrations, emails etc.)
- sync (syncing functionality, e.g. IAM)
- tests

But other categories may be added as needed.

Task definitions are written in yaml and can be stored in `etc`:

```
  └── etc
      ├── helm
      ├── docker
      └── tasks
         └── <deployment>
            └── probes
            └── reports
            └── sync
            └── tests
```

Each definition contains fields to specify the task logic module to be used and any required corresponding arguments. 
The structure of `etc/tasks` takes the following format: `<deployment>/<task_type>/<task_name>.yml` where `deployment` 
is a identifier for the datalake that the task will be deployed to.

A Helm chart for deployment to a kubernetes cluster is kept in `etc/helm`. **For deployments via Helm, task definitions 
must be specified separately in the `values` file.**

# Usage

This framework is designed to be run in a dockerised environment. 

## Building the image

Images should be built off a preexisting dockerised Rucio client image. This image could be the Rucio base 
provided by the Rucio maintainers (https://github.com/rucio/containers/tree/master/clients), included in the root 
`Makefile` as target "rucio", or an extended image built off this. Extended client images are used to encapsulate the 
prerequisite certificate bundles, VOMS setup (if x509) and Rucio template configs for a specific datalake.

**Extended images already exist for the prototype skao datalake as the Makefile target `skao`**. Builds for other 
datalake instances can be enabled by adding a new `docker build` routine as a new target in the root `Makefile` with 
the corresponding build arguments for the base client image and tag.

Unless the task manager code is being mounted as a volume inside the container, as shown in the examples below, the 
image will need to be rebuilt when a change is made to either the logic or definition, e.g. for skao images:

```bash
eng@ubuntu:~/rucio-task-manager$ make skao
```

## Required environment variables

To use the framework, it is first necessary to set a few environment variables. A brief description of each is given 
below:

- **RUCIO_CFG_ACCOUNT**: the Rucio account under which the tasks are to be performed
- **RUCIO_CFG_AUTH_TYPE**: the authentication type (userpass || x509 || oidc)
- **TASK_FILE_PATH**: the relative path from the package root to the task file or url

Depending on whether they are already set in the image's baked-in `rucio.cfg`, the following may need to be set:

- **RUCIO_CFG_RUCIO_HOST**: the Rucio server host
- **RUCIO_CFG_AUTH_HOST**: the Rucio auth host

Additionally, there are authentication type dependent variables that must be set.

### For authentication by username/password

For "userpass" authentication, the following variables are also required:

- **RUCIO_CFG_USERNAME**: username
- **RUCIO_CFG_PASSWORD**: the corresponding password for the user

### For authentication by X.509

For "x509" authentication, it is possible to supply the necessary credentials via two methods.

If the key/certificate values are stored in environment variables as plaintext, e.g. coming from a k8s secret, then:

- **RUCIO_CFG_CLIENT_CERT_VALUE**: a valid X.509 certificate
- **RUCIO_CFG_CLIENT_KEY_VALUE**: a valid X.509 key

Alternatively, the paths to the key/certificate can be held in the following variables:

- **RUCIO_CFG_CLIENT_CERT**: path to a valid X.509 certificate 
- **RUCIO_CFG_CLIENT_KEY**: path to a valid X.509 key
- **VOMS**: the virtual organisation that the user belongs to

but the key/certificate **must be volume mounted to these locations**.

### For authentication by OpenID Connect (OIDC)

For "oidc" authentication, it is possible to supply the necessary credentials via two methods.

#### Using an access token

The first (and easiest) method assumes that the user already has a valid access token:

- **OIDC_ACCESS_TOKEN**: an encrypted oidc-agent client with refresh token

To get an access token, refer to `Development > Getting started (OIDC) > Getting an access token`.

This method is advised for general development use.

#### Using an oidc-agent configuration

The second method requires that the user has a client configuration generated by the 
[https://github.com/indigo-dc/oidc-agent](oidc-agent) tool. This client should have a refresh token attached to it in 
order that access tokens can be generated when required. The encrypted oidc-agent client configuration and password 
are stored in environment variables as plaintext:

- **OIDC_AGENT_AUTH_CLIENT_CFG_VALUE**: an encrypted oidc-agent client with refresh token
- **OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD**: the password to decrypt this client

Depending on whether they are already set in the image's baked-in `rucio.cfg`, the following may also need to be set:

- **RUCIO_CFG_OIDC_SCOPE**: list of OIDC scopes
- **RUCIO_CFG_OIDC_AUDIENCE**: list of OIDC audiences

This method is useful for asynchronous cronjobs where a token needs to be retrieved at run-time.

## Examples

In all the examples below, it is also possible to override other `RUCIO_*` environment variables. If they are not 
explicitly supplied, they will be taken from the `rucio.cfg`.

### userpass

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=userpass \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_USERNAME=$RUCIO_CFG_USERNAME \
-e RUCIO_CFG_PASSWORD=$RUCIO_CFG_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

For development purposes, it is possible to mount the package from the host directly into the container provided you 
have exported the project's root directory path as `RUCIO_TASK_MANAGER_ROOT`, e.g.:

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=userpass \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_USERNAME=$RUCIO_CFG_USERNAME \
-e RUCIO_CFG_PASSWORD=$RUCIO_CFG_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

With this, it is not required to rebuild the image everytime it is run.

### x509

#### By passing in key/certificate values as plaintext

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=x509 \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_CLIENT_CERT_VALUE="`cat $RUCIO_CFG_CLIENT_CERT`" \
-e RUCIO_CFG_CLIENT_KEY_VALUE="`cat $RUCIO_CFG_CLIENT_KEY`" \
-e VOMS=skatelescope.eu \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

#### By passing in key/certificate paths

For X.509 authentication with Rucio via paths you must bind the certificate credentials to a volume inside the 
container, e.g.:

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=x509 \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e RUCIO_CFG_CLIENT_CERT=/opt/rucio/etc/client.crt \
-e RUCIO_CFG_CLIENT_KEY=/opt/rucio/etc/client.key \
-e VOMS=skatelescope.eu \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_CFG_CLIENT_CERT:/opt/rucio/etc/client.crt \
-v $RUCIO_CFG_CLIENT_KEY:/opt/rucio/etc/client.key \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

### oidc

#### By passing in an access token

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=oidc \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e OIDC_ACCESS_TOKEN="$OIDC_ACCESS_TOKEN" \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

#### By passing in an oidc-agent client configuration

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_AUTH_TYPE=oidc \
-e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT \
-e OIDC_AGENT_AUTH_CLIENT_CFG_VALUE="`cat ~/.oidc-agent/<client_name>`" \
-e OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD=$OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

# Deployment

## On Kubernetes

Deployment in a kubernetes cluster is managed by Helm. 

A rucio-task-manager image must first be built, tagged and pushed to a location accessible to the cluster, e.g. for 
SKAO's gitlab:

```bash
eng@ubuntu:~/rucio-task-manager$ make skao
eng@ubuntu:~/rucio-task-manager$ docker tag rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG` registry.gitlab.com/ska-telescope/src/ska-rucio-prototype/rucio-task-manager-client:latest
eng@ubuntu:~/rucio-task-manager$ docker push registry.gitlab.com/ska-telescope/src/ska-rucio-prototype/rucio-task-manager-client:latest
```

As per the standard procedure for Helm, the task values in `etc/helm/values.yaml` can be adjusted as required. 

Variables to be directly assigned as environment variables to the container can be specified in the `config` section, 
e.g.

```yaml
config:
  RUCIO_CFG_RUCIO_HOST: https://srcdev.skatelescope.org/rucio-dev
  RUCIO_CFG_AUTH_HOST: https://srcdev.skatelescope.org/rucio-dev
```

Secrets such as certificates and keys that are created on the cluster, e.g. 

```bash
$ kubectl create secret generic oidc-agent-auth-client --from-file=cfg=/path/to/file --from-literal=password=<password>
```

can be specified in the `secrets` section:

```yaml
secrets:
  - name: OIDC_AGENT_AUTH_CLIENT_CFG_VALUE
    fromSecretName: oidc-agent-auth-client
    fromSecretKey: cfg
  - name: OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD
    fromSecretName: oidc-agent-auth-client
    fromSecretKey: password
```

to be assigned as environment variables in the container.

Cronjobs for tasks can be scheduled by adding a new entry to the `cronjobs` section, e.g.

```yaml
cronjobs:
  - name: <task_name>
    minute: "0"
    hour: "*"
    day: "*"
    month: "*"
    weekday: "*"
    task_file_path: "path/to/test"
    disabled: no
```

Task files can either be specified as a path, `task_file_path`, or inline as yaml under `task_file_yaml`. If both are 
specified, then the inline yaml takes preference.

It is possible to substitute secrets into tasks using j2 templating syntax (`{{ variable }}`), e.g.

```bash
$ kubectl create secret generic task-stubs --from-literal=text=HelloWorld
```

```yaml
secrets:
  - name: TASK_STUBS_TEXT
    fromSecretName: task-stubs
    fromSecretKey: text
    
cronjobs:
  - name: stubs
    minute: "*/15"
    hour: "*"
    day: "*"
    month: "*"
    weekday: "*"
    task_file_yaml: 
      test-hello-world-stub:
        description: Test hello world stub
        module_name: tasks.stubs
        class_name: StubHelloWorld
        enabled: true
        args:
        kwargs:
          text: {{ TASK_STUBS_TEXT }}
    disabled: yes
```

# Development

## Getting started (OIDC)

### Getting an access token

#### Using the Rucio Client

To develop tasks that can be tested by running against an existing datalake, we must first retrieve a valid access 
token for the datalake. In this example, we will use OIDC (and assume our datalake instance supports this method of 
authentication).

This token can be retrieved by running a Rucio client container and authenticating against your account, 
`<account>`, with OIDC, e.g. using the SKAO Rucio prototype client:

```bash
$ export RUCIO_CFG_ACCOUNT=<account>
$ docker run -it --rm -e RUCIO_CFG_ACCOUNT=$RUCIO_CFG_ACCOUNT -e RUCIO_CFG_AUTH_TYPE=oidc registry.gitlab.com/ska-telescope/src/ska-rucio-client:release-1.29.0
$ rucio whoami
```

at which point you will be asked to follow the OIDC authorisation code flow. Once complete, a token will be generated 
in `/tmp/user/.rucio_user/auth_token_for_account_<account>`:

```bash
$ cat /tmp/user/.rucio_user/auth_token_for_account_<account> 
eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJ3bGNnLnZlciI6IjEuMCIsInN1Y...
```

Copy this token and export it to an environment variable, `OIDC_ACCESS_TOKEN`, in whatever shell you intend to run the 
manager in. 

#### Using cURL

To retrieve a token using the authorization code flow, we need to give Rucio an authorisation code from IAM. 
To generate the URL to begin the process we first curl the `/oidc` endpoint, which will return a populated URL pointing  
to the `/redirect` endpoint. In this example, we shall be authenticating against the SKAO Rucio prototype 
at `https://srcdev.skatelescope.org/rucio-dev/`:

```bash
$ curl -D - https://srcdev.skatelescope.org/rucio-dev/auth/oidc | grep X-Rucio-OIDC-Auth-URL | awk -F ': ' '{ print $2 }'
https://srcdev.skatelescope.org/rucio-dev/auth/oidc_redirect?MHsWssd...
```

where the URL is retrieved from the response header, `X-Rucio-OIDC-Auth-URL`. Following this link will take you 
to the corresponding auth client's IAM, where you will be asked to log in (and potentially allow the client to access 
your details if this is the first time).

Once logged in, you will be redirected to Rucio's `/oidc_code` endpoint with the authorisation code and state encoded 
as a query string in the URL. This code and state is automatically processed by Rucio to retrieve and store an access 
token for you in the database (temporarily). Take note of the code presented to you on the web page - it is the code 
that will be used to verify your request when retrieving your token in the next step. 

Finally, we submit another request to the `/oidc_redirect` endpoint with the code retrieved from the previous step as 
the query string and with `X-Rucio-Client-Fetch-Token` set to true as a request header:

```bash
$ curl -D - https://srcdev.skatelescope.org/rucio-dev/auth/oidc_redirect?<code> -H "X-Rucio-Client-Fetch-Token: True" | grep X-Rucio-Auth-Token | awk -F ": " '{ print $2 }'
eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJ3bGNnLnZlciI6IjEuMCIsInN1Y...
```

## Creating a new task

The procedure for creating a new tests is as follows:

1. Take a copy of the `TestStubHelloWorld` class stub in `src/tasks/stubs.py` and rename both the file and class name.
2. Amend the entrypoint `run()` function as desired. Functionality for communicating with Rucio is done by using the 
   client functions directly (see the 
   [Rucio Client API](https://rucio.github.io/documentation/client_api/accountclient) for details). Example usage can 
   be found in the `StubRucioAPI` class stub in `src/tasks/stubs.py`.
3. Create a new task definition file e.g. `etc/tasks/test.yml` copying the format of the `test-hello-world-stub` 
   definition in `etc/tasks/stubs.yml`. A task has the following mandatory fields:
   - `module_name` (starting from and including the `tasks.` prefix) and `class_name`, set accordingly to match the 
     modules/classes redefined in step 1,
   - `args` and `kwargs` keys corresponding to the parameters injected into the task's entry point `run()`,
   - `description`, and
   - `enabled`.
