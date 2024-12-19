# Rucio Task Manager

[[_TOC_]]

A modular and extensible framework for performing tasks on a Rucio datalake.

## Quickstart development (On the SKAO prototype datalake using an OIDC access token)

1. Review at least the [architecture](#architecture) section of this documentation!
2. Source the `tools/setup_environment.sh` script from within the `tools` directory:
   ```bash
   ska-src-dm-da-rucio-task-manager/tools$ . setup_environment_for_skao.sh
   ```
3. Run the `run-task` function for the required task, e.g. for the stubs:
   ```bash
   ska-src-dm-da-rucio-task-manager$ run-task etc/tasks/stubs.yml
   ```

## Architecture

Fundamentally, this framework is a task scheduler with Rucio authentication built in. A **task** is defined as any 
operation or sequence of operations that can be performed on the datalake.

Within this framework, a task comprises two parts: _logic_ and _definition_. Task logic is the code; when writing the
logic, consideration should be given to abstract and parameterise sufficiently enough to allow for easy re-use and 
chaining of tasks.

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

Task definitions contain fields to specify the task logic module to be used and any required corresponding arguments. 
They are written in yaml and should be stored in `etc`:

```
  └── etc
      └── tasks
         └── <deployment>
            └── probes
            └── reports
            └── sync
            └── tests
```

The structure of `etc/tasks` takes the following format: `<deployment>/<task_type>/<task_name>.yml` where `deployment` 
is an identifier for the datalake that the task will be deployed to.

For deployments via Helm, task definitions can instead be specified inline in the chart's `values.yaml` file by 
populating `cronjobs.[].task_file_yaml` rather than `cronjobs.[].task_file_path`. This enables changing the task manager 
deployment more easily by CI/CD.

## Usage

This framework is designed to be run in a dockerised environment. 

### Building the image

Images should be built off a preexisting dockerised Rucio client image. This image could be the Rucio base 
provided by the Rucio maintainers (https://github.com/rucio/containers/tree/master/clients), included in the root 
`Makefile` as target "build-base", or an extended image built off this. Extended client images are used to encapsulate 
the prerequisite certificate bundles, VOMS setup (if x509) and Rucio template configs for a specific datalake.

**Extended images already exist for the prototype SKAO datalake as the Makefile target `build-skao`**. Builds for other 
datalake instances can be enabled by adding a new `docker build` routine as a new target in the root `Makefile` with 
the corresponding build arguments for the base client image and tag.

Unless the task manager code is being mounted as a volume inside the container, as shown in the examples below, the 
image will need to be rebuilt whenever a change is made to either the logic or definition, e.g. for the SKAO image:

```bash
make build-skao
```

### Required environment variables

To use the framework, it is first necessary to set a few environment variables. A brief description of each is given 
below:

- **RUCIO_CFG_CLIENT_ACCOUNT**: the Rucio account under which the tasks are to be performed
- **RUCIO_CFG_CLIENT_AUTH_TYPE**: the authentication type (userpass || oidc)
- **TASK_FILE_PATH**: the relative path from the package root to the task file or url

Depending on whether they are already set in the image's baked-in `rucio.cfg`, the following may need to be set:

- **RUCIO_CFG_CLIENT_RUCIO_HOST**: the Rucio server host
- **RUCIO_CFG_CLIENT_AUTH_HOST**: the Rucio auth host

Additionally, there are authentication type dependent variables that must be set.

#### For authentication by username/password

For "userpass" authentication, the following variables are also required:

- **RUCIO_CFG_CLIENT_USERNAME**: username
- **RUCIO_CFG_CLIENT_PASSWORD**: the corresponding password for the user

#### For authentication by OpenID Connect (OIDC)

For "oidc" authentication, it is possible to supply the necessary credentials via four methods:

1. By setting up a volume mount pointing to a valid token,
2. Using a service client than can obtain a token when required via the client_credentials grant, 
3. By passing in an encoded oidc-agent session with included refresh token, or 
4. By passing in a valid access token directly

The order of precedence for which of these routes is selected is as numbered above.

##### By mounting a valid access token

It is possible to volume mount a valid access token to `/tmp/access_token`.

To get an access token, refer to the developer section on [getting an access token](#getting-an-access-token).

##### By using a service client to obtain a token via a client_credentials grant

To obtain a token via a client_credentials grant against a service client, set:

- **OIDC_CLIENT_ID**: The service client id 
- **OIDC_CLIENT_SECRET**: The service client secret
- **OIDC_TOKEN_ENDPOINT**: The IAM token endpoint

This method can be used for asynchronous cronjobs where a token needs to be retrieved at run-time.

##### By passing an oidc-agent configuration

This method requires that the user has a client configuration generated by the 
[https://github.com/indigo-dc/oidc-agent](oidc-agent) tool. This client should have a refresh token attached to it in 
order that access tokens can be generated when required. The encrypted oidc-agent client configuration and password 
are stored in environment variables as plaintext:

- **OIDC_AGENT_AUTH_CLIENT_CFG_VALUE**: an encrypted oidc-agent client with refresh token
- **OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD**: the password to decrypt this client

Depending on whether they are already set in the image's baked-in `rucio.cfg`, the following may also need to be set:

- **RUCIO_CFG_CLIENT_OIDC_SCOPE**: list of OIDC scopes
- **RUCIO_CFG_CLIENT_OIDC_AUDIENCE**: list of OIDC audiences

This method can be used for asynchronous cronjobs where a token needs to be retrieved at run-time.

##### By passing a valid access token directly

For this method, set:

**OIDC_ACCESS_TOKEN**: the access token

To get an access token, refer to the developer section on [getting an access token](#getting-an-access-token).

### Examples

In all the examples below, it is also possible to override other `RUCIO_*` environment variables. If they are not 
explicitly supplied, they will be taken from the `rucio.cfg`.

#### userpass

```bash
docker run --rm -it \
-e RUCIO_CFG_CLIENT_AUTH_TYPE=userpass \
-e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
-e RUCIO_CFG_CLIENT_USERNAME=$RUCIO_CFG_CLIENT_USERNAME \
-e RUCIO_CFG_CLIENT_PASSWORD=$RUCIO_CFG_CLIENT_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

For development purposes, it can be helpful to mount the package from the host directly into the container provided you 
have exported the project's root directory path as `RUCIO_TASK_MANAGER_ROOT`, e.g.:

```bash
eng@ubuntu:~/rucio-task-manager$ docker run --rm -it \
-e RUCIO_CFG_CLIENT_AUTH_TYPE=userpass \
-e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
-e RUCIO_CFG_CLIENT_USERNAME=$RUCIO_CFG_CLIENT_USERNAME \
-e RUCIO_CFG_CLIENT_PASSWORD=$RUCIO_CFG_CLIENT_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

With this, it is not required to rebuild the image everytime it is run.

#### oidc

##### By using a service client to obtain a token via a client_credentials grant

```bash
docker run --rm -it \
-e RUCIO_CFG_CLIENT_AUTH_TYPE=oidc \
-e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
-e OIDC_CLIENT_ID=<client_id> \
-e OIDC_CLIENT_SECRET=<client_secret> \
-e OIDC_TOKEN_ENDPOINT=<iam_token_endpoint> \
-e RUCIO_CFG_CLIENT_OIDC_SCOPE="openid profile offline_access rucio" \
-e RUCIO_CFG_CLIENT_OIDC_AUDIENCE="rucio https://wlcg.cern.ch/jwt/v1/any" \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

##### By passing in an oidc-agent client configuration

```bash
docker run --rm -it \
-e RUCIO_CFG_CLIENT_AUTH_TYPE=oidc \
-e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
-e OIDC_AGENT_AUTH_CLIENT_CFG_VALUE="`cat ~/.oidc-agent/<client_name>`" \
-e OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD=$OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

##### By passing in a valid access token

```bash
docker run --rm -it \
-e RUCIO_CFG_CLIENT_AUTH_TYPE=oidc \
-e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
-e OIDC_ACCESS_TOKEN="$OIDC_ACCESS_TOKEN" \
-e TASK_FILE_PATH=etc/tasks/stubs.yml \
-v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
--name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
```

## Deployment

### On Kubernetes

Deployment in a kubernetes cluster is managed by Helm. 

A rucio-task-manager image must first be built, tagged and pushed to a location accessible to the cluster, e.g. for 
SKAO's harbor:

```bash
make skao
docker tag rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG` registry.gitlab.com/ska-telescope/src/src-dm/ska-src-dm-da-rucio-task-manager:latest
docker push registry.gitlab.com/ska-telescope/src/src-dm/ska-src-dm-da-rucio-task-manager:latest
```

As per the standard procedure for Helm, the task values in `etc/helm/values.yaml` can be adjusted as required. 

Variables to be directly assigned as environment variables to the container can be specified in the `config` section, 
e.g.

```yaml
config:
  RUCIO_CFG_CLIENT_RUCIO_HOST: https://rucio.srcnet.skao.int
  RUCIO_CFG_CLIENT_AUTH_HOST: https://rucio-auth.srcnet.skao.int
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

## Development

### Getting started (OIDC)

#### Getting an access token

##### Using the Rucio Client

To develop tasks that can be tested by running against an existing datalake, we must first retrieve a valid access 
token for the datalake. In this example, we will use OIDC (and assume our datalake instance supports this method of 
authentication).

This token can be retrieved by running a Rucio client container and authenticating against your account, 
`<account>`, with OIDC, e.g. using the SKAO Rucio prototype client:

```bash
$ export RUCIO_CFG_CLIENT_ACCOUNT=<account>
$ docker run -it --rm -e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT -e RUCIO_CFG_CLIENT_AUTH_TYPE=oidc registry.gitlab.com/ska-telescope/src/ska-rucio-client:release-35.6.0
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

##### Using cURL

To retrieve a token using the authorization code flow, we need to give Rucio an authorisation code from IAM. 
To generate the URL to begin the process we first curl the `/oidc` endpoint, which will return a populated URL pointing  
to the `/redirect` endpoint. In this example, we shall be authenticating against the SKAO Rucio prototype 
at `https://rucio-auth.srcnet.skao.int`:

```bash
$ curl -D - https://rucio-auth.srcnet.skao.int/auth/oidc | grep X-Rucio-OIDC-Auth-URL | awk -F ': ' '{ print $2 }'
https://rucio-auth.srcnet.skao.int/auth/oidc_redirect?MHsWssd...
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
$ curl -D - https://rucio-auth.srcnet.skao.int/auth/oidc_redirect?<code> -H "X-Rucio-Client-Fetch-Token: True" | grep X-Rucio-Auth-Token | awk -F ": " '{ print $2 }'
eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJ3bGNnLnZlciI6IjEuMCIsInN1Y...
```

### Creating a new task

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
4. To run the new test locally, build and run the container image as described in more detail above:
    ```
    $ make build-skao
    $ docker run --rm -it \
      -e RUCIO_CFG_CLIENT_AUTH_TYPE=oidc \
      -e RUCIO_CFG_CLIENT_ACCOUNT=$RUCIO_CFG_CLIENT_ACCOUNT \
      -e OIDC_ACCESS_TOKEN="$OIDC_ACCESS_TOKEN" \
      -e TASK_FILE_PATH=etc/tasks/<your_test_file>.yml \
      -v $RUCIO_TASK_MANAGER_ROOT:/opt/rucio-task-manager \
      --name=rucio-task-manager rucio-task-manager:`cat BASE_RUCIO_CLIENT_TAG`
    ```
