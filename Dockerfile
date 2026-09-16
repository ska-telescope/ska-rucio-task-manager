ARG BASE_RUCIO_CLIENT_IMAGE=registry.gitlab.com/ska-telescope/src/src-dm/ska-src-dm-da-rucio-client/rucio-client-core-py313
ARG BASE_RUCIO_CLIENT_TAG=41.0.0

FROM $BASE_RUCIO_CLIENT_IMAGE:$BASE_RUCIO_CLIENT_TAG

ENV RUCIO_TASK_MANAGER_ROOT=/opt/rucio-task-manager

USER root

# repo for oidc-agent
RUN yum -y install wget
RUN wget https://repo.data.kit.edu/data-kit-edu-almalinux9.repo -O /etc/yum.repos.d/data-kit-edu-almalinux9.repo

RUN yum -y install wget vim git python3 python3-devel openssl-devel swig gcc-c++ oidc-agent jq

# fix for /bin/bin /in oidc-agent-service
RUN sed -i 's/bin\/bin/bin/' /usr/bin/oidc-agent-service

RUN python3 -m pip install --upgrade pip wheel \
 && python3 -m pip install "setuptools<70"

COPY requirements.txt /tmp/requirements.txt

# additional index for ska-src-mm-notification
RUN python3 -m pip install --no-build-isolation -r /tmp/requirements.txt --extra-index-url https://gitlab.com/api/v4/projects/77880073/packages/pypi/simple

# install the ska-src-dm-di-ingestor service so that its srcnet-ingest CLI is available to
# the ephemeral ingestion tests. This mirrors the ingestor's own image (Dockerfile.rucio.core):
# clone the repository at a pinned ref, install its dependencies with poetry directly into the
# system environment and add its bin/ directory to PATH.
ARG INGESTOR_REPO=https://gitlab.com/ska-telescope/src/src-dm/ska-src-dm-di-ingestor.git
ARG INGESTOR_REF=1.0.1
ENV INGESTOR_ROOT=/opt/ska-src-dm-di-ingestor
RUN python3 -m pip install poetry==1.8.5 && poetry config virtualenvs.create false
RUN git clone --depth 1 --branch ${INGESTOR_REF} ${INGESTOR_REPO} ${INGESTOR_ROOT} \
 && cd ${INGESTOR_ROOT} \
 && poetry install --only main \
 && chmod +x ${INGESTOR_ROOT}/bin/*
ENV PATH="${INGESTOR_ROOT}/bin:$PATH"

COPY --chown=user . ${RUCIO_TASK_MANAGER_ROOT}

WORKDIR ${RUCIO_TASK_MANAGER_ROOT}

ENV TASK_FILE_RELPATH=etc/tasks/stubs.yml

USER user

ENV DAVIX_DISABLE_SESSION_CACHING=true

ENTRYPOINT ["bash", "./etc/docker/docker-entrypoint.sh"]
