ARG BASE_RUCIO_CLIENT_IMAGE
ARG BASE_RUCIO_CLIENT_TAG

FROM $BASE_RUCIO_CLIENT_IMAGE:$BASE_RUCIO_CLIENT_TAG

ENV RUCIO_TASK_MANAGER_ROOT /opt/rucio-task-manager

USER root

# repo for oidc-agent
RUN wget https://repo.data.kit.edu/data-kit-edu-centos7.repo -O /etc/yum.repos.d/data-kit-edu-centos7.repo

RUN yum -y install wget vim python3 python3-devel openssl-devel swig gcc-c++ oidc-agent

# fix for /bin/bin /in oidc-agent-service
RUN sed -i 's/bin\/bin/bin/' /usr/bin/oidc-agent-service

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt

RUN python3 -m pip install -r /tmp/requirements.txt

COPY --chown=user . ${RUCIO_TASK_MANAGER_ROOT}

WORKDIR ${RUCIO_TASK_MANAGER_ROOT}

ENV TASK_FILE_RELPATH etc/tasks/stubs.yml

USER user

ENV DAVIX_DISABLE_SESSION_CACHING true

ENTRYPOINT ["bash", "./etc/docker/docker-entrypoint.sh"]
