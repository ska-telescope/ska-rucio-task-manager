ARG BASE_RUCIO_CLIENT_IMAGE
ARG BASE_RUCIO_CLIENT_TAG

FROM $BASE_RUCIO_CLIENT_IMAGE:$BASE_RUCIO_CLIENT_TAG

ENV RUCIO_TASK_MANAGER_ROOT /opt/rucio-task-manager

USER root

# centos7 eol mitigation
RUN sed -i.bak 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i.bak 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* 

# repo for oidc-agent
RUN wget https://repo.data.kit.edu/data-kit-edu-centos7.repo -O /etc/yum.repos.d/data-kit-edu-centos7.repo

RUN yum -y install wget vim python3 python3-devel openssl-devel swig gcc-c++ oidc-agent jq

# fix for /bin/bin /in oidc-agent-service
RUN sed -i 's/bin\/bin/bin/' /usr/bin/oidc-agent-service

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /tmp/requirements.txt

# additional indices for ingestor and the rucio-extended-client
RUN python3 -m pip install -r /tmp/requirements.txt --extra-index-url https://gitlab.com/api/v4/projects/51600992/packages/pypi/simple --extra-index-url https://gitlab.com/api/v4/projects/39600235/packages/pypi/simple

COPY --chown=user . ${RUCIO_TASK_MANAGER_ROOT}

WORKDIR ${RUCIO_TASK_MANAGER_ROOT}

ENV TASK_FILE_RELPATH etc/tasks/stubs.yml

USER user

ENV DAVIX_DISABLE_SESSION_CACHING true

ENTRYPOINT ["bash", "./etc/docker/docker-entrypoint.sh"]
