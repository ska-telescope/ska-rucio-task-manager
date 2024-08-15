# Makefile for extending rucio client images to include rucio-task-manager.
.ONESHELL:

BASE_RUCIO_CLIENT_TAG:=`cat BASE_RUCIO_CLIENT_TAG`

build-base:
	@docker build . -f Dockerfile --build-arg BASE_RUCIO_CLIENT_IMAGE=rucio/rucio-clients \
	--build-arg BASE_RUCIO_CLIENT_TAG=$(BASE_RUCIO_CLIENT_TAG) --tag rucio-task-manager:$(BASE_RUCIO_CLIENT_TAG)

build-skao:
	@docker build . -f Dockerfile \
	--build-arg BASE_RUCIO_CLIENT_IMAGE=registry.gitlab.com/ska-telescope/src/src-dm/ska-src-dm-da-rucio-client \
	--build-arg BASE_RUCIO_CLIENT_TAG=$(BASE_RUCIO_CLIENT_TAG) --tag rucio-task-manager:$(BASE_RUCIO_CLIENT_TAG)
