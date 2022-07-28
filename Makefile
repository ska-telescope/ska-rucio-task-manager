# Makefile for extending rucio client images to include rucio-analysis.
.ONESHELL:

BASE_RUCIO_CLIENT_TAG:=`cat BASE_RUCIO_CLIENT_TAG`

rucio:
	@docker build . -f Dockerfile --build-arg BASE_RUCIO_CLIENT_IMAGE=rucio/rucio-clients \
	--build-arg BASE_RUCIO_CLIENT_TAG=$(BASE_RUCIO_CLIENT_TAG) --tag rucio-analysis:$(BASE_RUCIO_CLIENT_TAG)

skao:
	@docker build . -f Dockerfile \
	--build-arg BASE_RUCIO_CLIENT_IMAGE=registry.gitlab.com/ska-telescope/src/ska-rucio-client \
	--build-arg BASE_RUCIO_CLIENT_TAG=$(BASE_RUCIO_CLIENT_TAG) --tag rucio-analysis:$(BASE_RUCIO_CLIENT_TAG)
