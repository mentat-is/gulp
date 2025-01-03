# Default build arguments
_BUILD_ARGS_RELEASE_TAG ?= latest
_BUILD_ARGS_DOCKERFILE ?= Dockerfile

# Which one should we use for the official version?
GIT_HASH ?= $(shell git log --format="%h" -n 1)
GIT_TAG ?= $(shell git describe --tags --always)

# Docker configuration
DOCKER_USERNAME ?= mentatis
APPLICATION_NAME ?= gulp

# Build the Docker image
_builder:
	DOCKER_BUILDKIT=1 docker build \
		--build-arg _VERSION=${GIT_HASH} \
		--rm \
		--tag ${DOCKER_USERNAME}/${APPLICATION_NAME}:${_BUILD_ARGS_RELEASE_TAG} \
		-f ${_BUILD_ARGS_DOCKERFILE} .

# Push the Docker image
_pusher:
	docker push ${DOCKER_USERNAME}/${APPLICATION_NAME}:${GIT_HASH}

# Public targets
build:
	$(MAKE) _builder

push:
	$(MAKE) _pusher
