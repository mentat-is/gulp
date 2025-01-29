- [docker Installation](#docker-installation)
  - [pull image directly from docker hub](#pull-image-directly-from-docker-hub)
  - [(re)build image from a dev environment](#rebuild-image-from-a-dev-environment)
  - [run with docker-compose](#run-with-docker-compose)

# docker Installation

## pull image directly from docker hub

```bash
docker run mentatis/gulp-core:latest
```

## (re)build image from a dev environment

1. clone this repository and perform [install from sources](<./Install Dev.md>)

2. build gulp image

~~~bash
docker buildx build --progress=plain --build-arg _VERSION=$(git describe --tags --always) --rm -t gulp-core .

# to rebuild, add --no-cache flag ...
~~~

## run with docker-compose

you have to just provide your own [gulp_cfg.json](../gulp_cfg_template.json) file to the container, and you're ready to go!

~~~bash
BIND_TO_PORT=8080 PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files GULP_CONFIG_PATH=/home/valerino/repos/gulp/gulp_cfg.json docker compose --profile full up
~~~

> to cleanup docker volumes, use the provided [reset script](../reset_docker.sh).
