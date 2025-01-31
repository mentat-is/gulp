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

2. build `gulp-core` image

~~~bash
docker buildx build --progress=plain --build-arg _VERSION=$(git describe --tags --always) --rm -t gulp-core .

# to rebuild, add --no-cache flag ...
~~~

## run with docker-compose

you have to just provide your own [gulp_cfg.json](../gulp_cfg_template.json) file to the container, and you're ready to go!

~~~bash
# supplying local GULP_IMAGE is optional, either the latest is pulled from our registry, starts gulp, gulp-web (and adminer and elasticvue for debugging)
GULP_IMAGE=gulp-core:latest BIND_TO_PORT=8080 PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files GULP_CONFIG_PATH=/home/valerino/repos/gulp/gulp_cfg.json docker compose --profile gulp --profile dev up
~~~

> multiple profiles may be specified using on the `docker compose` command line:
>
> - `--profile gui`: run gulp-web client
> - `--profile gulp`: run gulp server
> - `--profile dev`: also run adminer and elasticvue, for debugging
> - `--profile os-dashboards`: also run opensearch dahsboards
> - *no profile specified: just run `opensearch` and `postgresql`*

to cleanup `gulp`, `postgresql` and `opensearch data` volumes, use the provided [reset script](../reset_docker.sh).
