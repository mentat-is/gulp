- [docker Installation](#docker-installation)
  - [pull image directly from docker hub](#pull-image-directly-from-docker-hub)
  - [(re)build image](#rebuild-image)
  - [run with docker-compose](#run-with-docker-compose)
    - [cleanup](#cleanup)

# docker Installation

## pull image directly from docker hub

```bash
docker run mentatis/gulp-core:latest
```

## (re)build image

> **NOTE TO DEVTEAM**: to build an image, a proper `requirements.txt` is needed, with all the python packages needed to at least startup the core and the basic plugins.
>
> **it is the responsibility of the devteam, indeed, to keep  `requirements.txt` updated using [update_requirements.sh](../update_requirements_txt.sh) script before committing a stable tag/release on the repository.**

1. clone repositories

  ~~~bash
  mkdir ./build && cd ./build
  git clone --recurse-submodules https://github.com/mentat-is/gulp.git
  cd ./gulp
  git clone https://github.com/mentat-is/muty-python.git
  ~~~

2. build `gulp-core` image

  ~~~bash
  # to rebuild, add --no-cache flag ...
  docker buildx build --progress=plain --build-arg _VERSION=$(git describe --tags --always) --build-arg _MUTY_VERSION=$(cd muty-python && git describe --tags --always) --rm -t gulp-core .
  ~~~

## run with docker-compose

you can run all the gulp stack on the `local machine` using the provided [docker-compose.yml](../docker-compose.yml).

you have to just provide your own [gulp_cfg.json](../gulp_cfg_template.json) file to the container, and you're ready to go!

~~~bash
# supplying local GULP_IMAGE is optional, either the latest is pulled from our registry, starts gulp, gulp-web (and adminer and elasticvue for debugging)
GULP_IMAGE=gulp-core:latest BIND_TO_PORT=8080 PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files GULP_CONFIG_PATH=/home/valerino/repos/gulp/gulp_cfg.json docker compose --profile gulp --profile dev up

# to add extra arguments, provide them with EXTRA_ARGS, i.e. to tweak log-level
EXTRA_ARGS="--log-level warning" GULP_IMAGE=... BIND_TO_PORT=... (same as above)
~~~

> multiple profiles may be specified using on the `docker compose` command line:
>
> - `--profile gui`: run gulp-web client
> - `--profile gulp`: run gulp server
> - `--profile dev`: also run adminer and elasticvue, for debugging
> - `--profile os-dashboards`: also run opensearch dahsboards
> - *no profile specified: just run `opensearch` and `postgresql`*

of course, you may provide your own compose file to suit your particular configuration (multiple OpenSearch nodes, ...).

### cleanup

to cleanup `gulp`, `postgresql` and `opensearch` data volumes, use the provided [reset script](../reset_docker.sh).
