- [docker Installation](#docker-installation)
  - [pull image directly from docker hub](#pull-image-directly-from-docker-hub)
  - [(re)build image from a dev environment](#rebuild-image-from-a-dev-environment)
  - [run with docker-compose](#run-with-docker-compose)

# docker Installation

> THIS IS OUTDATED, NEEDS REVIEW.

[TOC]

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

use the provided [run_gulp.sh](../run_gulp.sh) script to manage gulp running through the provided [docker-compose](../docker-compose.yml):

> use `run_gulp.sh --help to see available options`

```bash
# default binds to port 8080 and interface 0.0.0.0. on first run, default collaboration database and default "gulpidx" index are initialized.
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh

# bind to a different port
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json PORT=8081 ./run_gulp.sh

# also to a a different interface
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json PORT=8081 IFACE=192.168.1.1 ./run_gulp.sh

# reset opensearch (use index name)
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh --reset-elastic myidx

# reset opensearch and collab
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh --reset-elastic myidx --reset-collab
```

> NOTE:
> when run through the provided docker-compose, gulp creates the following in the current directory:
>
> - opensearch_data
> - postgres_data
> - .gulpconfig
>
> to cleanup correctly and restart from scratch, **all of them should be removed**.

once you're done, `run_gulp.sh --stop` may be used to stop all the running containers.
