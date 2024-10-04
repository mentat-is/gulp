# Docker Installation

[TOC]

## pull image directly from docker hub

```bash
docker run mentatis/gulp-core:latest
```

## (re)build image from a dev environment

1. clone this repository and perform [install from sources](<./Install Dev.md>)

2. build gulp image

~~~bash
DOCKER_BUILDKIT=1 docker build --progress=plain --build-arg _VERSION=$(git describe --tags --always) --rm -t gulp-core .

# to rebuild, add --no-cache flag ...
~~~

## run with docker-compose

you have to just provide your own [gulp_cfg.json](../gulp_cfg_template.json) file to the container, and you're ready to go!

use the provided [run_gulp.sh](../run_gulp.sh) script to run gulp with docker-compose:

```bash
# default binds to port 8080 and interface 0.0.0.0
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh

# bind to a different port
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json PORT=8081 ./run_gulp.sh

# also to a a different interface
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json PORT=8081 IFACE=192.168.1.1 ./run_gulp.sh

# reset elasticsearch (use index name)
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh --reset-elastic myidx

# reset elasticsearch and collab
GULP_CONFIG_PATH=/path/to/your/gulp_cfg.json ./run_gulp.sh --reset-elastic myidx --reset-collab
```
