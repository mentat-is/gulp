- [docker Installation](#docker-installation)
  - [pull latest image directly from docker hub](#pull-latest-image-directly-from-docker-hub)
  - [(re)build image](#rebuild-image)
  - [run with docker-compose](#run-with-docker-compose)
    - [cleanup](#cleanup)

# docker Installation

> **WARNING**: for any issue refer to [troubleshooting.md](./troubleshooting.md)

## pull latest image directly from docker hub

> **WARNING**: this **may** be outdated, rebuild it following the docs below or use the [dev install](./install_dev.md) instead.

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
  ~~~

2. build `gulp-core` image

  ~~~bash
  cd ./gulp
  # to rebuild, add --no-cache flag ...
  export $(grep -v '^#' .env | xargs)
  docker buildx build --progress=plain --build-arg _PYTHON_VERSION=$PYTHON_VERSION --build-arg _VERSION=$(git describe --tags --always) --build-arg _MUTY_VERSION=$(cd muty-python && git describe --tags --always && cd ..) --rm -t gulp-core .
  ~~~

## run with docker-compose

you can run all the gulp stack on the `local machine` using the provided [docker-compose.yml](../docker-compose.yml), **even though this should be considered a **play-only** setup, not suitable for production**

you have to just provide your `GULP_WORKING_DIR` to the image, with a valid [gulp_cfg.json](../gulp_cfg_template.json) inside, and you're ready to go!

> if a configuration is not provided, a default one will be created in the `GULP_WORKING_DIR` you specified (may need some tweaking afterwards, though).

~~~bash
# supplying GULP_IMAGE is optional, either the latest is pulled from our registry.
# starts gulp backend and dev tools (--profile dev), ui must be run separately
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gulp --profile dev up

# also run the ui (--profile gui) in a container
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gui --profile dev up

# add extra arguments to pass on the gulp commandline with EXTRA_ARGS, i.e. to tweak log-level
EXTRA_ARGS="--log-level warning" GULP_IMAGE=gulp-core:latest GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gui --profile dev up

# run gulp service only, oneshot (--rm)
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose run -p 8080:8080 --rm gulp gulp --log-level warning
~~~

multiple profiles (one or more) may be specified using on the `docker compose` command line:

- `--profile gui`: run gulp-web client ui
- `--profile gulp`: run opensearch, postgresql, sftpd, gulp
- `--profile dev`: run opensearch, postgresql, sftpd , adminer, elasticvue
- `--profile os-dashboards`: run opensearch, opensearch-dahsboards
- *no profile specified: just `opensearch` and `postgresql` are run*

of course, you may provide your own compose file to suit your particular configuration (multiple OpenSearch nodes, ...).

> please note that the provided [docker-compose.yml](../docker-compose.yml) is just an example (albeit working!), and also have the `OpenSearch security plugin DISABLED`: you may want to change this for your production environment!

### cleanup

to cleanup `gulp`, `postgresql` and `opensearch` data volumes, use the provided [reset script](../reset_docker.sh).
