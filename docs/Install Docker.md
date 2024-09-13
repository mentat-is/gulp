# docker install

## (re)build image

> until we provide a docker image to be pulled :)

1. clone this repository and perform [install from sources](<./Install Dev.md>)

2. build gulp image

~~~bash
./update_requirements_txt.sh
docker build --rm --tag gulp:latest --ssh default .

# or rebuild adding the --no-cache flag
# docker build --rm --no-cache --tag gulp:latest --ssh default .
~~~

## run

> using default [docket-compose](../docker-compose.yml), you should provide your own ...

```bash
# create configuration from template, if needed
#
# NOTE: elastic_url, postgres_url, path_certs will be overridden by environment variables set in the docker-compose.yml file
copy ./template_cfg.json ./gulp_cfg.json

# bring up the full stack of services
docker compose --profile full up -d
```

> also, on first run, database must be initialized first:

```bash
# 1. brings up the full stack of services except gulp
docker compose up -d

# 2. run gulp only, telling it to reset/create elasticsearch index "testidx" and collaboration database
EXTRA_ARGS="--reset-collab --reset-elastic testidx" docker compose up gulp
```
