- [docker Installation](#docker-installation)
  - [pull latest image directly from docker hub](#pull-latest-image-directly-from-docker-hub)
  - [(re)build image](#rebuild-image)
  - [run with docker-compose](#run-with-docker-compose)
    - [compose profiles](#compose-profiles)
    - [exposed services](#exposed-services)
    - [SSL certificates](#ssl-certificates)
    - [cleanup](#cleanup)

# docker Installation

> **WARNING**: for any issue refer to [troubleshooting.md](./troubleshooting.md)

## pull latest image directly from docker hub

> **WARNING**: this **may** be outdated, rebuild it following the docs below or use the [dev install](./install_dev.md) instead.

```bash
docker run mentatis/gulp-core:latest
```

## (re)build image

> **NOTE TO DEVTEAM**: Docker image dependencies are resolved from
> [pyproject.toml](../pyproject.toml). Keep that file current before cutting a
> stable tag or release.

1. clone repository

  ~~~bash
  mkdir ./build && cd ./build
  git clone https://github.com/mentat-is/gulp.git
  ~~~

2. build `gulp-core` image

  ~~~bash
  # WARNING: must run in bash compatible shell, i.e. fish doesnt work
  # to rebuild, add --no-cache flag to the docker buildx line
  cd ./gulp
  bash -c "export $(grep -v '^#' .env | xargs) && docker buildx build --progress=plain --build-arg _VERSION=$(git describe --tags --always) --rm -t gulp-core ."
  ~~~

## run with docker-compose

you can run all the gulp stack on the `local machine` using the provided [docker-compose.yml](../docker-compose.yml), **even though this should be considered a **play-only** setup, not suitable for production**

you have to just provide your `GULP_WORKING_DIR` to the image, with a valid [gulp_cfg.json](../src/gulp/gulp_cfg_template.json) inside, and you're ready to go!

> if a configuration is not provided, a default one will be created in the `GULP_WORKING_DIR` you specified (may need some tweaking afterwards, though).

~~~bash
# supplying GULP_IMAGE is optional, either the latest is pulled from our registry.
# starts gulp backend and dev tools (--profile dev), ui must be run separately
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gulp --profile dev up

# also run the ui (--profile gui) in a container
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gui --profile dev up

# add extra arguments to pass on the gulp commandline with EXTRA_ARGS, i.e. to tweak log-level
EXTRA_ARGS="--log-level warning" GULP_IMAGE=gulp-core:latest GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose --profile gui --profile dev up

# run gulp service only, oneshot (--rm)
GULP_IMAGE=gulp-core:latest GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8080 GULP_WORKING_DIR=/home/valerino/.config/gulp docker compose run -p 8080:8080 --rm gulp gulp --log-level warning
~~~

multiple profiles (one or more) may be specified using on the `docker compose` command line:

> opensearch, postgresql, minio, redis, are always run regardless of the profile.

### compose profiles

- `--profile gui`: run gulp-web client ui
- `--profile gulp`: also run gulp
- `--profile dev`: also run adminer, elasticvue, redis-insight
- `--profile extra`: also run `opensearch-dashboards` to manage opensearch and `sftpd` to manage files in the `GULP_WORKING_DIR`

of course, you may provide your own compose file to suit your particular configuration (multiple OpenSearch nodes, ...).

> please note that the provided [docker-compose.yml](../docker-compose.yml) is just an example (albeit working!), and also have the `OpenSearch security plugin DISABLED`: you may want to change this for your production environment!

### exposed services

With the default [docker-compose.yml](../docker-compose.yml), services are
exposed as follows. Extra profiles expose extra tools; check the compose file
when changing profiles.

- gULP API: [http://localhost:8080/docs](http://localhost:8080/docs)
- gULP web UI, with `--profile gui`: [http://localhost:3000](http://localhost:3000)
  - default gULP admin user: `admin/admin`
- PostgreSQL: `localhost:5432`
  - user/password: `postgres/Gulp1234!`
- Adminer, with `--profile dev`: [http://localhost:8081](http://localhost:8081)
  - server/user/password: `postgres/postgres/Gulp1234!`
- OpenSearch: [http://localhost:9200](http://localhost:9200)
  - user/password: `admin/Gulp1234!`
- Elasticvue, with `--profile dev`: [http://localhost:8082](http://localhost:8082)
- OpenSearch Dashboards, with `--profile extra`: [http://localhost:5601](http://localhost:5601)
- Redis: `localhost:6379`
  - user/password: `default/Gulp1234!`
- Redis Insight, with `--profile dev`: [http://localhost:5540](http://localhost:5540)
- MinIO: [http://localhost:9000](http://localhost:9000)
  - MinIO console: [http://localhost:9001](http://localhost:9001)
  - user/password: `admin/Gulp1234!`
  - used as S3-compatible storage for plugins that need it
- Prometheus, with `--profile dev`: [http://localhost:9090](http://localhost:9090)
- Grafana, with `--profile dev`: [http://localhost:3030](http://localhost:3030)
- FTP, with `--profile extra`: `localhost:21`, passive ports `21000-21010`

### SSL certificates

Put certificates under `$GULP_WORKING_DIR/certs`, which is mounted into the
containers by the compose file.

OpenSearch:

- `opensearch_verify_certs`: set to `false` to skip server verification.
- `$GULP_WORKING_DIR/certs/opensearch-ca.pem`: CA certificate.
- `$GULP_WORKING_DIR/certs/opensearch.pem`: client certificate.
- `$GULP_WORKING_DIR/certs/opensearch.key`: passwordless client key.

PostgreSQL:

- `postgres_ssl`: use SSL for PostgreSQL connections.
- `postgres_verify_certs`: set to `false` to skip server verification.
- `$GULP_WORKING_DIR/certs/postgres-ca.pem`: CA certificate.
- `$GULP_WORKING_DIR/certs/postgres.pem`: client certificate.
- `$GULP_WORKING_DIR/certs/postgres.key`: client key.

gULP HTTPS:

- `https_enforce`: require HTTPS.
- `https_enforce_client_certs`: require client certificates signed by
  `gulp-ca.pem`.
- `$GULP_WORKING_DIR/certs/gulp-ca.pem`: gULP CA.
- `$GULP_WORKING_DIR/certs/gulp.pem`: gULP server certificate.
- `$GULP_WORKING_DIR/certs/gulp.key`: gULP server key.

For Redis and MinIO TLS, use their service-specific configuration and mount any
needed files under `$GULP_WORKING_DIR/certs`.

### cleanup

to cleanup `gulp`, `postgresql` and `opensearch` data volumes, use the provided [reset script](../reset_docker.sh).
