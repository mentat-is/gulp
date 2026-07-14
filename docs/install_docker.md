- [Docker installation](#docker-installation)
  - [Run the default stack](#run-the-default-stack)
  - [Run a distributed stack](#run-a-distributed-stack)
  - [Use a local backend image](#use-a-local-backend-image)
  - [Compose profiles](#compose-profiles)
    - [Exposed services](#exposed-services)
    - [TLS certificates](#tls-certificates)
    - [Cleanup](#cleanup)

# Docker installation

Docker Compose runs the backend, its bundled web UI, and required services.
This configuration is intended for local evaluation; secure it and adapt it
before production use.

## Run the default stack

The `gulp` profile starts one backend container. Its image includes the built
web UI, served at the backend address.

```bash
git clone https://github.com/mentat-is/gulp.git && cd gulp
docker compose --profile gulp up -d
```

Open [http://localhost:8080](http://localhost:8080) and sign in with
`admin/admin`. The API reference is at [http://localhost:8080/docs](http://localhost:8080/docs).
Set `GULP_WORKING_DIR` before the command to store the generated configuration,
certificates, plugins, and mappings outside the default `~/.config/gulp`.

## Run a distributed stack

Use the `distributed` profile when a reverse proxy, CDN, or deployment topology needs
the UI and API to scale independently. It starts the backend on port 8080 and a
separate `gulp-ui` container on port 3000.

```bash
docker compose --profile distributed up -d --build
```

## Use a local backend image

Build the backend image from this repository, including the bundled UI:

```bash
docker buildx build --progress=plain --build-arg _VERSION="$(git describe --tags --always)" --rm -t gulp-core:local .
GULP_IMAGE=gulp-core:local docker compose --profile gulp up -d
```

## Compose profiles

OpenSearch, PostgreSQL, MinIO, and Redis always start. Add profiles as needed:

- `--profile gulp`: run one gULP backend container with its bundled web UI
- `--profile distributed`: run gULP and a separate `gulp-ui` container
- `--profile dev`: also run adminer, elasticvue, redis-insight
- `--profile metrics`: also run Prometheus, Grafana, Redis Insight, and Elasticvue
- `--profile extra`: also run `opensearch-dashboards` to manage opensearch and `sftpd` to manage files in the `GULP_WORKING_DIR`

You can provide your own Compose file for production topologies. The supplied
Compose file disables the OpenSearch security plugin and is not a production
security configuration.

### Exposed services

With the default [docker-compose.yml](../docker-compose.yml), services are
exposed as follows. Extra profiles expose extra tools; check the compose file
when changing profiles.

- gULP API and default web UI, with `--profile gulp`: [http://localhost:8080](http://localhost:8080)
  - API reference: [http://localhost:8080/docs](http://localhost:8080/docs)
- Separate gULP web UI, with `--profile distributed`: [http://localhost:3000](http://localhost:3000)
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
- Prometheus, with `--profile metrics`: [http://localhost:9090](http://localhost:9090)
- Grafana, with `--profile metrics`: [http://localhost:3030](http://localhost:3030)
- FTP, with `--profile extra`: `localhost:21`, passive ports `21000-21010`

### TLS certificates

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

### Cleanup

to cleanup `gulp`, `postgresql` and `opensearch` data volumes, use the provided [reset script](../reset_docker.sh).
