- [Docker installation](#docker-installation)
  - [Run the backend](#run-the-backend)
  - [Run the web UI](#run-the-web-ui)
  - [Use a local backend image](#use-a-local-backend-image)
  - [Compose profiles](#compose-profiles)
    - [Exposed services](#exposed-services)
    - [TLS certificates](#tls-certificates)
    - [Cleanup](#cleanup)

# Docker installation

Docker Compose defines separate backend and nginx web UI services alongside
the backend's required services.
This configuration is intended for local evaluation; secure it and adapt it
before production use.

## Run the backend

The `gulp` profile starts the backend without a web UI.

```bash
git clone https://github.com/mentat-is/gulp.git && cd gulp
docker compose --profile gulp up -d
```

The API is at [http://localhost:8080](http://localhost:8080), and its reference
is at [http://localhost:8080/docs](http://localhost:8080/docs).
Set `GULP_WORKING_DIR` before the command to store the generated configuration,
certificates, plugins, and mappings outside the default `~/.config/gulp`.

## Run the web UI

The independent `ui` profile starts only the nginx web UI when the service is
named explicitly. nginx serves the compiled production build; pnpm development
servers are not used in Docker.

```bash
docker compose --profile ui up -d gulp-ui
```

To run the local backend and UI together:

```bash
docker compose --profile gulp --profile ui up -d
```

Add `--build` when UI source or build arguments have changed.

## Use a local backend image

Build the backend image from this repository:

```bash
docker buildx build --progress=plain --build-arg _VERSION="$(git describe --tags --always)" --rm -t gulp-core:local .
GULP_IMAGE=gulp-core:local docker compose --profile gulp up -d
```

## Compose profiles

Without an explicit service target, OpenSearch, PostgreSQL, MinIO, and Redis
always start. Add profiles as needed:

- `--profile gulp`: run the gULP backend
- `--profile ui`: run the independent nginx `gulp-ui` container
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

- gULP API, with `--profile gulp`: [http://localhost:8080](http://localhost:8080)
  - API reference: [http://localhost:8080/docs](http://localhost:8080/docs)
- nginx-served gULP web UI, with `--profile ui`: [http://localhost:3000](http://localhost:3000)
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
