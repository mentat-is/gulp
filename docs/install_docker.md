- [Docker installation](#docker-installation)
  - [Run the backend](#run-the-backend)
  - [Run the web UI](#run-the-web-ui)
  - [Docker Compose configuration](#docker-compose-configuration)
  - [Use a local backend image](#use-a-local-backend-image)
  - [Compose profiles](#compose-profiles)
    - [Exposed services](#exposed-services)
    - [Production TLS](#production-tls)
    - [Stop without deleting data](#stop-without-deleting-data)

# Docker installation

Docker Compose defines separate backend and nginx web UI services alongside
the backend's required services. Use
[docker-compose-docker-prod.yml](../docker-compose-docker-prod.yml) when all of
them run in Docker. It keeps the same services, volumes, profiles, and ports
as [docker-compose.yml](../docker-compose.yml). The production file passes
gULP four dependency URLs with Docker service names instead of the template's
`localhost` addresses, and uses the published UI image instead of a local
build.

## Run the backend

The `gulp` profile starts the backend without a web UI.

```bash
git clone https://github.com/mentat-is/gulp.git && cd gulp
docker compose -f docker-compose-docker-prod.yml --profile gulp up -d
```

On the Docker host, the API is at [http://localhost:8080](http://localhost:8080)
and its reference is at [http://localhost:8080/docs](http://localhost:8080/docs).
Set `GULP_WORKING_DIR` before the command to store the generated configuration,
plugins, and mappings outside the default `~/.config/gulp`.
The container starts as root to prepare the bind-mounted working directory and
its cache mounts for the `gulp` runtime user, then drops privileges before
starting the backend. This also handles directories Docker created as root on
the host during a fresh install. The ownership check skips recursive work on
later starts once the mount roots have the expected owner.

## Run the web UI

The independent `ui` profile starts only the nginx web UI when the service is
named explicitly; start the backend separately if it is not already running.
The production Compose file uses `mentatis/gulp-ui:latest` from Docker Hub,
or the image set in `GULP_UI_IMAGE`. nginx serves its compiled production build;
pnpm development servers are not used in Docker.

```bash
docker compose -f docker-compose-docker-prod.yml --profile ui up -d gulp-ui
```

To run the backend and UI together:

```bash
docker compose -f docker-compose-docker-prod.yml --profile gulp --profile ui up -d
```

The UI image is published by `.github/workflows/ui-docker-ci.yml` when a
version tag is pushed. To fetch a newer `latest` image before starting the UI,
run `docker compose -f docker-compose-docker-prod.yml --profile ui pull gulp-ui`.
To build the UI from source instead, use the original `docker-compose.yml` with
`--profile ui up -d --build gulp-ui`.
From another computer, open `http://SERVER_IP:3000` and enter
`http://SERVER_IP:8080` in the UI's **Server address** field. The
`localhost:8080` placeholder in that field would refer to the browser's
computer, not the Docker host. The host firewall must allow access to both
ports for this setup.

## Docker Compose configuration

Run the commands from the repository root. Docker Compose automatically reads
the existing `.env` file for PostgreSQL, OpenSearch, Redis, and SeaweedFS
settings and credentials. The new file passes those values to gULP in
`GULP_POSTGRES_URL`, `GULP_OPENSEARCH_URL`, `GULP_REDIS_URL`, and `GULP_S3_URL`,
using `postgres`, `os01`, `redis`, and `seaweedfs` as hosts. The generated
`gulp_cfg.json` may still show `localhost` defaults, but these environment
variables take precedence; no manual edit is needed.
Because Compose inserts the credentials directly into these URLs, use
URL-safe values when changing the passwords or S3 keys in `.env`.

Use `-f docker-compose-docker-prod.yml` with later commands too:

```bash
docker compose -f docker-compose-docker-prod.yml --profile gulp --profile ui ps
docker compose -f docker-compose-docker-prod.yml --profile gulp --profile ui logs --tail=100 gulp
```

If the original Compose file is already running in this repository directory,
stop it with `docker compose --profile gulp --profile ui down` (without `-v`),
then start the Docker variant. The project name, volumes, and `.env` values
stay the same.

This file retains the original published ports and disabled OpenSearch security
plugin. Before exposing it outside a trusted network, configure a firewall and
HTTPS/WSS reverse proxy as described in the
[enterprise deployment guide](enterprise_deployment.md).

## Use a local backend image

The repository's backend Dockerfile defaults to Python 3.13 on Debian Bookworm.
Build a local image with:

```bash
docker buildx build --progress=plain --build-arg _VERSION="$(git describe --tags --always)" --rm -t gulp-core:local .
GULP_IMAGE=gulp-core:local docker compose -f docker-compose-docker-prod.yml --profile gulp up -d
```

Pass `_PYTHON_VERSION` as an additional build argument when validating another
supported Python version.

## Compose profiles

Without an explicit service target, OpenSearch, PostgreSQL, SeaweedFS, and Redis
always start. Add profiles as needed:

- `--profile gulp`: run the gULP backend
- `--profile ui`: run the independent nginx `gulp-ui` container
- `--profile dev`: also run adminer, elasticvue, redis-insight
- `--profile metrics`: also run Prometheus, Grafana, Redis Insight, and Elasticvue
- `--profile extra`: also run OpenSearch Dashboards and the FTP server that exposes files in `GULP_WORKING_DIR`

Both Compose files expose the same ports. The Docker variant changes gULP's
dependency URLs and uses the published UI image; it does not add production
security controls.

### Exposed services

With either Compose file, services are exposed as follows when `.env` retains
its repository defaults. Docker publishes these ports on all host interfaces;
the `localhost` addresses below are examples for access from the Docker host.
Extra profiles expose extra tools; check the Compose file when changing
profiles.

- gULP API, with `--profile gulp`: [http://localhost:8080](http://localhost:8080)
  - API reference: [http://localhost:8080/docs](http://localhost:8080/docs)
- nginx-served gULP web UI, with `--profile ui`: [http://localhost:3000](http://localhost:3000)
  - default gULP admin login on a fresh installation: `admin/admin`
- PostgreSQL: `localhost:5432`
  - user/password: `postgres/Gulp1234!`
- Adminer, with `--profile dev`: [http://localhost:8081](http://localhost:8081)
  - server/user/password: `postgres/postgres/Gulp1234!`
- OpenSearch: [http://localhost:9200](http://localhost:9200)
  - the security plugin is disabled, so authentication is not enforced
- Elasticvue, with `--profile dev`: [http://localhost:8082](http://localhost:8082)
- OpenSearch Dashboards, with `--profile extra`: [http://localhost:5601](http://localhost:5601)
- Redis: `localhost:6379`
  - user/password: `default/Gulp1234!`
- Redis Insight, with `--profile dev`: [http://localhost:5540](http://localhost:5540)
- SeaweedFS S3 API: [http://localhost:9000](http://localhost:9000)
  - filer web UI: [http://localhost:9001](http://localhost:9001)
  - S3 access key and secret are read from `.env` (`admin`/`Gulp1234!` by default)
  - used as S3-compatible storage for plugins that need it
- Prometheus, with `--profile metrics`: [http://localhost:9090](http://localhost:9090)
- Grafana, with `--profile metrics`: [http://localhost:3030](http://localhost:3030)
- FTP, with `--profile extra`: `localhost:21`, passive ports `21000-21010`

### Production TLS

Both Compose files keep their HTTP defaults and publish the API and data
services on the host. For a public deployment, restrict direct access to these
ports and terminate HTTPS/WSS at nginx, an ingress controller, or a managed
load balancer. The
[enterprise deployment guide](enterprise_deployment.md) includes a complete
nginx example, optional mutual TLS, and native TLS configuration for
PostgreSQL, OpenSearch, Redis, and S3.

### Stop without deleting data

To stop the Docker stack and remove its containers and network while keeping
the named data volumes and gULP working directory:

```bash
docker compose -f docker-compose-docker-prod.yml --profile gulp --profile ui down
```

The [reset script](../reset_docker.sh) uses the default Compose file and stops
both the `gulp` and `ui` profiles. It also deletes volumes and the first-run
marker, so use the `down` command above when preserving data.
