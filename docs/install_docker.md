# Docker Compose reference

The repository's [docker-compose.yml](../docker-compose.yml) runs the services
gULP needs and, optionally, the backend, the web UI and a few tools. To get
started, see [Try it with Docker](installation.md#try-it-with-docker); for a
server, see the [server installation](install_server.md).

The supplied file is meant for local use: default passwords from
[.env](../.env), every port on all network interfaces, and OpenSearch's
security plugin disabled.

## Profiles

Without a profile, `docker compose up -d` starts OpenSearch, PostgreSQL, Redis
and SeaweedFS. Add profiles for more:

| Profile | Adds |
| --- | --- |
| `gulp` | the gULP backend, from the `mentatis/gulp-core` image |
| `ui` | the web UI, served by nginx; the image is built from `gulp-ui/` |
| `dev` | Adminer, Elasticvue and Redis Insight |
| `metrics` | Prometheus, Grafana, Redis Insight and Elasticvue |
| `extra` | OpenSearch Dashboards, and an FTP server on the working directory |

`docker compose --profile ui up -d gulp-ui` starts the web UI alone. Add
`--build` after changing the UI's sources.

`GULP_WORKING_DIR` (default `~/.config/gulp`) is mounted into the backend
container: the generated configuration, extra plugins and mappings live there.
Set it before `docker compose` to keep them elsewhere.

## Ports and default credentials

| Service | Address | Credentials |
| --- | --- | --- |
| gULP API (`gulp`) | <http://localhost:8080>, reference at `/docs` | `admin` / `admin` |
| web UI (`ui`) | <http://localhost:3000> | |
| PostgreSQL | `localhost:5432` | `postgres` / `Gulp1234!` |
| OpenSearch | <http://localhost:9200> | none: security disabled |
| Redis | `localhost:6379` | `default` / `Gulp1234!` |
| SeaweedFS S3 API | <http://localhost:9000> | `admin` / `Gulp1234!` |
| SeaweedFS filer UI | <http://localhost:9001> | |
| Adminer (`dev`) | <http://localhost:8081> | server `postgres`, `postgres` / `Gulp1234!` |
| Elasticvue (`dev`) | <http://localhost:8082> | |
| Redis Insight (`dev`) | <http://localhost:5540> | |
| OpenSearch Dashboards (`extra`) | <http://localhost:5601> | |
| Prometheus (`metrics`) | <http://localhost:9090> | |
| Grafana (`metrics`) | <http://localhost:3030> | |
| FTP (`extra`) | `localhost:21`, passive ports 21000-21010 | `gulp` / `Gulp1234!` |

## Images

Official release images use the release tag, for example
`mentatis/gulp-core:v2.0.0` and `mentatis/gulp-ui:v2.0.0`; their OCI labels
contain the version and the full source revision. Select them with
`GULP_IMAGE` and `GULP_UI_IMAGE`.

To build the backend image from this repository:

```bash
docker buildx build --progress=plain \
  --build-arg GULP_VERSION=0.0.0 \
  --build-arg GULP_REVISION="$(git rev-parse HEAD)" \
  --rm -t gulp-core:local .
GULP_IMAGE=gulp-core:local docker compose --profile gulp up -d
```

The backend image uses Debian 13 (Trixie). Rebuild local images after the
Dockerfile changes, or pull a newly published image, to get the new system
libraries.

## Reset

[reset_docker.sh](../reset_docker.sh) deletes the gULP, PostgreSQL and
OpenSearch data volumes.
