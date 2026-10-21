# Installation

gULP has three parts:

- the **backend** (`gulp`, Python): the REST and WebSocket API, on port 8080;
- the **web UI** (`gulp-ui/`): a static React application, on port 3000. It
  runs in the browser and connects to the backend directly;
- the **services** the backend keeps its data in: OpenSearch (events),
  PostgreSQL (collaboration data), Redis (queues) and SeaweedFS (S3-compatible
  storage). The repository's [Compose file](../docker-compose.yml) runs all
  four with Docker.

## Choose an installation

| You want to | Follow |
| --- | --- |
| try gULP on your own machine in a few minutes | [Try it with Docker](#try-it-with-docker), below |
| run gULP on a Linux server for your team | [Server installation](install_server.md) |
| install a release on your workstation, outside Docker | [Install a release from source](#install-a-release-from-source), below |
| work on gULP's code | [Development installation](install_dev.md) |
| publish gULP over HTTPS, or run several backends | the server installation, then the [enterprise deployment guide](enterprise_deployment.md) |

## Requirements

| | |
| --- | --- |
| Operating system | Linux |
| Docker | Docker Engine with the Compose plugin |
| Memory | at least 8 GB of RAM; 16 GB or more for real investigations |
| Kernel setting | `vm.max_map_count` of at least 262144, for OpenSearch |
| Python | 3.12 or 3.13, outside Docker |
| Node.js | 22, with pnpm 10, to build and serve the web UI outside Docker |

## Try it with Docker

Everything runs in containers: the services, the backend and the web UI.

```bash
sudo sysctl -w vm.max_map_count=262144
git clone https://github.com/mentat-is/gulp.git && cd gulp
docker compose --profile gulp --profile ui up -d
```

The first start downloads the images and builds the web UI image, which takes
a few minutes. Then open <http://localhost:3000>, enter
`http://localhost:8080` as the backend address and log in as `admin` /
`admin`. The API reference is at <http://localhost:8080/docs>.

> **For evaluation only.** Every service uses the default passwords in
> [.env](../.env) and listens on all network interfaces, and OpenSearch runs
> without authentication. To run gULP for real, follow the
> [server installation](install_server.md).

Stop everything with `docker compose --profile gulp --profile ui down`, and
delete all data with [reset_docker.sh](../reset_docker.sh). The
[Docker Compose reference](install_docker.md) lists the profiles, the ports
and the default credentials.

## Install a release from source

gULP is not published on PyPI: install it from a release tag. The services
still run with Docker, from the same checkout.

```bash
git clone --branch v2.0.0 --depth 1 https://github.com/mentat-is/gulp.git && cd gulp
docker compose up -d
python3 -m venv .venv && . .venv/bin/activate
bash scripts/build_webui_package.sh
pip install .
gulp
```

Replace `v2.0.0` with the
[latest release](https://github.com/mentat-is/gulp/releases). The Git tag gives
the package its version; GitHub's **Source code (zip)** archive of a release
works the same way, with the commands run in the extracted folder.
`scripts/build_webui_package.sh` builds the web UI into the package; in another
terminal, with the virtual environment active, `gulp-webui` serves it on
<http://localhost:3000>.

## Configuration

- The working directory, `~/.config/gulp` or the directory in
  `GULP_WORKING_DIR`, holds `gulp_cfg.json`, extra plugins and mapping files,
  local ingestion files and temporary uploads.
- The first start creates `gulp_cfg.json` from the template. In an empty working
  directory, the first start also recreates the collaboration database and
  creates the `test_operation` operation.
- `GULP_POSTGRES_URL`, `GULP_OPENSEARCH_URL`, `GULP_REDIS_URL` and
  `GULP_S3_URL` override the service URLs in `gulp_cfg.json`.
  `GULP_BIND_TO_ADDR` and `GULP_BIND_TO_PORT` (set both) change the listen
  address, `0.0.0.0:8080` by default.
- Extra plugins and mapping files go in the working directory's `plugins/` and
  `mapping_files/` folders.
- The `gulp` command's options are described in the
  [README](../README.md#backend-run-examples) and by `gulp --help`.

If something goes wrong, see [troubleshooting](troubleshooting.md).
