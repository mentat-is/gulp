# Installation

Choose the installation mode that fits your use case:

1. **Development:** use a repository checkout and run backend and UI tools
   manually.
2. **Tagged source:** clone a release tag or download its GitHub source archive
   and install it locally.
3. **Docker:** run the backend and UI in separate containers.
4. **Enterprise:** put private backend instances behind a TLS-terminating proxy.

## Required services

Every installation method requires running OpenSearch, PostgreSQL, and Redis
services. Start them before the gULP backend and keep them running. SeaweedFS is
also required when using features or plugins that need object storage.

For a quick setup, the recommended option is to start these services with the
[Docker Compose file](../docker-compose.yml) provided in the repository:

```bash
docker compose up -d
```

For a source installation without a repository checkout, download the Compose
file and its environment defaults from GitHub and start the services in one line:

```bash
mkdir gulp-services && cd gulp-services && curl -fsSLO 'https://raw.githubusercontent.com/mentat-is/gulp/master/{.env,docker-compose.yml}' && docker compose up -d
```

Tagged-source and development installations may use this Compose stack or
provide the services separately.

## Tagged source installation

Requirements: Python 3.12+. Select an immutable release tag when cloning:

```bash
git clone --branch v2.0.0 --depth 1 https://github.com/mentat-is/gulp.git
cd gulp
python3 -m venv .venv
source .venv/bin/activate
pip install .
```

The Git tag and commit generate `src/gulp/_version.py` during installation. To
install from GitHub's **Source code (zip)** archive instead, extract the archive
and run the same virtual-environment and `pip install .` commands inside it.
Archive metadata preserves the tag and commit even though the ZIP has no `.git`
directory.

The source package does not contain an independently versioned React build. To
build and bundle the UI, install Node.js and pnpm, then run:

```bash
bash scripts/build_webui_package.sh
pip install .
gulp-webui
```

Run `gulp` in another terminal for the backend. The first run creates
`~/.config/gulp/gulp_cfg.json`; the default local credentials are `admin/admin`.

Open [http://localhost:3000](http://localhost:3000) for the standalone UI and
[http://localhost:8080/docs](http://localhost:8080/docs) for the API.

## Development installation

Development uses the repository checkout and runs the backend and UI separately
for live editing. Follow [development installation](install_dev.md), then start
the UI with the [Web UI development guide](webui/install.md).

## Docker installation

Docker keeps the backend and nginx web UI in separate containers. Follow the
[Docker guide](install_docker.md) for the independent `gulp` and `ui` profiles.

## Enterprise deployment

For public HTTPS/WSS, multiple backend instances, or optional mutual TLS, follow
the [enterprise nginx deployment guide](enterprise_deployment.md). gULP itself
stays on private HTTP port 8080 behind the proxy.
