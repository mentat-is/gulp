# Installation

Choose the installation mode that fits your use case:

1. **Development:** use a repository checkout and run backend and UI tools
   manually.
2. **PyPI:** install `mentat-gulp`; the wheel includes the compiled UI and a
   `gulp-webui` launcher.
3. **Docker:** run the backend and UI in separate containers.

## Required services

Every installation method requires running OpenSearch, PostgreSQL, and Redis
services. Start them before the gULP backend and keep them running. SeaweedFS is
also required when using features or plugins that need object storage.

For a quick setup, the recommended option is to start these services with the
[Docker Compose file](../docker-compose.yml) provided in the repository:

```bash
docker compose up -d
```

For a PyPI installation without a repository checkout, download the Compose
file and its environment defaults from GitHub and start the services in one
line:

```bash
mkdir gulp-services && cd gulp-services && curl -fsSLO 'https://raw.githubusercontent.com/mentat-is/gulp/master/{.env,docker-compose.yml}' && docker compose up -d
```

PyPI and manual development installations may use this Compose stack or
provide the services separately.

## PyPI installation

Requirements: Python 3.12+. The wheel includes a prebuilt UI, so no repository
checkout or frontend build is required. Running it separately with `gulp-webui`
requires Node.js and pnpm.

For example, on Arch Linux:

```bash
sudo pacman -S nodejs pnpm
```

```bash
python3 -m venv .venv && source .venv/bin/activate
# ensure you get 2.x 
pip install mentat-gulp\>=2.0.0
gulp
```

In another terminal, serve the packaged UI:

```bash
gulp-webui
```

Open [http://localhost:3000](http://localhost:3000) for the standalone UI and
[http://localhost:8080/docs](http://localhost:8080/docs) for the API. The
first run creates `~/.config/gulp/gulp_cfg.json`; the default local credentials
are `admin/admin`.

## Development installation

Development uses the repository checkout and runs the backend and UI separately
for live editing. Follow [development installation](install_dev.md), then start
the UI with the [Web UI development guide](webui/install.md).

## Docker installation

Docker keeps the backend and nginx web UI in separate containers. Follow the
[Docker guide](install_docker.md) for the independent `gulp` and `ui` profiles.
