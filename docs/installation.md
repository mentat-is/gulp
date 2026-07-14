# Installation

Choose the installation mode that fits your use case:

1. **Development:** use a repository checkout and run backend and UI tools
   manually.
2. **PyPI:** install `mentat-gulp`; `gulp` serves both the backend and bundled
   UI.
3. **Docker:** run the backend and UI in separate containers.

## PyPI installation

Requirements: Python 3.12+ and configured OpenSearch, PostgreSQL, Redis, and
optional MinIO services. The wheel includes a prebuilt UI, so Node.js, pnpm,
and a repository checkout are not required.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install mentat-gulp>=2.0.0
gulp
```

Open [http://localhost:8080](http://localhost:8080) for the UI and
[http://localhost:8080/docs](http://localhost:8080/docs) for the API. The
first run creates `~/.config/gulp/gulp_cfg.json`; the default local credentials
are `admin/admin`.

## Development installation

Development uses the repository checkout and runs the backend and UI separately
for live editing. Follow [development installation](install_dev.md), then start
the UI with the [Web UI development guide](webui/install.md).

## Docker installation

Docker runs the backend with its UI in one container by default. Follow the
[Docker guide](install_docker.md) for the `gulp` profile or the separate
`distributed` profile.
