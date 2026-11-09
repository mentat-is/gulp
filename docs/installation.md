# Installation

Choose the installation mode that fits your use case:

1. **Development:** use a repository checkout and run backend and UI tools
   manually.
2. **Docker:** run the backend and UI in separate containers.
3. **Enterprise:** put private backend instances behind a TLS-terminating proxy.

## Required services

Every installation method requires running OpenSearch, PostgreSQL, and Redis
services. Start them before the gULP backend and keep them running. SeaweedFS is
also required when using features or plugins that need object storage.

For a manual development installation, start these services with the
[Docker Compose file](../docker-compose.yml) provided in the repository:

```bash
docker compose up -d
```

The [Docker installation guide](install_docker.md) covers the full container
deployment, including service URLs and the independent web UI.

## Development installation

Development uses the repository checkout and runs the backend and UI separately
for live editing. Follow [development installation](install_dev.md), then start
the UI with the [Web UI development guide](webui/install.md).

## Docker installation

Docker keeps the backend and nginx web UI in separate containers. Follow the
[Docker guide](install_docker.md) for the local `gulp` and `ui` profiles or the
standalone Compose file that supplies container service URLs to gULP.

## Enterprise deployment

For public HTTPS/WSS, multiple backend instances, or optional mutual TLS, follow
the [enterprise nginx deployment guide](enterprise_deployment.md). gULP itself
stays on private HTTP port 8080 behind the proxy.
