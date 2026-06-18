- [enable observability](#enable-observability)
  - [1) Enable Prometheus metrics in gULP](#1-enable-prometheus-metrics-in-gulp)
  - [2) Run Prometheus (i.e. via Docker Compose)](#2-run-prometheus-ie-via-docker-compose)
  - [3) Confirm Prometheus is scraping gULP](#3-confirm-prometheus-is-scraping-gulp)
  - [4) Customizing scrape targets](#4-customizing-scrape-targets)
  - [5) Backend runbook](#5-backend-runbook)
  - [6) Visualize metrics in Grafana](#6-visualize-metrics-in-grafana)
    - [Add Prometheus as a data source](#add-prometheus-as-a-data-source)
    - [Import the sample dashboard](#import-the-sample-dashboard)

# enable observability

gULP can expose Prometheus `/metrics` via an HTTP endpoint. This lets you scrape gULP from a Prometheus server and build dashboards/alerts in Grafana.

## 1) Enable Prometheus metrics in gULP

In your `gulp_cfg.json` (usually in `$HOME/.config/gulp/gulp_cfg.json` or in the directory mounted into the container), enable the Prometheus endpoint:

~~~json
{
  "prometheus_enabled": true,
  "prometheus_endpoint": "/metrics",
  "prometheus_collect_interval": 15
}
~~~

- `prometheus_enabled`: enables the `/metrics` endpoint (default: `false`).
- `prometheus_collect_interval`: how often gULP collects gauge metrics (default: 15s).

Restart gULP after changing the config.

## 2) Run Prometheus (i.e. via Docker Compose)

The sample [docker-compose.yml](../docker-compose.yml) includes a `prometheus` service (profile `metrics`) and a default scrape config in [prometheus.yml](../prometheus.yml) is also present in the repository.
Backend alerting rules are defined in [prometheus_alerts.yml](../prometheus_alerts.yml)
and mounted by the same Docker Compose profile.

Start Prometheus with:

~~~bash
# run Prometheus (assumes gULP is reachable on host ports 8080 and 8100)
docker compose --profile metrics up -d prometheus
~~~

To bring up the full stack including Prometheus (and Grafana), run:

~~~bash
docker compose --profile metrics up -d
~~~

## 3) Confirm Prometheus is scraping gULP

Open the Prometheus UI at `http://localhost:9090/targets` and verify the `gulp` targets are `UP`.

You can also query metrics directly in Prometheus, e.g.:

- `gulp_ws_connected_sockets`
- `gulp_main_process_cpu_percent`
- `gulp_worker_cpu_percent`

## 4) Customizing scrape targets

The example [prometheus.yml](../prometheus.yml) scrapes gULP at `host.docker.internal:8080` and `host.docker.internal:8100`, and uses the default `/metrics` path. This covers the default ports used by [run_multi_instance_tests.sh](../tests/integration/run_multi_instance_tests.sh).

If you run gULP on a different host/port (or inside the same Docker network), update [prometheus.yml](../prometheus.yml) accordingly.
If gULP runs inside a devcontainer while Prometheus runs as a sibling Docker
container, `host.docker.internal` may point at the Docker host instead of the
devcontainer. In that case use the devcontainer IP shown by `hostname -I`, for
example:

~~~yaml
static_configs:
  - targets:
      - "192.168.65.3:8080"
      - "192.168.65.3:8100"
~~~

Restart Prometheus after changing scrape targets:

~~~bash
docker compose --profile metrics restart prometheus
~~~

> Note: When running Prometheus inside Docker, `host.docker.internal` points to the Docker host (where gULP is exposed on the host port). If Prometheus runs on the same Docker network as gULP, you can also use service names such as `gulp:8080`.

## 5) Backend runbook

Prometheus provides the backend metrics, while runtime logs remain the existing
`MutyLogger` stdout/syslog stream. For queue pressure, failed tasks, websocket
pressure, pub/sub pointer failures, and recommended backend config profiles,
see [production_runbook.md](./production_runbook.md).

## 6) Visualize metrics in Grafana

Grafana is a common frontend for Prometheus metrics.

The default [docker-compose.yml](../docker-compose.yml) also includes a `grafana` service under the `metrics` profile, so visit `http://localhost:3030` and log in with the default Grafana credentials (`admin` / `admin`).

### Add Prometheus as a data source

In Grafana:

1. Go to **Connections > Add new connection**
2. select **Prometheus** and click **Add data source**
3. Set the URL to `http://host.docker.internal:9090` (or `http://localhost:9090` if running Grafana outside Docker)
4. Click **Save & test**

### Import the sample dashboard

This repository includes a sample dashboard at
[grafana/gulp-comprehensive-observability.json](./grafana/gulp-comprehensive-observability.json).
It covers:

- gULP target health, uptime, live servers, workers, and websocket sessions
- Prometheus target health and scrape behavior per instance
- API request rate and latency
- Redis task queue depth, pending work, task transitions, and queue age
- PostgreSQL-backed active, stale, and recently finished request counts
- task admission rejections and websocket/pub-sub backpressure
- Redis pub/sub throughput and websocket queue pressure
- Redis pub/sub bytes, subscriber counts, reconnects, subscriber errors, targeted publishes, and token bucket pressure
- websocket connections by type, enqueue timeouts, broadcast dedup drops, and payload pointer resolution outcomes
- OpenSearch bulk ingest throughput
- OpenSearch bulk errors and skipped documents
- PostgreSQL pool usage
- main process, worker process, worker dispatch, and host resource usage

To import it through the Grafana UI:

1. Open `http://localhost:3030/dashboards`.
2. Click **New > Import**.
3. Upload `docs/grafana/gulp-comprehensive-observability.json`.
4. Select the Prometheus data source that scrapes gULP.
5. Click **Import**.

You can also import it with the Grafana API:

~~~bash
python3 - <<'PY' | curl -u admin:admin \
  -H 'Content-Type: application/json' \
  -X POST http://localhost:3030/api/dashboards/import \
  -d @-
import json

with open("docs/grafana/gulp-comprehensive-observability.json", encoding="utf-8") as f:
    dashboard = json.load(f)

print(json.dumps({
    "dashboard": dashboard,
    "folderUid": "gulp",
    "overwrite": True,
    "inputs": [
        {
            "name": "DS_PROMETHEUS",
            "type": "datasource",
            "pluginId": "prometheus",
            "value": "prometheus"
        }
    ]
}))
PY
~~~

The dashboard expects Prometheus to scrape the gULP `/metrics` endpoint. With
the repository Docker Compose setup, the data source URL is typically
`http://host.docker.internal:9090`.
