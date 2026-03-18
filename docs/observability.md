- [enable observability](#enable-observability)
  - [1) Enable Prometheus metrics in gULP](#1-enable-prometheus-metrics-in-gulp)
  - [2) Run Prometheus (i.e. via Docker Compose)](#2-run-prometheus-ie-via-docker-compose)
  - [3) Confirm Prometheus is scraping gULP](#3-confirm-prometheus-is-scraping-gulp)
  - [4) Customizing scrape targets](#4-customizing-scrape-targets)
  - [5) Visualize metrics in Grafana](#5-visualize-metrics-in-grafana)
    - [Add Prometheus as a data source](#add-prometheus-as-a-data-source)
    - [Build dashboards](#build-dashboards)

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

Start Prometheus with:

~~~bash
# run Prometheus (assumes gULP is reachable on host port 8080)
docker compose --profile metrics up -d prometheus
~~~

To bring up the full stack including Prometheus (and Grafana), run:

~~~bash
docker compose --profile metrics up -d
~~~

## 3) Confirm Prometheus is scraping gULP

Open the Prometheus UI at `http://localhost:9090/targets` and verify the `gulp` target is `UP`.

You can also query metrics directly in Prometheus, e.g.:

- `gulp_ws_connected_sockets`
- `gulp_main_process_cpu_percent`
- `gulp_worker_cpu_percent`

## 4) Customizing scrape targets

The example [prometheus.yml](../prometheus.yml) scrapes gULP at `host.docker.internal:8080` and uses the default `/metrics` path.

If you run gULP on a different host/port (or inside the same Docker network), update [prometheus.yml](../prometheus.yml) accordingly.

> Note: When running Prometheus inside Docker, `host.docker.internal` points to the Docker host (where gULP is exposed on the host port). If Prometheus runs on the same Docker network as gULP, you can also use `gulp:8080`.

## 5) Visualize metrics in Grafana

Grafana is a common frontend for Prometheus metrics.

The default [docker-compose.yml](../docker-compose.yml) also includes a `grafana` service under the `metrics` profile, so visit `http://localhost:3000` and log in with the default Grafana credentials (`admin` / `admin`).

### Add Prometheus as a data source

In Grafana:

1. Go to **Connections > Add new connection**
2. select **Prometheus** and click **Add data source**
3. Set the URL to `http://host.docker.internal:9090` (or `http://localhost:9090` if running Grafana outside Docker)
4. Click **Save & test**

### Build dashboards

You can now build dashboards using gULP metrics such as `gulp_ws_connected_sockets`, `gulp_main_process_cpu_percent`, `gulp_worker_cpu_percent`, and any other `gulp_*` metrics exposed by `gULP`.
