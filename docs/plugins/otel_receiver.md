# OpenTelemetry ingest plugin for Gulp

This plugin implements an extension for Gulp that receives OpenTelemetry (OTLP) payloads over HTTP POST, converts them into standardized Gulp documents, and enqueues them for ingestion. The plugin handles timestamp extraction, context mapping, and source identification.

## Overview

- Receives OTLP logs, metrics, and traces via HTTP endpoints.
- Flattens OTLP resource and record attributes into simple dictionaries.
- Spawns background ingestion tasks to persist records into a Gulp operation.
- Supports both plain JSON and gzipped JSON payloads.

## Endpoints

- `POST /v1/logs` — expects `resourceLogs` in the OTLP payload.
- `POST /v1/metrics` — expects `resourceMetrics` in the OTLP payload.
- `POST /v1/traces` — expects `resourceSpans` in the OTLP payload.

## Authentication

Two authentication methods are supported:

- `token` header (token-style) — token returned from login endpoint.
- HTTP Basic auth — validated and mapped to a user session.

## Required headers

- `Operation-ID`: operation identifier used to route/associate ingestion.
- `content-encoding`: set to `json` for plain JSON or leave unset/use gzip for compressed payloads.

Other headers (e.g., proxy or network headers) depend on your environment.

## Mapping

The plugin uses a mapping file (default `otel.json`) to translate flattened fields into the internal document model. Update or provide a custom mapping file to control how fields are mapped into `GulpDocument`.

### Open Telemtry Metrics

For Metric open telemetry logs gulp grouped into a single document  the datapoints that share the same timestamp/context/source/scope.
This grouping reduces the number of documents by aggregating metrics that belong together in time and context.
Each grouped document contains a `metrics` list with per-datapoint records (with `metric_name`, `metric_type`, value fields, etc.).

## Context and source extraction

The plugin attempts to determine a context name and a source name from the OTLP payload. These fallbacks are applied in order:

- Context (resource attributes): `gulp.context`, `host.name`, `k8s.pod.name`, `host.id`, otherwise `unknown_context`.

- Source (depending on signal):
  - Logs: `gulp.source`, `log.file.path`, `log.file.name`, `systemd.unit`, `syslog.identifier`, `container.name`, `service.name`, `SYSLOG_IDENTIFIER`, otherwise `unknown_log_source`.
  - Metrics: `gulp.source`, `service.name`, otherwise `system_metrics`.
  - Spans: `gulp.source`, `db.system`, `messaging.system`, `service.name`, otherwise `unknown_trace_source`.

These defaults can be overridden in the collector using processors that insert `gulp.context` or `gulp.source` attributes before export.

## OTEL Collector configuration (example placeholder)

Configure your OTEL Collector to export OTLP HTTP (otlphttp) to the Gulp endpoints. The example below is a placeholder — complete it with real hostnames, ports, and authentication details for your environment.

```yaml
# Example extensions configuration (fill in values for your environment)
extensions:
  basicauth/exporters: # needes to authenticate to gulp server
    client_auth:
      username: admin #gulp username
      password: admin #gulp password

# Example exporter configuration (fill in values for your environment)
exporters:
  otlphttp/gulp:
    endpoint: "http://<GULP_HOST>:<GULP_PORT>"
     tls:
      insecure: true #false is gulp is under tls
    encoding: json # or gzip
    auth:
      authenticator: basicauth/exporters
    headers:
      Operation-ID: "<YOUR_OPERATION_ID>"

# Example processor to ovveride gulp context and source processing
processors:
  resource/hostmetrics_tags:
    attributes:
    - key: gulp.source
      value: "system metrics" # source name
      action: insert
    - key: gulp.context
      value: "ubuntu server" # context name
      action: insert

# Add the exporter to the appropriate pipelines (logs/metrics/traces) and configure
# any processors you need, e.g. resource detection or attribute insertion.
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [resourcedetection, batch]
      exporters: [otlphttp/gulp]

    metrics:
      receivers: [hostmetrics]
      processors: [batch]
      exporters: [otlphttp/gulp]

    logs:
      receivers: [journald, filelog/docker, otlp]
      processors: [resourcedetection, batch]
      exporters: [otlphttp/gulp]
  extensions: [... , basicauth/exporters] # add basic auth extension
```

## Troubleshooting

- If ingestion fails, review Gulp server logs for errors, JSend exceptions, or authentication failures.
- Verify that the `Operation-ID` header matches an existing operation or is a valid token to create one.
- Confirm whether the collector sends gzipped payloads and set the `content-encoding` header accordingly.
