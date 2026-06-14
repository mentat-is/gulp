# backend production runbook

This runbook covers backend operations for a horizontally scaled gULP
deployment. It focuses on task queues, websocket routing, raw websocket ingest,
Prometheus metrics, and the existing `MutyLogger` log stream.

## service checks

Use these checks before deeper triage:

~~~bash
curl -fsS http://localhost:8080/docs >/dev/null && echo up || echo down
curl -fsS http://localhost:8080/metrics | rg '^gulp_'
curl -fsS http://localhost:9090/api/v1/targets
~~~

`/metrics` is available only when Prometheus support is enabled in
`~/.config/gulp/gulp_cfg.json`:

~~~json
{
  "prometheus_enabled": true,
  "prometheus_endpoint": "/metrics",
  "prometheus_collect_interval": 15
}
~~~

Start the repository-provided Prometheus service with:

~~~bash
docker compose --profile metrics up -d prometheus
~~~

The same Prometheus profile mounts [prometheus_alerts.yml](../prometheus_alerts.yml)
as `/etc/prometheus/rules/gulp_alerts.yml`. Those rules encode the backend
alert starting points below and should be tuned against production traffic.

See [observability.md](./observability.md) for the full Prometheus setup.

## logs

Backend logs are the existing `MutyLogger` output. Depending on deployment
configuration, operators should collect the process stdout/stderr stream or the
syslog destination configured for gULP. Do not add a second backend logging
transport for task lifecycle events; add any new lifecycle messages through
`MutyLogger` at the same call sites that own the state transition.

Useful log patterns during incidents:

- `queue full`, `TaskQueueFullError`, or HTTP `503`: admission pressure.
- `dead-letter`: valid task exhausted retries or malformed task payload.
- `duplicate_running`: a duplicate delivery was suppressed by lifecycle state.
- `side-effect lease`: a reclaimed task was kept out of handler side effects.
- `autoclaim`: Redis stream pending entry recovery path.
- `payload pointer`: Redis pub/sub large-payload pointer resolution.
- `queue full for ws` or websocket enqueue timeout: client is not draining.
- `ws ingest`: raw websocket ingest stream creation, pressure, or cleanup.

## task queue triage

Primary metrics:

- `gulp_redis_task_stream_depth{task_type}`
- `gulp_redis_task_stream_pending{task_type}`
- `gulp_redis_task_oldest_queued_age_seconds{task_type}`
- `gulp_redis_task_oldest_pending_age_seconds{task_type}`
- `gulp_redis_task_running{server_id,task_type}`
- `gulp_redis_task_dead_letter_depth{task_type}`
- `gulp_redis_task_delayed_retries`
- `gulp_redis_task_recovered_retries{task_type}`
- `gulp_redis_task_active_reservations{scope}`
- `gulp_redis_task_transition_total{action,task_type,outcome}`
- `gulp_redis_task_execution_duration_seconds{task_type,outcome}`
- `gulp_api_request_rejected_total{endpoint,reason,task_type,scope}`

Quick Prometheus queries:

~~~bash
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=gulp_redis_task_oldest_queued_age_seconds'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=gulp_redis_task_oldest_pending_age_seconds'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(gulp_api_request_rejected_total[5m])'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=gulp_redis_task_dead_letter_depth'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=gulp_redis_task_recovered_retries'
~~~

Triage rules:

- High queued depth with low running count usually means not enough eligible
  workers, too narrow `instance_roles`, or workers cannot reach dependencies.
- High pending age with live running owners usually means long-running work; use
  task duration histograms before assuming a stuck task.
- High pending age with no corresponding live running owner points to reclaim or
  worker failure; inspect `autoclaim`, `duplicate_running`, and side-effect lease
  logs.
- Dead-letter growth means a valid task exhausted retries or invalid payloads
  are being produced; inspect the request stats row for the affected `req_id`
  and the `MutyLogger` exception at the first failure.
- Recovered retries mean tasks eventually succeeded but only after at least one
  failed attempt. Treat this as a warning signal during load tests: inspect
  worker exceptions and lifecycle metadata before calling the run clean.
- API rejections with `scope="task_type"` mean the task-type stream reached
  `redis_stream_task_maxlen`.
- API rejections with `scope="user"` or `scope="operation"` mean active fairness
  limits are working; clients should honor `retry_after_msec`.

## websocket triage

Primary metrics:

- `gulp_ws_connected_sockets`
- `gulp_ws_connected_sockets_by_type{server_id,socket_type}`
- `gulp_ws_queue_utilization`
- `gulp_ws_queue_high_watermark{server_id,socket_type}`
- `gulp_ws_enqueue_timeout_total{server_id,socket_type}`
- `gulp_redis_publish_by_route_total{route_target,channel_scope}`
- `gulp_redis_publish_bytes_by_route_total{route_target,channel_scope}`
- `gulp_ws_payload_pointer_resolve_total{outcome}`
- `gulp_redis_subscriber_errors_total`
- `gulp_redis_pubsub_reconnect_total`

Quick Prometheus queries:

~~~bash
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=gulp_ws_queue_high_watermark'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(gulp_ws_enqueue_timeout_total[5m])'
curl -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(gulp_ws_payload_pointer_resolve_total[5m])'
~~~

Triage rules:

- Enqueue timeouts mean clients are not draining fast enough; lower document
  chunk size or increase client read throughput before increasing queue limits.
- Large payload pointer `missing` or `error` outcomes mean Redis pointer storage
  expired or failed before all intended instances decoded the payload.
- Pub/sub reconnects or subscriber errors indicate Redis connectivity or
  callback exceptions; inspect `MutyLogger` around the first reconnect.
- Sequence gaps are expected to be detectable, not fully replayed by pub/sub.
  Clients should recover final request state through request stats/collab APIs
  for recoverable event types.

## raw websocket ingest triage

Primary knobs:

- `ws_ingest_stream_max_entries`
- `ws_ingest_stream_max_buffered_bytes`
- `ingestion_documents_chunk_size`
- `ingestion_documents_adaptive_chunk_size`

Triage rules:

- Backpressure on raw ingest is expected when a websocket stream exceeds entry
  or buffered-byte limits.
- Disconnect before the final `last=true` packet should mark request stats as
  failed rather than leaving a pending request.
- Owner-heartbeat cleanup should remove orphan raw ingest stream keys after the
  owning backend instance is no longer live.

## PostgreSQL collaboration triage

Primary metrics:

- `gulp_postgres_pool_size`
- `gulp_postgres_pool_checked_out`
- `gulp_postgres_pool_idle`
- `gulp_postgres_pool_overflow`

Primary knobs:

- `postgres_adaptive_pool_size`
- `postgres_pool_size`
- `postgres_max_overflow`
- `postgres_pool_timeout_sec`
- `postgres_pool_recycle_sec`
- `postgres_lock_timeout_ms`
- `postgres_statement_timeout_ms`
- `postgres_idle_in_transaction_session_timeout_ms`
- `postgres_user_session_touch_interval_ms`

Connection pools are per process. Budget the maximum PostgreSQL connections as:

~~~text
per_process_connections = postgres_pool_size + postgres_max_overflow
total_possible_connections =
  gulp_instances * processes_per_instance * per_process_connections
~~~

`processes_per_instance` is the main process plus configured worker processes.
With the default PostgreSQL settings, each process may open up to
`3 + 2 = 5` PostgreSQL connections. A single instance with four workers can
therefore use up to `1 * 5 * 5 = 25` PostgreSQL connections before any other
gULP instance is counted.

Triage rules:

- High checked-out connections with low idle connections means requests are
  waiting for the pool or holding transactions too long. Inspect request
  latency and logs for pool checkout timeouts before increasing pool caps.
- Lock timeout failures usually mean a hot collaboration row or request-stats
  row is serializing work. Reduce transaction scope and keep Redis,
  websocket, and OpenSearch work outside PostgreSQL transactions before raising
  `postgres_lock_timeout_ms`.
- Repeated writes to the same `user_session` row during read-heavy traffic can
  be reduced with `postgres_user_session_touch_interval_ms`. The default keeps
  a sliding expiration window while avoiding a commit on every authenticated
  call.
- For multi-instance deployments, reduce `postgres_pool_size`,
  `postgres_max_overflow`, `parallel_processes_max`, or instance count before
  raising per-process pool caps.

## config profiles

Profiles are starting points. Tune against real OpenSearch, PostgreSQL, Redis,
and client websocket behavior.

### small controlled deployment

Use this for development, demos, or a small single-tenant backend:

~~~json
{
  "parallel_processes_max": 1,
  "concurrency_adaptive_num_tasks": true,
  "concurrency_opensearch_num_nodes": 1,
  "concurrency_postgres_num_nodes": 1,
  "concurrency_tasks_cap_per_process": 16,
  "postgres_adaptive_pool_size": true,
  "postgres_pool_size": 3,
  "postgres_max_overflow": 2,
  "postgres_pool_timeout_sec": 10,
  "postgres_pool_recycle_sec": 3600,
  "postgres_lock_timeout_ms": 5000,
  "postgres_statement_timeout_ms": 0,
  "postgres_idle_in_transaction_session_timeout_ms": 30000,
  "postgres_user_session_touch_interval_ms": 60000,
  "redis_stream_task_maxlen": 1000,
  "redis_task_active_user_max": 0,
  "redis_task_active_operation_max": 0,
  "ws_queue_max_size": 4096,
  "ws_enqueue_timeout": 5.0,
  "ws_ingest_stream_max_entries": 1000,
  "ws_ingest_stream_max_buffered_bytes": 134217728,
  "prometheus_enabled": true
}
~~~

### medium multi-instance deployment

Use this when running multiple gULP instances against shared Redis,
PostgreSQL, and OpenSearch:

~~~json
{
  "parallel_processes_max": 0,
  "concurrency_adaptive_num_tasks": true,
  "concurrency_opensearch_num_nodes": 3,
  "concurrency_postgres_num_nodes": 1,
  "concurrency_tasks_cap_per_process": 32,
  "postgres_adaptive_pool_size": true,
  "postgres_pool_size": 3,
  "postgres_max_overflow": 2,
  "postgres_pool_timeout_sec": 10,
  "postgres_pool_recycle_sec": 3600,
  "postgres_lock_timeout_ms": 5000,
  "postgres_statement_timeout_ms": 0,
  "postgres_idle_in_transaction_session_timeout_ms": 30000,
  "postgres_user_session_touch_interval_ms": 60000,
  "redis_stream_task_maxlen": 10000,
  "redis_task_active_user_max": 50,
  "redis_task_active_operation_max": 100,
  "redis_task_autoclaim_idle_ms": 60000,
  "redis_task_lease_refresh_interval_ms": 20000,
  "redis_task_max_attempts": 3,
  "ws_queue_max_size": 8192,
  "ws_enqueue_timeout": 5.0,
  "prometheus_enabled": true
}
~~~

### high-volume deployment

Use this only with measured backend capacity and active alerting:

~~~json
{
  "parallel_processes_max": 0,
  "concurrency_adaptive_num_tasks": true,
  "concurrency_opensearch_num_nodes": 6,
  "concurrency_postgres_num_nodes": 2,
  "concurrency_tasks_cap_per_process": 64,
  "postgres_adaptive_pool_size": true,
  "postgres_pool_size": 3,
  "postgres_max_overflow": 2,
  "postgres_pool_timeout_sec": 10,
  "postgres_pool_recycle_sec": 3600,
  "postgres_lock_timeout_ms": 5000,
  "postgres_statement_timeout_ms": 0,
  "postgres_idle_in_transaction_session_timeout_ms": 30000,
  "postgres_user_session_touch_interval_ms": 60000,
  "redis_stream_task_maxlen": 50000,
  "redis_task_active_user_max": 100,
  "redis_task_active_operation_max": 250,
  "redis_task_autoclaim_idle_ms": 120000,
  "redis_task_lease_refresh_interval_ms": 30000,
  "redis_task_retry_backoff_base_ms": 1000,
  "redis_task_retry_backoff_max_ms": 60000,
  "redis_task_lifecycle_ttl_sec": 86400,
  "ws_queue_max_size": 16384,
  "ws_enqueue_timeout": 10.0,
  "ws_ingest_stream_max_entries": 5000,
  "ws_ingest_stream_max_buffered_bytes": 536870912,
  "prometheus_enabled": true
}
~~~

For role-specialized instances, set `instance_roles` explicitly. Example:

~~~json
{
  "instance_roles": ["ingest", "query"]
}
~~~

An empty or unset `instance_roles` list means the instance may process all known
task types.

## alert starting points

Use these as initial alert conditions, then tune thresholds from production
traffic:

- `gulp_redis_task_oldest_queued_age_seconds > 300` for 10 minutes.
- `gulp_redis_task_oldest_pending_age_seconds > 900` with no matching live
  `gulp_redis_task_running` owner.
- `gulp_redis_task_dead_letter_depth > 0`.
- `rate(gulp_api_request_rejected_total[5m]) > 0` outside expected load tests.
- `rate(gulp_ws_enqueue_timeout_total[5m]) > 0`.
- `rate(gulp_ws_payload_pointer_resolve_total{outcome=~"missing|error"}[5m]) > 0`.
- `rate(gulp_redis_subscriber_errors_total[5m]) > 0`.

## safe operational actions

- Scale out role-compatible backend instances when queued age rises and
  dependencies have spare capacity.
- Reduce `parallel_processes_max`, `concurrency_tasks_cap_per_process`, or
  request chunk sizes when OpenSearch/PostgreSQL are saturated.
- Increase `redis_stream_task_maxlen` only after verifying Redis memory headroom.
- Increase websocket queue sizes only after verifying clients can eventually
  drain; otherwise it only moves memory pressure into the backend.
- Do not use `gulp --reset-collab` on production data. That command is for
  development and integration-test backends only.
