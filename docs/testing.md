# Testing gULP

Install the local SDK, then start a clean development backend when one is not
already available:

~~~bash
.venv/bin/pip install -e ./gulp-sdk
curl -fsS http://localhost:8080/docs >/dev/null || \
  gulp --reset-collab --create test_operation
~~~

Use ports `>= 8100` for additional instances:

~~~bash
GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 gulp
~~~

Stop any backend instance you started when testing finishes:

~~~bash
gulp --stop
~~~

Run the normal suite as needed:

~~~bash
.venv/bin/python -m pytest -v -s tests/
.venv/bin/python -m pytest -v -s tests/integration/
# usually, test_stress.py with BIG_SIGMAS=1 is enough to dictate the overall health of the backend
BIG_SIGMAS=1 .venv/bin/python -m pytest -v -s -x tests/integration/test_stress.py
~~~

The full-suite commands omit pytest's `-x` option so that failures are reported
after the remaining tests have run. Add `-x` when fail-fast behavior is desired.
Tests requiring the dedicated internal-event probe skip during a normal
single-instance run; use the multi-instance launcher below to exercise them.

`tests/integration/test_request_observer.py` skips too, unless its probe
is installed as an extension before the backend starts. The probe exposes
unauthenticated test routes: install it for the test only, and remove it
afterwards.

~~~bash
ln -s "$PWD/tests/fixtures/plugins/request_observer_probe.py" \
  "${GULP_WORKING_DIR:-$HOME/.config/gulp}/plugins/extension/"
# (re)start the backend, then
.venv/bin/python -m pytest -v -s -x tests/integration/test_request_observer.py
rm "${GULP_WORKING_DIR:-$HOME/.config/gulp}/plugins/extension/request_observer_probe.py"
~~~

Use [run_tests.sh](../test_scripts/run_tests.sh) when a wrapper is more
convenient:

~~~bash
./test_scripts/run_tests.sh tests/integration/test_queries.py
~~~

## Concurrent multi-user scenario

`test_concurrent_multi_user_scenario.py` is the core high-concurrency scenario.
Ten authenticated users run against one shared operation by default. They
concurrently exercise raw and EVTX ingestion, enrichment, queries, websocket
routing and message-type filters, large targeted and broadcast client data, and
context/source plus note/link/highlight create-update-delete lifecycles. Every
collaboration event is asserted on every subscribed websocket.

Run it against an already running single instance:

~~~bash
GULP_CONCURRENT_USERS=10 .venv/bin/pytest -v -s \
  tests/integration/test_concurrent_multi_user_scenario.py
~~~

The two-instance launcher requires Prometheus observability to be configured.
Enable the metrics endpoint in `$HOME/.config/gulp/gulp_cfg.json`; the launcher
uses the same configuration for both temporary gULP instances:

~~~json
{
  "prometheus_enabled": true,
  "prometheus_endpoint": "/metrics"
}
~~~

Start Prometheus before the test, using `PROMETHEUS_URL` when it is not exposed
at the default `http://localhost:9090`:

~~~bash
docker compose --profile metrics up -d prometheus
curl -fsS http://localhost:9090/api/v1/targets >/dev/null
~~~

The `gulp` scrape job in [prometheus.yml](../prometheus.yml) must reach both
temporary instances on ports `8080` and `8100`. When running from a devcontainer,
adjust the target host address if the sample address is not the host gateway for
the current environment. See [Observability](./observability.md) for setup and
verification details.

For the complete two-instance scenario, use the launcher. It installs the
integration-only internal-event probe, starts both backends, spreads users
round-robin across them, runs internal-event stress at the same time, and
removes the probe and stops both processes afterward:

~~~bash
tests/integration/run_multi_instance_tests.sh
~~~

Both local ports must be free; the launcher exits before resetting data if
either port is already in use.

Tune load with `GULP_CONCURRENT_USERS`,
`GULP_MULTI_INSTANCE_RAW_INGEST_DOCS`,
`GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT`, and
`GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT`. Optional external services, paid
plugins, and storage backends retain their dedicated integration suites.

## Ingestion pipeline comparison

`ingestion_pipeline_enabled` is a server-only startup setting. To compare the
same workload, run it once with the setting set to `false`, stop gULP, change
the setting to `true`, and restart before the second run. Do not pass a plugin
parameter: clients and the Web UI intentionally cannot override this setting.

Use a unique operation/source for every run so OpenSearch `create` conflicts do
not contaminate timings. Record elapsed time, events/s, worker peak RSS, bulk
request byte/document histograms, retries, and the indexed-document digest.
The candidate is accepted only when the one-million-event median improves by
at least 10% without changing counts, ordering, terminal status, or retry
pressure. The quick live functional check is:

~~~bash
.venv/bin/python -m pytest -v -s -x \
  tests/integration/test_ingestion_pipeline.py
~~~

## Sigma ZIP stress harness

`test_scripts/sigma_zip_big_harness.py` is the end-to-end stress harness. It
creates users and operations, ingests Windows EVTX data, runs BIG_SIGMAS through
the paid `query_sigma_zip` plugin, creates matching notes, and checks the final
counts. Use `--help` for every option.

> This is deliberately unrealistic: ten users concurrently submit the same large workflow (hundreds of concurrent ingestion mixed with thousands of queries, millions of generated notes). It is a safety and capacity test, not a production sizing recommendation. for normal production stress test, just run test_stress.py.

Start from a clean backend for a comparable run:

~~~bash
gulp --reset-collab --create test_operation --log-level info
~~~

Run the two reference profiles from another terminal.

One operation per user exercises independent ingestion, queries, and notes:

~~~bash
.venv/bin/python test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --instances 1 \
  --concurrency 10 \
  --allow-heavy-query-run
~~~

The shared-operation profile puts every user on one operation and deliberately
amplifies collaboration-note contention:

~~~bash
.venv/bin/python test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --instances 1 \
  --concurrency 10 \
  --shared-operation \
  --allow-heavy-query-run
~~~

Measured cold-cache reference results on the local single-instance development
environment are below. Counts are `records / completed queries / hits / notes`.

| Profile | Elapsed | Result |
| --- | ---: | --- |
| One operation per user | 969.50s | PASS `986,330 / 11,490 / 734,640 / 734,640` |
| Shared operation | 1770.51s | PASS `986,330 / 11,490 / 7,346,400 / 7,346,400` |

The shared profile has ten times the hits and notes, so compare it only with
other shared-operation runs. The harness cleans up on success, but deletion of
millions of shared notes can exceed PostgreSQL lock timeouts. If that happens,
stop the backend and reset the development collaboration database rather than
leaving competing cleanup requests running.

Watch the backend, OpenSearch, PostgreSQL, and Redis while the harness runs:

~~~bash
sudo docker stats --no-stream gulp-os01-1 gulp-postgres-1 gulp-redis-1
sudo docker exec gulp-os01-1 curl -fsS \
  'http://localhost:9200/_cat/nodes?h=heap.percent,heap.max,cpu,load_1m,node.role,name'
~~~

Additional gULP instances or OpenSearch/PostgreSQL nodes can be added for an
environment-specific experiment. Point the harness at the added backend URLs
with `--instance-url`, then retune and measure the whole deployment; the local
reference results above do not predict a different topology.

To verify dedicated task consumers, run the role-split integration test. It
starts an ingest-only node and a query-only node, then completes the small Sigma
ZIP ingest/query flow through both. The runner sets `GULP_INSTANCE_ROLES` for
each temporary node:

~~~bash
tests/integration/run_multi_instance_tests.sh --role-split-sigma
~~~

Stop a backend started for testing when finished:

~~~bash
gulp --stop
~~~

## Related tools

- [Ingestion tool](./integration.md)
- [External query tool](./query.md)
- [Observability](./observability.md)
