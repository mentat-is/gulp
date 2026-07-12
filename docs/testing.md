- [testing gulp](#testing-gulp)
  - [prerequisites](#prerequisites)
  - [normal test runs](#normal-test-runs)
  - [sigma-zip load harness](#sigma-zip-load-harness)
    - [options](#options)
    - [examples](#examples)
    - [backend tuning](#backend-tuning)
    - [measured single-node profile](#measured-single-node-profile)
    - [local OpenSearch cluster probe](#local-opensearch-cluster-probe)
  - [ingestion tool](#ingestion-tool)
  - [query external tool](#query-external-tool)
  - [observability](#observability)

# testing gulp

## prerequisites

Ensure [gulp api client sdk](https://github.com/mentat-is/gulp-sdk) is installed:

~~~bash
pip3 install -e ./gulp-sdk
~~~

Start gULP first. If a backend is already listening on `:8080`, reuse it:

~~~bash
curl -fsS http://localhost:8080/docs >/dev/null && echo up || \
  gulp --reset-collab --create test_operation
~~~

## normal test runs

Tests are located in the [tests](../tests) folder and can be run independently.

> [stress_test](../tests/integration/test_stress.py) is a test that runs a subset of critical tests in sequence: it is useful to verify if the main functionalities are working as expected.

~~~bash
# run all
python3 -m pytest -v -s -x ./tests/

# run all with coverage
pytest -v -s -x --cov=gulp_sdk tests/

# run integration tests only
python3 -m pytest -v -s -x ./tests/integration

# run stress tests only (NOTE: the sigma-zip part needs the non-free query_sigma_zip plugin)
python3 -m pytest -v -s -x ./tests/integration/test_stress.py

# when Prometheus is enabled, stress tests also assert critical /metrics families
# are exported after load; tune the scrape wait if collection is slower
GULP_STRESS_METRICS_TIMEOUT=60 python3 -m pytest -v -s -x ./tests/integration/test_stress.py::test_concurrent_ingest_and_query

# run two-instance websocket/pub-sub routing checks; the script starts and stops
# backends on :8080 and :8100 and drives both instances concurrently
./tests/integration/run_multi_instance_tests.sh

# run the same two-instance checks as a visible large-payload fanout soak
# (defaults to 20 pointer iterations, 500 raw docs per instance, and one large-note collab iteration)
GULP_MULTI_INSTANCE_SOAK=1 ./tests/integration/run_multi_instance_tests.sh

# suppress live backend logs while keeping log files in the final summary
QUIET=1 GULP_MULTI_INSTANCE_SOAK=1 ./tests/integration/run_multi_instance_tests.sh

# tune the large-payload fanout manually for heavier pre-release soak runs
GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT=100 \
GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS=600 \
GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT=1 \
GULP_MULTI_INSTANCE_RAW_INGEST_DOCS=1000 \
./tests/integration/run_multi_instance_tests.sh

# run a specific test, use full windows sigma rules set
BIG_SIGMAS=1 python3 -m pytest -v -s -x ./tests/integration/test_stress.py::test_concurrent_ingest_and_query_same_operation

# run the BIG_SIGMAS sigma-zip harness as created users
# (requires the non-free query_sigma_zip plugin)
./test_scripts/sigma_zip_big_harness.py --users 3 --allow-heavy-query-run
~~~

or use the provided [run_tests.sh](../test_scripts/run_tests.sh) script to run all tests automatically (or a subset of them)

~~~bash
# run all tests
./test_scripts/run_tests.sh ./tests
# run only specific tests
./test_scripts/run_tests.sh ./tests/integration/test_queries.py ./tests/integration/test_ingest.py::test_ingest_win_evtx_sample
~~~

## sigma-zip load harness

`test_scripts/sigma_zip_big_harness.py` drives the full Windows EVTX ingest plus
`query_sigma_zip` flow as multiple real users. It is intended for UI-visible
load testing and backend capacity checks, not as a quick unit test.

The harness creates deterministic users by default:

- `user1` / `user1`
- `user2` / `user2`
- `userN` / `userN`

For non-query-only runs, operations are deterministic too:

- one operation per user: `op1`, `op2`, `opN`
- shared-operation mode: all users use `op1`

Created operations are explicitly granted to `guest`, `admin`, and the relevant
harness users, so they can be opened from the UI while the harness is running.
The harness prints a `targets` line listing each worker, user, password,
operation, and URL.

The harness also prints:

- a `gulp_config` line at startup with concurrency/parallelism settings read
  from `gulp_cfg.json`
- periodic `live` aggregate counts while workers are still running
- phase progress lines as workers complete
- final start/end timestamps, elapsed time, totals, expected totals, and one
  compact worker summary per user

### options

| option | env var | meaning |
| --- | --- | --- |
| `--base-url` | `GULP_BASE_URL` | Primary backend URL. Default: `http://localhost:8080`. |
| `--instances` | `GULP_SIGMA_ZIP_INSTANCES` | Number of backend instances to spread workers across. Default: `1`. |
| `--instance-url` | `GULP_SIGMA_ZIP_INSTANCE_URLS` | Extra backend URL. Repeat on CLI or pass comma-separated env values. |
| `--admin-user` | `GULP_TEST_USER` | Admin user used for setup and cleanup. Default: `admin`. |
| `--admin-password` | `GULP_TEST_PASSWORD` | Admin password. Default: `admin`. |
| `--users` | `GULP_SIGMA_ZIP_USERS` | Number of users/workers. Default: `10`. |
| `--concurrency` | `GULP_SIGMA_ZIP_CONCURRENCY` | Max harness worker flows running at once. `0` means all users. |
| `--worker-password` | `GULP_SIGMA_ZIP_USER_PASSWORD` | Override password for all created users. Empty default means `userN/userN`. |
| `--small-sigmas` | `GULP_SIGMA_ZIP_SMALL` | Use the small sigma zip after full EVTX ingest. Useful for smoke tests. |
| `--allow-heavy-query-run` | `GULP_SIGMA_ZIP_ALLOW_HEAVY` | Required for multi-user full BIG_SIGMAS runs. |
| `--query-only` | `GULP_SIGMA_ZIP_QUERY_ONLY` | Reuse existing operations and run only the sigma query phase. |
| `--shared-operation` | `GULP_SIGMA_ZIP_SHARED_OPERATION` | Put all users on one operation. Ingest uses per-user contexts. |
| `--skip-note-ws-count` | `GULP_SIGMA_ZIP_SKIP_NOTE_WS_COUNT` | Do not count note websocket creates; final note counts are still checked. |
| `--progress-interval` | `GULP_SIGMA_ZIP_PROGRESS_INTERVAL` | Seconds between live aggregate count lines. Default: `30`. |
| n/a | `GULP_SIGMA_ZIP_MONITOR_CONTAINERS` | Docker service containers watched for restarts. Empty disables the guard. |
| `--operation-id` | `GULP_SIGMA_ZIP_OPERATION_IDS` | Existing operation for `--query-only`; repeat or comma-separate. |
| `--cleanup` / `--no-cleanup` | `GULP_SIGMA_ZIP_CLEANUP` | Delete created users and operations at the end. Default: cleanup enabled. |
| `--keep-operations` | `GULP_SIGMA_ZIP_KEEP_OPERATIONS` | Keep created operations for later `--query-only` runs. |

### examples

Quick shared-operation smoke test for UI inspection:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --users 2 \
  --concurrency 2 \
  --small-sigmas \
  --shared-operation \
  --progress-interval 5
~~~

Full 10-user run, one operation per user:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --concurrency 10 \
  --instances 1 \
  --allow-heavy-query-run
~~~

Full 10-user run where every user shares `op1`:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --concurrency 10 \
  --instances 1 \
  --shared-operation \
  --allow-heavy-query-run
~~~

Keep the seeded operations, then rerun only the query phase:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --concurrency 10 \
  --keep-operations \
  --allow-heavy-query-run

./test_scripts/sigma_zip_big_harness.py \
  --query-only \
  --users 10 \
  --operation-id op1 \
  --operation-id op2 \
  --operation-id op3 \
  --operation-id op4 \
  --operation-id op5 \
  --operation-id op6 \
  --operation-id op7 \
  --operation-id op8 \
  --operation-id op9 \
  --operation-id op10 \
  --allow-heavy-query-run
~~~

Query-only rerun against one shared operation:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --query-only \
  --shared-operation \
  --users 10 \
  --operation-id op1 \
  --allow-heavy-query-run
~~~

Spread workers across three backend instances:

~~~bash
./test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --concurrency 10 \
  --instances 3 \
  --instance-url http://localhost:8100 \
  --instance-url http://localhost:8101 \
  --allow-heavy-query-run
~~~

Equivalent environment-driven run:

~~~bash
GULP_SIGMA_ZIP_USERS=10 \
GULP_SIGMA_ZIP_CONCURRENCY=10 \
GULP_SIGMA_ZIP_CLEANUP=true \
GULP_BASE_URL=http://localhost:8080 \
GULP_SIGMA_ZIP_INSTANCES=3 \
GULP_SIGMA_ZIP_INSTANCE_URLS=http://localhost:8100,http://localhost:8101 \
GULP_TEST_USER=admin \
GULP_TEST_PASSWORD=admin \
GULP_SIGMA_ZIP_ALLOW_HEAVY=true \
./test_scripts/sigma_zip_big_harness.py
~~~

### backend tuning

Tune the harness and backend together. The harness submits work; gULP decides
how much of that work executes at once.

Relevant gULP config keys:

- `workers_max`: process count and ingest execution limit. `0` means CPU count.
- `workers_concurrency_max`: coroutine budget per worker process.
- `query_concurrency_max`: healthy ceiling for query/external-query tasks.
  Under Redis, OpenSearch, or PostgreSQL pressure, gULP may temporarily go lower
  and ramp back up after the pressure clears. Set it to `0` for no
  query-specific ceiling; query tasks may then use the global worker dispatch
  capacity, calculated as `workers_max * workers_concurrency_max` after resolving
  `workers_max=0` to CPU count.
- `opensearch_pool_maxsize`: OpenSearch HTTP pool per process.
- `postgres_pool_size`: PostgreSQL connections per process. By default this
  matches `query_concurrency_max`.
- `postgres_max_overflow`: extra PostgreSQL connections above `postgres_pool_size`;
  the default is `0`.

For query-heavy runs, each queued query task carries one individual query. This
is intentionally fine-grained: the BIG_SIGMAS rules vary widely in runtime and
result volume, so one-query tasks give the adaptive dispatcher the best fairness
and backpressure unit. `query_concurrency_max` controls the healthy number of
query tasks that may run at the same time across all users. The
dispatcher starts at that ceiling while Redis has queued query work, OpenSearch
is green/yellow with no search queue or new search rejections, Redis responds,
and this process's PostgreSQL pool is not exhausted. Any failed health probe or
observed pressure cuts the current group capacity; healthy checks then ramp it
back one slot at a time. The dispatcher fair-shares throttled execution groups
by request id so one accepted request does not monopolize all query slots while
another accepted request appears idle.

Recommended starting points:

~~~json
{
  "workers_max": 0,
  "workers_concurrency_max": 16,
  "query_concurrency_max": 4,
  "opensearch_pool_maxsize": 10,
  "postgres_pool_size": 4,
  "postgres_max_overflow": 0
}
~~~

### measured single-node profile

This profile was used for the 10-user BIG_SIGMAS harness run on a single
OpenSearch container under Docker Desktop/WSL. It uses `query_concurrency_max=12`
as the healthy query and query-note ceiling only while OpenSearch, Redis, and
PostgreSQL look healthy. Under dependency pressure, gULP may temporarily run
fewer tasks and then ramp back toward `12`.

Measured host/container context for this recommendation:

- CPU: 16 logical CPUs, 11th Gen Intel Core i7-11800H, 8 cores / 16 threads.
- WSL memory visible during the run: about `15 GiB`, with `4 GiB` swap.
- OpenSearch, PostgreSQL, Redis, and MinIO were Docker Desktop containers on the
  same host as the devcontainer and harness.
- OpenSearch was a single node/container; higher ceilings did not improve this
  setup even though gULP had more Python worker capacity.

Use this gULP config in `~/.config/gulp/gulp_cfg.json`:

~~~json
{
  "workers_max": 0,
  "workers_concurrency_max": 16,
  "query_concurrency_max": 12,
  "opensearch_pool_maxsize": 10,
  "postgres_pool_size": 12,
  "postgres_max_overflow": 0
}
~~~

With `workers_max=0`, gULP resolves the worker process count to CPU count. On
the measured WSL host this resolved to `16`, so the broad dispatcher capacity was
`16 * 16 = 256`; query execution was still governed by the adaptive
query controller.

The adaptive task controller also has three internal constants:

- `TASK_ADAPTIVE_CHECK_INTERVAL_SEC=2.0`: refresh Redis/OpenSearch/PostgreSQL health
  at most every two seconds instead of checking on every Redis pop.
- Pressure backoff is fixed: when Redis/OpenSearch/PostgreSQL pressure is
  observed, cut current group capacity to one sixth, down to at least one
  task, and hold that reduced capacity for thirty seconds before ramping again.
- `TASK_FAIRNESS_MAX_DEFERRALS=128`: while fair-sharing query tasks by request
  id, scan/requeue at most 128 same-request batches before giving up for that
  dispatch attempt. Requeued batches are acknowledged immediately so Redis
  pending does not grow by the scan depth.

Start from a clean queue and backend:

~~~bash
curl -fsS http://localhost:8080/docs >/dev/null && gulp --stop || true

sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
  XLEN gulp:stream:tasks:query
sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
  XLEN gulp:stream:tasks:ingest

setsid -f zsh -lc 'gulp --log-level info > /tmp/gulp-backend.log 2>&1'

for i in {1..60}; do
  curl -fsS http://localhost:8080/docs >/dev/null && break
  sleep 1
done
~~~

Check service headroom before starting the harness:

~~~bash
sudo docker stats --no-stream gulp-redis-1 gulp-postgres-1 gulp-os01-1

curl -fsS -u 'admin:Gulp1234!' \
  'http://localhost:9200/_cluster/health?pretty'

curl -fsS -u 'admin:Gulp1234!' \
  'http://localhost:9200/_cat/thread_pool/search?v'
~~~

If reusable operations do not exist yet, seed them once and keep them:

~~~bash
python test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --instances 1 \
  --concurrency 10 \
  --keep-operations \
  --allow-heavy-query-run
~~~

That command runs the full flow: user setup, operation creation, EVTX ingestion,
BIG_SIGMAS query execution, note creation, final count checks, and retained
operations. Use it when you need to measure the complete ingest-plus-query
workflow. On the measured `query_concurrency_max=12` run it printed:

- `elapsed=705.77s`
- `workload_elapsed=691.88s`
- `records_ingested=986330`
- `completed_queries=11490`
- `total_hits=734640`
- `notes=734640`

The seed/full-flow run creates deterministic operations `op1` through `op10`.
Reuse those for query-only comparisons:

~~~bash
python test_scripts/sigma_zip_big_harness.py \
  --query-only \
  --users 10 \
  --operation-id op1 \
  --operation-id op2 \
  --operation-id op3 \
  --operation-id op4 \
  --operation-id op5 \
  --operation-id op6 \
  --operation-id op7 \
  --operation-id op8 \
  --operation-id op9 \
  --operation-id op10 \
  --instances 1 \
  --concurrency 10 \
  --allow-heavy-query-run
~~~

For each run, compare the final harness `elapsed` and `workload_elapsed` lines,
not per-worker times. Also record Redis stream drain state and dependency
pressure:

~~~bash
# Redis query backlog and pending state.
sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
  XLEN gulp:stream:tasks:query
sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
  XINFO GROUPS gulp:stream:tasks:query

# OpenSearch query pressure. Watch `queue` and `rejected`.
curl -fsS -u 'admin:Gulp1234!' \
  'http://localhost:9200/_cat/thread_pool/search?v'

# Container CPU and memory.
sudo docker stats --no-stream gulp-redis-1 gulp-postgres-1 gulp-os01-1

# Backend health.
curl -fsS http://localhost:8080/docs >/dev/null && echo backend_up
~~~

For longer runs, keep this monitor loop in another terminal:

~~~bash
while true; do
  date -Is

  sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
    XLEN gulp:stream:tasks:query
  sudo docker exec gulp-redis-1 redis-cli -a 'Gulp1234!' --no-auth-warning \
    XINFO GROUPS gulp:stream:tasks:query || true

  curl -fsS -u 'admin:Gulp1234!' \
    'http://localhost:9200/_cat/thread_pool/search?v' || true

  sudo docker stats --no-stream gulp-redis-1 gulp-postgres-1 gulp-os01-1

  ps -o pid,rss,pcpu,etime,args -C python \
    | rg 'sigma_zip_big_harness|PID' || true

  sleep 30
done
~~~

Stop or lower the ceiling if OpenSearch search `rejected` increases, the
OpenSearch container restarts, Redis pending keeps growing instead of draining,
PostgreSQL memory climbs without settling, or backend health checks fail.

PostgreSQL cluster tests use the same harness commands. Point
`GULP_POSTGRES_URL` or `postgres_url` at the cluster writer/proxy endpoint, then
restart gULP and rerun the profile. gULP does not take multiple PostgreSQL URLs;
connection pools are per process and still need the same budget math:
`gulp_instances * processes_per_instance * (postgres_pool_size +
postgres_max_overflow)`. Extra PostgreSQL nodes are useful only if that endpoint
actually improves write latency, connection headroom, failover behavior, or
managed storage performance for the collaboration workload.

Observed `query_concurrency_max=12` query-only reference result on this setup:

- clean pass: `passed=10 failed=0`
- current stronger-backoff controller: `elapsed=616.46s`,
  `workload_elapsed=616.12s`
- older controller baseline: `elapsed=529.63s`, `workload_elapsed=529.34s`
- `completed_queries=11490`, `total_hits=734640`, `notes=734640`
- Redis query stream drained to `0`, pending `0`
- OpenSearch stayed green with no sampled search rejections

Do not jump from this profile to `query_concurrency_max=0` on a single-node
Docker Desktop/WSL setup. With the stronger backoff controller,
`query_concurrency_max=16` completed without restarting OpenSearch, but took
`817.64s`, slower than `query_concurrency_max=12` on the same data. On this machine, higher query
ceilings mostly trigger backoff rather than useful throughput.

Increase `--users` to model more active users. Increase `--concurrency` only
when you want more of those users active at the same time. Increase
`query_concurrency_max` only when OpenSearch still has CPU headroom and query
tasks are draining slowly. Increase `workers_max` only when ingestion is the
bottleneck and OpenSearch/PostgreSQL can absorb more parallel writes.

Free Python worker processes alone are not a reason to raise `query_concurrency_max`.
Check OpenSearch CPU, search rejections, circuit breaker messages, request
latency, and PostgreSQL note-write pressure first. On a single-node WSL/Docker
Desktop setup, `query_concurrency_max=12` with `workers_concurrency_max=16` completed
a 10-user full ingest plus BIG_SIGMAS run cleanly, but OpenSearch was already
near saturation during the query phase.

To retest the same single-node profile with a larger OpenSearch heap, start the
stack with the separate override:

~~~bash
sudo docker compose -f docker-compose.yml -f docker-compose.opensearch-big.yml --profile dev up -d
~~~

Confirm that `os01` picked up the `4 GiB` heap before running the harness:

~~~bash
curl -fsS 'http://localhost:9200/_nodes/os01/jvm?filter_path=nodes.*.jvm.mem.heap_max_in_bytes,nodes.*.name'
~~~

Observed larger-heap single-node result on this setup:

- `OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g`: clean full-flow pass,
  `elapsed=953.11s`, `workload_elapsed=938.58s`, exact totals; Redis query
  stream drained to `0`, pending `0`; sampled OpenSearch search rejections
  stayed at `0`. Compared with the original/default `os01` single-node service
  result (`elapsed=705.77s`, `workload_elapsed=691.88s`), the larger heap was
  more stable-looking in memory/headroom samples but slower end to end. Treat the
  larger heap as a pressure/stability probe, not the default fast path.

On Docker Desktop/WSL, watch both harness and service memory. If Redis streams
retain stale interrupted work after a crash, clear only confirmed stale harness
streams before restarting gULP, otherwise the backend will immediately resume
the old load.

### local OpenSearch cluster probe

If `query_concurrency_max=12` is stable but higher ceilings only trigger adaptive backoff, the next
useful experiment is to give OpenSearch more local capacity. The repository
includes [docker-compose.opensearch-cluster.yml](../docker-compose.opensearch-cluster.yml),
a three-node OpenSearch variant for this devcontainer workflow. It is a separate
compose file so the default single-node setup stays unchanged.

This is a local capacity probe, not production high availability: all three
OpenSearch containers still share the same Docker Desktop/WSL CPU, memory, disk,
and network path. It can show whether one OpenSearch JVM was the limiting point,
but it cannot prove multi-host cluster behavior.

Stop the current single-node stack and recreate the services with the cluster
compose:

~~~bash
curl -fsS http://localhost:8080/docs >/dev/null && gulp --stop || true

sudo docker compose -f docker-compose.yml down
sudo docker compose -f docker-compose.opensearch-cluster.yml --profile dev up -d
~~~

This does not normally require restarting Docker Desktop or WSL. If Docker
Desktop/WSL is already unresponsive, restart it first from the host, then run the
compose commands.

Before creating or reseeding operations, choose the OpenSearch replica count in
`~/.config/gulp/gulp_cfg.json`:

~~~json
{
  "opensearch_index_replicas": 0,
  "query_concurrency_max": 12
}
~~~

Use `opensearch_index_replicas=0` for the fastest local write-heavy probe. Use
`1` only when you explicitly want replica copies and can afford the extra
ingest/note write cost. gULP's OpenSearch storage is usually analysis-time
working storage, not permanent retention, so `0` is the normal local and
high-throughput recommendation.

Then start gULP normally:

~~~bash
setsid -f zsh -lc 'gulp --log-level info > /tmp/gulp-backend.log 2>&1'

for i in {1..60}; do
  curl -fsS http://localhost:8080/docs >/dev/null && break
  sleep 1
done
~~~

Check that all OpenSearch nodes joined:

~~~bash
curl -fsS -u 'admin:Gulp1234!' 'http://localhost:9200/_cat/nodes?v'
curl -fsS -u 'admin:Gulp1234!' 'http://localhost:9200/_cluster/health?pretty'
curl -fsS -u 'admin:Gulp1234!' 'http://localhost:9200/_cat/thread_pool/search?v'
~~~

For a clean comparison, reseed the BIG_SIGMAS operations after changing
`opensearch_index_replicas`. Existing data streams keep the replica count they
were created with and should not be used to judge a different replica setting.

Run the same harness commands used by the `query_concurrency_max=12` profile. Keep
`query_concurrency_max=12` for the
first cluster run so the only major variable is OpenSearch capacity:

~~~bash
python test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --instances 1 \
  --concurrency 10 \
  --keep-operations \
  --allow-heavy-query-run
~~~

If the full-flow run is clean, compare query-only runs against the single-node
baseline and only then raise `query_concurrency_max`:

~~~bash
python test_scripts/sigma_zip_big_harness.py \
  --query-only \
  --users 10 \
  --operation-id op1 \
  --operation-id op2 \
  --operation-id op3 \
  --operation-id op4 \
  --operation-id op5 \
  --operation-id op6 \
  --operation-id op7 \
  --operation-id op8 \
  --operation-id op9 \
  --operation-id op10 \
  --instances 1 \
  --concurrency 10 \
  --allow-heavy-query-run
~~~

Monitor all three OpenSearch containers during the run:

~~~bash
sudo docker stats --no-stream \
  gulp-redis-1 gulp-postgres-1 gulp-os01-1 gulp-os02-1 gulp-os03-1

curl -fsS -u 'admin:Gulp1234!' 'http://localhost:9200/_cat/nodes?v'
curl -fsS -u 'admin:Gulp1234!' 'http://localhost:9200/_cat/thread_pool/search?v'
~~~

The pass criteria are the same as the single-node profile: exact harness totals,
Redis query stream drained to zero, no OpenSearch restarts, and no increasing
search rejections. Compare final harness `elapsed` and `workload_elapsed`, not
per-worker times.

Observed local three-node results on the WSL/Docker host:

- `opensearch_index_replicas=1`, `query_concurrency_max=12`: failed correctness
  checks after `elapsed=1100.21s`; OpenSearch stayed green and queue-free. Track
  this as a separate investigation before drawing conclusions about replicas.
- `opensearch_index_replicas=0`, `query_concurrency_max=12`: clean full-flow
  pass, `elapsed=1155.77s`, exact totals.
- `opensearch_index_replicas=0`, `query_concurrency_max=0`: clean query-only
  pass, `elapsed=929.46s`, exact totals.
- `opensearch_index_replicas=0`, `query_concurrency_max=24`: clean query-only
  pass, `elapsed=854.76s`, exact totals.
- `opensearch_index_replicas=0`, `query_concurrency_max=32`: clean query-only
  pass, `elapsed=825.99s`, exact totals.
- `opensearch_index_replicas=0`, `query_concurrency_max=48`: clean query-only
  pass, `elapsed=874.48s`, exact totals. This was slower than `32` and sampled
  idle OpenSearch periods while Redis still had queued query tasks.
- single-node `query_concurrency_max=0`:
  clean query-only pass, `elapsed=687.48s`, `workload_elapsed=687.14s`, exact
  totals, no sampled search rejections or restart. This is safe but slower than
  both single-node qmax=12 references, so do not use qmax=0 as the stable
  single-node profile.

On this single physical host, three OpenSearch containers did not beat the
single-node `query_concurrency_max=12` baselines. Each operation used one
primary shard, so a single operation's searches mostly ran on one node while the
cluster still paid coordination and Docker networking overhead. Treat local
multi-node results as a capacity-shape probe, not proof that real multi-host
OpenSearch will be slower.

## ingestion tool

see [gulp-cli `ingest file` command](https://github.com/mentat-is/gulp-cli/blob/master/docs/examples.md#ingestion-workflows)

## query external tool

see [gulp-cli `query external` command](https://github.com/mentat-is/gulp-cli/blob/master/docs/examples.md#query-external-data-source)

## observability

See [observability.md](./observability.md) for details on enabling Prometheus metrics and connecting Grafana for dashboards.
