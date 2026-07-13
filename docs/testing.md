# Testing gULP

Install the local SDK, then start a clean development backend when one is not
already available:

~~~bash
pip3 install -e ./gulp-sdk
curl -fsS http://localhost:8080/docs >/dev/null || \
  gulp --reset-collab --create test_operation
~~~

Run the normal suite as needed:

~~~bash
python3 -m pytest -v -s -x tests/
python3 -m pytest -v -s -x tests/integration/
# usually, test_stress.py is enough to dictate the overall health of the backend
python3 -m pytest -v -s -x tests/integration/test_stress.py
~~~

Use [run_tests.sh](../test_scripts/run_tests.sh) when a wrapper is more
convenient:

~~~bash
./test_scripts/run_tests.sh tests/integration/test_queries.py
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
python test_scripts/sigma_zip_big_harness.py \
  --users 10 \
  --instances 1 \
  --concurrency 10 \
  --allow-heavy-query-run
~~~

The shared-operation profile puts every user on one operation and deliberately
amplifies collaboration-note contention:

~~~bash
python test_scripts/sigma_zip_big_harness.py \
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

Stop a backend started for testing when finished:

~~~bash
gulp --stop
~~~

## Related tools

- [Ingestion tool](./integration.md)
- [External query tool](./query.md)
- [Observability](./observability.md)
