# purpose

the plan is about optimizing gulp for high loads query/ingestion requests.

# expected results

high volume ingestion and query must be supported, current target is 10 users massively ingesting and querying using `sigma_zip_big_harness` which exercises gulp with massive ingestion and sigma queries in parallel and concurrently (the harness is currently buggy and not guaranteed to work correctly)

# the current problems

from recent tests, the weak points are PostgreSQL and Opensearch pools (PostgreSQL "too many connections", OpenSearch cancelling task due to high resource consumption, and such).

particularly, it has been noted that batch notes generation implemented in collab/note.py is particularly heavy and tends to saturate PostgreSQL with the big load caused by the test (thousands or millions of notes).

# rules you must follow

1. parallelism and concurrency must be maintained, gulp have a `workers_max` sized process pool which executes asynchronous tasks in worker processes : no slow task must block other tasks to run if the server is healthy (read later)
2. all received requests must be queued on redis on request reception by the REST API handlers, those will be dequeued and dispatched by the task scheduler implemented in server_api.py::dispatch_tasks : you have to consider the implementation sub-optimal and to be highly optimized.
3. keep a particular eye on note batching as said above: consider to rearchitect the note table if needed to reduce pressure.
4. query_sigma_zip plugin already batches the queries, they would be double-batched then by the scheduler, avoid it, only the scheduler should batch. it also parses the rules twice, once to count, once to convert them. fix it.

you must never attempt to fix/implement the following:

1. legacy support / backward compatibility: change whatever you want and forget about it except if you touch the REST API. if so, you have to change also gulp-sdk, gulpui-web and gulp-cli accordingly.

2. requests replay / retry : gulp doesn't support this by design, never try to implement it.

3. req_id are unique, no duplicate requests are possible

# my idea

(do not trust blindly, validate it): implement the scheduler so it always start with the maximum concurrency per worker as defined in config.worker_concurrency_max: tasks are dequeued in batch from redis and feeded to the workers pool. the scheduler should monitor postgres/opensearch/redis health, gradually tuning concurrency depending on the services load

# testing

for testing, use sigma_zip_big_harness.

currently, the harness allows to specify the number of concurrent users (which it creates, and creates an operation for each user to store data in), the number of gulp instances running, the additional gulp instances. you can rearchitect it as you wish, as far as it mantains the multiuser / single gulp instance/multi gulp instances capabilities and the correctness check against the defined _EXPECTED_RECORDS_INGESTED,_EXPECTED_COMPLETED_QUERIES, _EXPECTED_TOTAL_HITS (per operation # of ingested documents/# of queries run/# of matches)

- concentrate on single instance first, multiple gulp instances will be addressed once single instance is stable and satisfies the requirements.
- consider the full BIG_SIGMAS query phase the dangerous part of the harness: it submits 1149 queries per user, so the 10-user target means 11490 query submissions after ingest. Do not run the 10-user full-sigma harness until smaller query-side validation shows the target environment can drain that backlog without locking the machine.

## running gulp

use gulp --reset-collab --create test_operation to spawn the first instance (will use localhost, port 8080), and spawn subsequent instances using ports >=8100, i.e. GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 gulp. in the end, using gulp --stop you can stop all running instances.

Docker is reachable from this devcontainer through passwordless sudo. Use:

- inspect services: `sudo docker ps`
- service logs: `sudo docker logs --since ... --until ... <container>`
- live resources: `sudo docker stats --no-stream`
- Redis CLI: `sudo docker exec gulp-redis-1 redis-cli ...`

use top/htop to inspect containers load.

## implementation status

Keep implementation history compact by merging superseded details; keep every benchmark elapsed time in the append-only table below.

- Scheduler: every REST request enters a typed Redis stream; generic dispatch owns worker concurrency, samples Redis/PostgreSQL/OpenSearch health, and avoids nested query process-pool fan-out.
- Redis: multi-stream reads never overclaim requested capacity, rotate fairly, and leave no hidden pending entries.
- Notes: automatic query/Sigma text is stored once per operation in `note_text`; indexed notes retain only per-hit text while preserving the REST representation.
- Sigma: source metadata transactions end before CPU-heavy conversion; `query_sigma_zip` queues preprocessing, converts each rule once, spools the exact total, and sends count/byte-bounded batches to the plugin-agnostic scheduler. Queries larger than 1 MiB are isolated because they cannot be split semantically.
- Harness/profile: multi-user correctness and five-second progress work with `workers_max=2`, `workers_concurrency_max=4`, PostgreSQL `4+0`, and Redis backlog `20000`.
- Proof: backend `274 passed`; Sigma ZIP `4 passed` including 1,001 rules and 200 KB queries; paid plugins otherwise `61 passed` with two unrelated Vir.IT expectation failures.
- Next gate: preserve generic scheduling while preventing one request's contiguous batch run from dominating FIFO tail latency, then increase toward 10 users while exact counts and service health remain stable.

## benchmark history

Counts are `records / completed queries / hits / notes`; times are harness wall-clock elapsed when available.

| Date | Workload | Implementation | Elapsed | Result |
| --- | --- | --- | ---: | --- |
| 2026-07-13 | 1 user, small | adaptive scheduler + shared note text | 66.44s | PASS `98,633 / 14 / 15 / 15` |
| 2026-07-13 | 2 users, small | adaptive scheduler + shared note text | 92.70s | PASS `197,266 / 28 / 30 / 30` |
| 2026-07-13 | 1 user, full | per-query Redis fan-out | 142.66s | PASS `98,633 / 1,149 / 73,464 / 73,464` |
| 2026-07-13 | 2 users, full | per-query Redis fan-out | 231.33s | PASS `197,266 / 2,298 / 146,928 / 146,928` |
| 2026-07-13 | 2 users, full | Redis claim fix | 336.40s | PASS `197,266 / 2,298 / 146,928 / 146,928` |
| 2026-07-13 | 1 user, full | rejected whole-ZIP Redis envelope | 165.21s | PASS; one 4.8 MB task payload |
| 2026-07-13 | 1 user, small | bounded plugin batches | 79.64s | PASS `98,633 / 14 / 15 / 15` |
| 2026-07-13 | 1 user, full | bounded plugin batches | 181.92s | PASS `98,633 / 1,149 / 73,464 / 73,464`; 288 batches, sampled entries 3-667 KB |
| 2026-07-13 | 2 users, full | bounded plugin batches | 308.47s | PASS `197,266 / 2,298 / 146,928 / 146,928`; users finished in 232.56s and 305.23s |
