# purpose

the plan is about optimizing gulp for high loads query/ingestion requests.

# expected results

high volume ingestion and query must be supported, current target is 10 users massively ingesting and querying using `sigma_zip_big_harness` which exercises gulp with massive ingestion and sigma queries in parallel and concurrently (the harness is currently buggy and not guaranteed to work correctly)

# the current problems

from recent tests, the weak points are PostgreSQL and Opensearch pools (PostgreSQL "too many connections", OpenSearch cancelling task due to high resource consumption, and such).

particularly, it has been noted that batch notes generation implemented in collab/note.py is particularly heavy and tends to saturate PostgreSQL with the big load caused by the test (thousands or millions of notes).

# rules you must follow

1. parallelism and concurrency must be maintained, gulp have a `workers_max` sized process pool which executes asynchronous tasks in worker processes
2. all received requests must be queued on redis on request reception by the REST API handlers, those will be dequeued and dispatched by the task scheduler implemented in server_api.py::dispatch_tasks : you have to consider the implementation sub-optimal and to be highly optimized.
2.1. implement the scheduler so it always aims at maximum concurrency per worker 
as defined in config.worker_concurrency_max: tasks are dequeued in batch from redis and feeded to the workers pool. the scheduler monitors postgres/opensearch/redis health, tuning concurrency dynamically depending on the services load
2.2. all tasks should be treated equally (no special cases for ingestion or query tasks), the scheduler should be agnostic to the task type and should only care about the services load and the workers pool concurrency.
2.3. no slow task must block other tasks to run if the server is healthy, the scheduler should be able to dispatch tasks in a way that all workers are busy and no worker is idle while there are pending tasks in the queue.
3. if you change the collab database layout (i.e. adding a table), you must provide a migration script in collab_migrate/v2.0.0

you must never attempt to fix/implement the following:

1. legacy support / backward compatibility: change whatever you want and forget about it except REST API changes must also update `gulpui-web`, `gulp-sdk`, `gulp-cli`.

2. backend request replay is out of scope.

3. req_id are unique, no duplicate requests are possible

# testing

for testing, use sigma_zip_big_harness.

currently, the harness allows to specify the number of concurrent users (which it creates, and creates an operation for each user to store data in), the number of gulp instances running, the additional gulp instances. you can rearchitect it as you wish, as far as it mantains the multiuser / single gulp instance/multi gulp instances capabilities and the correctness check against the defined _EXPECTED_RECORDS_INGESTED,_EXPECTED_COMPLETED_QUERIES, _EXPECTED_TOTAL_HITS (per operation # of ingested documents/# of queries run/# of matches)

- concentrate on single instance first, multiple gulp instances will be addressed once single instance is stable and satisfies the requirements.
- consider the full BIG_SIGMAS query phase the dangerous part of the harness: it submits 1149 queries per user, so the 10-user target means 11490 query submissions after ingest. Do not run the 10-user full-sigma harness until smaller query-side validation shows the target environment can drain that backlog without locking the machine.

## load-test monitoring

During harness runs, periodically monitor PostgreSQL, Redis, OpenSearch, and gULP processes and always report service load and effective gULP concurrency.

## implementation status

Keep implementation history compact by merging superseded details; keep every benchmark elapsed time in the append-only table below.

- Scheduler: every REST request enters a typed Redis stream; batched requests use request-scoped streams so generic dispatch rotates work fairly without task-type cases. Stream registration and drained-stream deletion are atomic. Generic dispatch owns worker concurrency, samples Redis/PostgreSQL/OpenSearch health, avoids nested query process-pool fan-out, and resolves `workers_concurrency_max=0` from runtime service node counts with PostgreSQL/OpenSearch weighted above Redis before spawning workers. Pressure halves dispatch concurrency; each healthy sample restores four slots, avoiding both the measured throughput loss of `+1` and the OpenSearch queue oscillation of config-sized `+16` recovery.
- Redis: multi-stream reads never overclaim requested capacity, rotate fairly, and leave no hidden pending entries.
- Notes: automatic query/Sigma text is stored once per operation in `note_text`; indexed notes retain only per-hit text while preserving the REST representation.
- Sigma: source metadata transactions end before CPU-heavy conversion; `query_sigma_zip` queues preprocessing, converts each rule once, spools the exact total, and sends count/byte-bounded batches to the plugin-agnostic scheduler. Queries larger than 1 MiB are isolated because they cannot be split semantically.
- Harness/profile: the 10-user full-Sigma target passes with the default `workers_max=0` and `workers_concurrency_max=0`, resolving on this host to 16 processes, 16 coroutine slots per process, a nominal capacity of 256, and a health-limited OpenSearch ceiling of 25. PostgreSQL `4+0` gives a theoretical 68-connection gULP budget; all users progress independently once runnable.
- Proof: backend `293 passed` after bounded recovery; Sigma ZIP `4 passed` including 1,001 rules and 200 KB queries; paid plugins otherwise `61 passed` with two unrelated Vir.IT expectation failures.
- Next gate: design and verify the cluster-wide process and PostgreSQL budget before starting a second default-sized instance. Two naive `workers_max=0` instances would budget `2 * (1 + 16) * 4 = 136` PostgreSQL connections against `max_connections=100`, so the single-instance default cannot be assumed safe for multi-instance deployment.

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
| 2026-07-13 | 2 users, full | fair request streams | 231.62s | PASS `197,266 / 2,298 / 146,928 / 146,928`; users finished in 211.01s and 228.49s; PostgreSQL 19 connections, Redis 19.71 MiB, OpenSearch 70% heap with no queue/rejections |
| 2026-07-13 | 4 users, full | fair request streams | 441.31s | PASS `394,532 / 4,596 / 293,856 / 293,856`; all runnable requests progressed; sampled peak OpenSearch 72% heap, 60% CPU, 5 active searches with no queue/rejections; PostgreSQL 19 connections |
| 2026-07-13 | 6 users, full | fair request streams | 718.82s | PASS `591,798 / 6,894 / 440,784 / 440,784`; three concurrent conversions plus five query batches filled all 8 dispatcher slots; PostgreSQL 19 connections and OpenSearch had no queue/rejections |
| 2026-07-13 | 10 users, full | fair request streams | 1312.81s | PASS `986,330 / 11,490 / 734,640 / 734,640`; all 10 requests progressed; sampled peak Redis 87.56 MiB and OpenSearch 77% heap, 67% CPU, 11 active searches with no queue/rejections; PostgreSQL 20 connections |
| 2026-07-13 | 1 user, full | auto `16x16`, config-sized recovery | 174.42s | PASS `98,633 / 1,149 / 73,464 / 73,464`; PostgreSQL pools expanded to 75 total sessions |
| 2026-07-13 | 2 users, full | auto `16x16`, config-sized recovery | 256.94s | PASS `197,266 / 2,298 / 146,928 / 146,928`; 14 active OpenSearch searches with no queue/rejections; worker recycling reduced rather than accumulated PostgreSQL sessions |
| 2026-07-13 | 10 users, full | auto `16x16`, config-sized `+16` recovery | 1176.67s | PASS `986,330 / 11,490 / 734,640 / 734,640`; dynamic backoff reached 6 but the one-sample recovery repeatedly recreated OpenSearch queues; no rejections |
| 2026-07-13 | 4 users, full | auto `16x16`, additive `+1` recovery | 458.93s | PASS `394,532 / 4,596 / 293,856 / 293,856`; fail-safe `25 -> 1` recovery was stable but slow |
| 2026-07-13 | 10 users, full | auto `16x16`, additive `+1` recovery | ~484s partial | ABORT at `986,330 / 1,553 / 28,393 / 28,393`; sustained query progress materially trailed the config-sized recovery run, so the failed approach was stopped early |
| 2026-07-13 | 4 users, full | auto `16x16`, bounded `+4` recovery | 422.11s | PASS `394,532 / 4,596 / 293,856 / 293,856`; 8.0% faster than `+1` and 4.4% faster than the prior `2x4` run; no rejections |
| 2026-07-13 | 10 users, full | auto `16x16`, bounded `+4` recovery | 1202.96s | PASS `986,330 / 11,490 / 734,640 / 734,640`; 8.4% faster than `2x4`, 2.2% slower than oscillatory `+16`; all users progressed independently, no OpenSearch rejections or PostgreSQL connection failures |
