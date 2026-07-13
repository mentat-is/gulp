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

## memory

append progress in this file each time you progress.

### 2026-07-13: initial audit

- Traced REST query ingestion through Redis streams, `GulpServer._dispatch_tasks`, and the `aiomultiprocess` pool. The scheduler already fills `workers_max * workers_concurrency_max` slots and refills a free slot without waiting for slower siblings, but it has no service-health feedback.
- Confirmed the installed `query_sigma_zip` extension first converts every rule in `count_only` mode, converts the same files again, batches the resulting queries, and enqueues each batch. Core query execution batches that payload again inside one worker, so requirement 4 is not satisfied.
- Confirmed automatic notes are bulk inserted and websocket payload text is stripped, but every matched document still stores the repeated query/Sigma text inline in PostgreSQL.
- The backend was down at audit time; Redis, PostgreSQL, and OpenSearch containers were healthy at idle. Historical logs show a full 1,149-query request needed about 35 minutes and generated very large result messages, so the full 10-user run remains unsafe until smaller query validation passes.

### 2026-07-13: scheduler owns query concurrency

- Removed the nested query process-pool dispatch: the Redis scheduler now assigns query tasks directly to workers, and a queued query executes in that worker instead of holding a main-process PostgreSQL session while creating another worker batch.
- Core query producers now enqueue one Redis message per query in one atomic Redis transaction. Redis dequeue batching is therefore the only query batching layer.
- Added scheduler service sampling every two seconds. Redis failure, PostgreSQL connection usage, OpenSearch search queue/rejections, JVM heap, and CPU drive multiplicative backoff; healthy service capacity drives gradual ramp-up. The initial ceiling is bounded by PostgreSQL headroom and OpenSearch search threads rather than blindly starting `workers_max * workers_concurrency_max` work against a single node.
- Focused scheduler/query queue tests pass: `50 passed`.

### 2026-07-13: automatic note storage pressure

- Added one `note_text` row per distinct operation/query body. Automatic notes reference that row and store only per-hit highlight text inline, while serialization still returns the original combined `text` field; no REST/SDK/UI contract changed.
- Added scalar `doc_id` plus indexes for note context, source, document, shared text, and shared-text operation cascades. Note filtering now uses the indexed document ID and searches both inline and shared text.
- Manual note edits detach shared text so edited content remains independent. Focused note, scheduler, and idempotency tests pass: `63 passed`.

### 2026-07-13: Sigma ZIP request queueing and single conversion

- The extension REST handler now queues the completed ZIP as a Redis query task instead of starting untracked main-process background work.
- A scheduler-assigned worker unzips and converts the full rule set exactly once, creates request stats from the resulting query count, then uses the core atomic fan-out helper to enqueue one Redis message per query. Empty conversions finish cleanly and preprocessing/fan-out errors mark request stats failed.
- Added paid-plugin coverage for immediate preprocessing queueing and one-pass conversion. Combined focused coverage passes: `65 passed`.

### 2026-07-13: staged single-instance validation

- Fixed harness setup discovered at runtime: generated worker passwords now satisfy normal backend validation, configured PostgreSQL defaults are reported correctly, and operation creation no longer re-adds default grants (which triggered an expired async relationship load). Removed two stale CLI tests for flags the harness never implemented.
- Temporary validation profile: `workers_max=2`, `workers_concurrency_max=4`, PostgreSQL pool `4+0`, Redis task backlog `20000`; service containers stayed alive throughout.
- 1 user, small Sigma: PASS in 66.44s with exact `98,633 / 14 / 15 / 15` records, completed queries, hits, and notes.
- 2 users concurrently, small Sigma: PASS in 92.70s with exact aggregate `197,266 / 28 / 30 / 30`.
- 1 user, full 1,149 Sigma: PASS in 142.66s overall (query phase about 78s) with exact `98,633 / 1,149 / 73,464 / 73,464`. A live sample at 20,810 notes had only 4 `note_text` rows; OpenSearch search queue/rejections were zero and PostgreSQL used 21/100 connections.
- 2 users concurrently, full Sigma: PASS in 231.33s with exact aggregate `197,266 / 2,298 / 146,928 / 146,928`. This clears the staged query-side safety gate before attempting the 10-user target.
- Full backend unit suite passes: `271 passed` (two third-party Scapy deprecation warnings). PostgreSQL pool defaults now remain conservative at 4 unless worker concurrency is explicitly configured.

### 2026-07-13: target-run Redis claim correction

- The first 10-user full-Sigma attempt exposed a Redis Streams semantic bug under simultaneous ingest and query traffic: `XREADGROUP COUNT` applies per stream, so the scheduler could claim more than its free capacity, and the single-task blocking path claimed one entry from every stream while returning only the first. The unreturned query preprocessing entries later became abandoned and failed with zero completed queries.
- Redis task reads now poll one stream at a time, divide each dequeue batch across task types without exceeding the requested total, and rotate the first stream for fairness. The blocking path also claims exactly one entry per read, preventing hidden pending entries. Task queue unit coverage passes: `31 passed`.
