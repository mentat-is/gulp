# Scaling gULP

Scale the measured bottleneck. Extra gULP nodes add API and worker capacity;
they do not make a saturated OpenSearch or PostgreSQL service faster.

## Choose the bottleneck

| Observation | Scale first | Trade-off / when to use it |
| --- | --- | --- |
| gULP CPU, workers, or websocket/API handling is saturated while Redis, PostgreSQL, and OpenSearch have headroom | gULP nodes | Add stateless instances behind a load balancer. Best for more clients, raw ingestion, or task consumers. Use `instance_roles` only when one task type dominates. |
| Search or bulk ingest latency climbs; OpenSearch CPU, heap, disk, or queues are saturated | OpenSearch | Add CPU/RAM/faster disk first, then cluster nodes for capacity or availability. Replicas improve availability/read capacity but add write cost. |
| Note/collaboration writes slow down or PostgreSQL pools are exhausted | PostgreSQL | Increase database capacity or use a managed/HA deployment behind one writer/proxy endpoint. gULP does not route application writes across PostgreSQL nodes. |
| Queue or pub/sub latency rises, or Redis memory is pressured | Redis | Use a larger or managed Redis service. This is usually a coordination bottleneck, not a way to increase query throughput. |
| More than one tier is saturated | Mixed | Scale the limiting services together. Add gULP nodes only after the shared services can absorb the added work. |

## Common topologies

### Multiple gULP nodes

Run identical gULP instances against the same Redis, PostgreSQL, and OpenSearch
endpoints. Put client traffic behind a load balancer. This increases API,
websocket, raw-ingestion, and worker capacity, but all nodes still contend for
the same storage services. The [enterprise deployment guide](enterprise_deployment.md)
provides an nginx upstream for one or more private gULP instances. With the
repository's Compose file, the services listen on `127.0.0.1` unless
`GULP_SERVICES_BIND` names an address the other nodes can reach.

The 10-user Sigma ZIP stress run completed successfully with two gULP nodes:
all expected records, queries, hits, and notes passed without a service restart.
It took 1449.92s versus 969.50s with one node because the single OpenSearch
node was already saturated. Treat extra gULP nodes as a stability/capacity
option only when service metrics show headroom.

### Multiple OpenSearch nodes

Use this when indexing or searches are the limit. It is the relevant scale-out
choice for data-heavy workloads. Configure `opensearch_index_replicas` before
operations are created: use `0` for write-heavy disposable data, or replicas
when availability/read capacity justifies slower writes.

### Multiple PostgreSQL nodes

Use an externally managed PostgreSQL HA/scale solution exposed as one endpoint.
It improves availability and database capacity, but current collaboration writes
remain write-heavy; do not add gULP-side read/write routing.

## Tune and verify

Keep `workers_max: 0` unless measurement shows a reason to cap it. Bound
`workers_concurrency_max`, OpenSearch pool size, and PostgreSQL pools to what
the dependent services can sustain. gULP reduces task admission under service
pressure; that protects stability but cannot create storage capacity.

Use the [stress harness](./testing.md) for an intentionally extreme test and
[observability](./observability.md) to identify the bottleneck before changing
the topology. Architecture details, including roles and raw ingestion routing,
remain in [architecture.md](./architecture.md).
