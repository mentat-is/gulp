- [querying gulp](#querying-gulp)
  - [overview](#overview)
  - [`query_raw_paginate` and deep pagination](#query_raw_paginate-and-deep-pagination)
  - [queuing and preview behavior](#queuing-and-preview-behavior)
  - [debugging query issues](#debugging-query-issues)

# querying gulp

this section documents the main query endpoints of the Gulp REST API: `query_gulp`, `query_raw`, `query_sigma`, and `query_external`.

these endpoints provide flexible and powerful ways to query data in Gulp, leveraging OpenSearch as the backend.

understanding their relationships and the underlying query mechanics is essential for advanced usage and troubleshooting.

## overview

- **`query_gulp`**: for simple queries using pre-defined filters: recommended for most use cases where you want to leverage Gulp's built-in filtering logic.

- **`query_raw`**: the core endpoint for advanced users, also used internally by the gULP web UI: accepts raw [OpenSearch Query DSL](https://opensearch.org/docs/latest/query-dsl/) queries, giving you full control over the search logic.

    the following is an example of what you may use as the `q` parameter in `query_raw`:

    ```json
    [
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": "(gulp.operation_id: test_operation AND gulp.context_id: dbdcd5d70efd3e4242cedd1e4a0c9b2d186a5a8f AND gulp.source_id: 7344ed16e93ee2dcb2a1e019c01596e72249d4c3 AND gulp.timestamp: [1727734017000000000 TO 1730414835000000000])"
                            }
                        },
                        {
                            "wildcard": {
                                "gulp.unmapped.Guid": {
                                    "case_insensitive": true,
                                    "value": "*8-4994-a5bA*"
                                }
                            }
                        }
                    ]
                }
            }
        }
    ]
    ```

- **`query_raw_paginate`**: similar to `query_raw`, but synchronous and with
  direct paginated results. Existing clients can continue to use `limit` and
  `offset`; PIT sessions add a stable snapshot, cursor continuation, and deep
  page jumps beyond OpenSearch's 10,000-result window.
  
- **`query_sigma`**: accepts [Sigma rules](https://github.com/SigmaHQ/sigma), translates them to OpenSearch queries, and executes them: useful for threat detection and security analytics.

- **`query_external`**: for querying external sources via plugins: relies on plugin-specific logic to query the external source (i.e. a SIEM) using its specific DSL/API.

> **NOTE:** except for `query_external`, all the above endpoints ultimately resolve to `query_raw` internally.

## `query_raw_paginate` and deep pagination

`POST /query_raw_paginate` accepts one OpenSearch DSL query in `q` and a
`GulpQueryParameters` object in `q_options`. The response data always contains
`total_hits` and `docs`.

### Offset mode

`pagination_mode` defaults to `offset`, so existing requests remain valid:

```json
{
  "q": {"query": {"match_all": {}}},
  "q_options": {
    "limit": 50,
    "offset": 125000,
    "sort": {"@timestamp": "asc"}
  }
}
```

The requested `offset` is a logical position, not an OpenSearch `from` value.
For a request that crosses the 10,000-result window, gULP creates a temporary
point-in-time (PIT) snapshot, seeks to the requested position with
`search_after`, and closes that PIT before returning. This preserves the old
response shape and permits arbitrary deep offsets without retaining server-side
pagination state between requests.

### PIT mode

Set `pagination_mode` to `pit` when several pages must use a consistent
snapshot. Omit `pit_id` on the first request:

```json
{
  "q": {"query": {"match_all": {}}},
  "q_options": {
    "pagination_mode": "pit",
    "limit": 50,
    "offset": 0,
    "sort": {"@timestamp": "asc"}
  }
}
```

The response data includes the session and next-page cursor:

```json
{
  "total_hits": 200000,
  "docs": [],
  "pit_id": "…",
  "search_after": [1710000000000000000, 918273]
}
```

Pass the returned `pit_id` on every later request. For the immediately following
page, also pass `search_after`; this avoids seeking and performs one OpenSearch
search. For a direct jump to any page whose cursor is not known, omit
`search_after` and set `offset` to the target logical position. The backend
seeks from whichever end of the result set is closer and returns documents in
the originally requested order.

Cursor values are opaque JSON scalars. Store and return the complete list
unchanged; do not assume that a cursor contains objects. PIT sorting has a
stable `_shard_doc` tie-breaker in addition to the requested sort.

Each search refreshes the PIT lifetime configured in `gulp_cfg.json` (the
shipped default is shown here):

```json
{
  "query_pagination_pit_keep_alive": "5m"
}
```

A PIT is a snapshot: documents indexed after it was opened are visible only
after starting a new session.

Close a session as soon as it is no longer needed:

```http
DELETE /query_raw_paginate?operation_id=<operation-id>
Content-Type: application/json

{"pit_id": "…"}
```

The endpoint applies the same operation ACL as the query and returns
`{"closed": true}` in JSend `data`. Closure is idempotent from the client's
perspective. If OpenSearch has already expired the snapshot, pagination returns
HTTP `410` with error type `PaginationSnapshotExpired`; clients must discard
their cursors and open a new PIT. The backend does not retry against a different
snapshot.

## queuing and preview behavior

- Non-preview queries are enqueued to the Redis task queue and processed by worker processes. Results and progress are streamed back to the caller over the websocket identified by `ws_id`.
- `preview_mode` queries are executed synchronously and the HTTP response contains a small sample of results directly (no websocket events).

This queuing model ensures low-latency preview responses while offloading heavier or multi-query workloads to the worker pool for robust, scalable processing.

## debugging query issues

for advanced debugging or to understand exactly what is being executed, be sure to run gulp with debug logs enabled using `--log-level debug` command line paramter!
