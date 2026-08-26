- [querying gulp](#querying-gulp)
  - [overview](#overview)
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

- **`query_raw_paginate`**: similar to `query_raw` but `synchronous` and with simple pagination using `limit` and `offset` parameters: useful for advanced users who want to issue paginated queries and directly get the results without the overhead of background processing.
  
- **`query_sigma`**: accepts [Sigma rules](https://github.com/SigmaHQ/sigma), translates them to OpenSearch queries, and executes them: useful for threat detection and security analytics.

- **`query_external`**: for querying external sources via plugins: relies on plugin-specific logic to query the external source (i.e. a SIEM) using its specific DSL/API.

> **NOTE:** except for `query_external`, all the above endpoints ultimately resolve to `query_raw` internally.

## queuing and preview behavior

- Non-preview queries are enqueued to the Redis task queue and processed by worker processes. Results and progress are streamed back to the caller over the websocket identified by `ws_id`.
- `preview_mode` queries are executed synchronously and the HTTP response contains a small sample of results directly (no websocket events).

This queuing model ensures low-latency preview responses while offloading heavier or multi-query workloads to the worker pool for robust, scalable processing.

### bounded web UI streaming

The scalable web UI advertises a `query_chunk_ack_window` in the optional data
of its authenticated `/ws` connection. When present, raw queries use a sliding
window per active child query instead of producing an unbounded sequence of
`docs_chunk` messages. The UI acknowledges a chunk after it has released the
raw JSON into its bounded normalization queue. For a terminal single-query
chunk, the backend waits for a durable acknowledgement before emitting
`query_done`.

This negotiation is optional. WebSocket clients that do not advertise it keep
the existing push behavior and are never required to send acknowledgements.
Cancellation while a worker is waiting for an acknowledgement produces the
normal `canceled` query result rather than an execution error.

## debugging query issues

for advanced debugging or to understand exactly what is being executed, be sure to run gulp with debug logs enabled using `--log-level debug` command line paramter!
