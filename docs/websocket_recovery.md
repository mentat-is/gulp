# Websocket Recovery Contract

Redis pub/sub is a best-effort delivery channel in gULP. Websocket clients must
not treat it as a durable event log. Clients recover from disconnects by
refreshing the durable source for the event type.

## Recovery Matrix

| Event type | Durable source | Recovery action |
| --- | --- | --- |
| `stats_create` | PostgreSQL `request_stats` | Call `/request_get_by_id` for the `req_id`, or `/request_list` for the operation after reconnect. |
| `stats_update` | PostgreSQL `request_stats` | Call `/request_get_by_id` for the `req_id`; terminal status in request stats is authoritative. |
| `query_done` | PostgreSQL `request_stats` for request final state | Call `/request_get_by_id`; per-query notification ordering is not resent. |
| `ingest_source_done` | PostgreSQL `request_stats` plus collab source/context state | Call `/request_get_by_id` and refresh operation sources/contexts if the UI needs source lists. |
| `ingest_raw_progress` | PostgreSQL `request_stats` | Call `/request_get_by_id`; raw progress packets are not resent individually. |
| `rebase_done` | PostgreSQL `request_stats` | Call `/request_get_by_id`; rebase completion state is authoritative there. |
| `collab_create`, `collab_update`, `collab_delete` | PostgreSQL collab tables | Refresh visible operation collab state after reconnect. |
| `docs_chunk` | None | Treat as volatile. Rerun or repaginate the query/ingest-derived view instead of expecting resend. |
| `client_data` | None | Treat as volatile UI-to-UI traffic. The sender/client protocol must provide its own acknowledgement if needed. |
| `ws_error` | None, unless it also updates request stats | If tied to a `req_id`, call `/request_get_by_id`; otherwise surface the error and reconnect. |
| `ws_connected` | Current websocket connection only | Not resent. Re-authenticate and resubscribe filters on reconnect. |
| `user_login`, `user_logout` | Current user/session state | Refresh user/session state if the UI depends on presence information. |

## Client Reconnect Procedure

On reconnect, clients should:

1. Open a new websocket and send the normal auth packet.
2. Reapply any message `types` and `operation_ids` filters used by that socket.
3. Call `/request_list` for each visible operation.
4. For any request still visible in the UI, call `/request_get_by_id` if its
   last known status was not terminal.
5. Refresh operation collab state.
6. Restart or repaginate any view that depended on missed `docs_chunk` packets.

## Backend Guarantees

- Request terminal state is authoritative in `request_stats`, not in websocket
  delivery.
- Large payload pointers are temporary and TTL-bound. If pointer resolution
  fails, clients must recover through the request/query API rather than waiting
  for old packets to be resent.
