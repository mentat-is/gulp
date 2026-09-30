#!/bin/bash
# One-time bootstrap for the OpenSearch indices that receive gulp's internal
# (Glug) logs, shipped by Fluent Bit (see fluent-bit.conf and
# docs/logging.md, "Shipping logs to OpenSearch").
#
# Run this once against the target OpenSearch cluster, before starting the
# fluent-bit profile for the first time. Re-running it is safe (idempotent:
# every PUT here either creates the resource or replaces it with the same
# definition).
#
# gulp itself never calls any of this — it doesn't know this index exists,
# by design (see plans/log-shipping.md for why).
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
CURL_AUTH=()
if [[ -n "${OPENSEARCH_USER:-}" ]]; then
    CURL_AUTH=(-u "${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD:-}")
fi

put() {
    local path="$1"
    local body="$2"
    echo "PUT ${path}"
    curl -fsS -X PUT "${OPENSEARCH_URL}${path}" \
        "${CURL_AUTH[@]}" \
        -H 'Content-Type: application/json' \
        -d "${body}"
    echo
}

# both indices are named with a leading "." — OpenSearch's system-index
# convention (matching e.g. ".opendistro-ism-config" internally): combined
# with "index.hidden": true below, they're excluded from default wildcard
# listings/searches (_cat/indices, a plain "*" query) and queried only by
# exact name/alias — these are gulp's own operational logs, meant for a
# dedicated future gulp plugin doing system-log debugging/analysis, not
# something to stumble into while browsing operation data.

# shared field mapping for both indices — 1:1 with the JSON schema Glug
# writes (src/gulp/glug/formatter.py, GlugJsonFormatter). "detail" is
# free-form (dev_mode-only, decision 16 in plans/log-revolution.md) and
# stored but not indexed, to avoid mapping conflicts across differently
# shaped payloads.
read -r -d '' MAPPINGS <<'EOF' || true
{
  "properties": {
    "timestamp_msec": { "type": "date", "format": "epoch_millis" },
    "level": { "type": "keyword" },
    "process_role": { "type": "keyword" },
    "hostname": { "type": "keyword" },
    "pid": { "type": "long" },
    "thread_id": { "type": "long" },
    "async_task_name": { "type": "keyword" },
    "module_name": { "type": "keyword" },
    "module_path": { "type": "keyword" },
    "func": { "type": "keyword" },
    "line": { "type": "long" },
    "message": { "type": "text" },
    "dev_mode": { "type": "boolean" },
    "context": {
      "properties": {
        "req_id": { "type": "keyword" },
        "operation_id": { "type": "keyword" },
        "user_id": { "type": "keyword" },
        "plugin": { "type": "keyword" },
        "client_ip": { "type": "ip" }
      }
    },
    "hint": { "type": "text" },
    "detail": { "type": "object", "enabled": false },
    "exception": {
      "properties": {
        "type": { "type": "keyword" },
        "message": { "type": "text" },
        "stack": { "type": "text" }
      }
    },
    "@timestamp": { "type": "date" }
  }
}
EOF

# --- INFO+ index: rollover by size/age, delete after 90 days ---

put "/_plugins/_ism/policies/gulp_internal_logs_info_policy" "$(cat <<EOF
{
  "policy": {
    "description": "rollover and 90-day retention for gulp internal logs, level INFO and above",
    "default_state": "rollover",
    "states": [
      {
        "name": "rollover",
        "actions": [
          { "rollover": { "min_size": "20gb", "min_index_age": "1d" } }
        ],
        "transitions": [
          { "state_name": "delete", "conditions": { "min_index_age": "90d" } }
        ]
      },
      { "name": "delete", "actions": [ { "delete": {} } ] }
    ],
    "ism_template": [
      { "index_patterns": [".gulp-internal-logs-info-*"], "priority": 100 }
    ]
  }
}
EOF
)"

put "/_index_template/gulp-internal-logs-info-template" "$(cat <<EOF
{
  "index_patterns": [".gulp-internal-logs-info-*"],
  "template": {
    "settings": {
      "index.hidden": true,
      "plugins.index_state_management.rollover_alias": ".gulp-internal-logs-info"
    },
    "mappings": ${MAPPINGS}
  },
  "priority": 100
}
EOF
)"

put "/.gulp-internal-logs-info-000001" '{
  "aliases": { ".gulp-internal-logs-info": { "is_write_index": true, "is_hidden": true } }
}'

# --- DEBUG index: same rollover shape, delete after 14 days ---
# (only ever populated when gulp runs with --log-level debug/--dev-mode —
# higher volume, much shorter useful lifetime, see plans/log-shipping.md)

put "/_plugins/_ism/policies/gulp_internal_logs_debug_policy" "$(cat <<EOF
{
  "policy": {
    "description": "rollover and 14-day retention for gulp internal logs, level DEBUG",
    "default_state": "rollover",
    "states": [
      {
        "name": "rollover",
        "actions": [
          { "rollover": { "min_size": "20gb", "min_index_age": "1d" } }
        ],
        "transitions": [
          { "state_name": "delete", "conditions": { "min_index_age": "14d" } }
        ]
      },
      { "name": "delete", "actions": [ { "delete": {} } ] }
    ],
    "ism_template": [
      { "index_patterns": [".gulp-internal-logs-debug-*"], "priority": 100 }
    ]
  }
}
EOF
)"

put "/_index_template/gulp-internal-logs-debug-template" "$(cat <<EOF
{
  "index_patterns": [".gulp-internal-logs-debug-*"],
  "template": {
    "settings": {
      "index.hidden": true,
      "plugins.index_state_management.rollover_alias": ".gulp-internal-logs-debug"
    },
    "mappings": ${MAPPINGS}
  },
  "priority": 100
}
EOF
)"

put "/.gulp-internal-logs-debug-000001" '{
  "aliases": { ".gulp-internal-logs-debug": { "is_write_index": true, "is_hidden": true } }
}'

echo "done."
