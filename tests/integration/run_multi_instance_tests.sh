#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
GULP_BIN="$ROOT_DIR/.venv/bin/gulp"
PYTEST_BIN="$ROOT_DIR/.venv/bin/pytest"
PRIMARY_URL="${GULP_BASE_URL:-http://localhost:8080}"
SECONDARY_URL="${GULP_SECOND_BASE_URL:-http://localhost:8100}"
QUIET="${QUIET:-0}"
TEST_FILES=(
  "$SCRIPT_DIR/test_multi_instance_query_routing.py"
  "$SCRIPT_DIR/test_multi_instance_collab_broadcast.py"
  "$SCRIPT_DIR/test_multi_instance_client_data.py"
)

start_gulp() {
  local __pid_var="$1"
  local log_file="$2"
  local label="$3"
  shift 3
  if [[ "$QUIET" == "1" ]]; then
    env XDG_CACHE_HOME="${XDG_CACHE_HOME:-/tmp/gulp-cache}" "$@" \
      >>"$log_file" 2>&1 &
  else
    env XDG_CACHE_HOME="${XDG_CACHE_HOME:-/tmp/gulp-cache}" "$@" \
      > >(stdbuf -oL -eL sed -u "s/^/[${label}] /" | tee -a "$log_file") \
      2> >(stdbuf -oL -eL sed -u "s/^/[${label}] /" | tee -a "$log_file" >&2) &
  fi
  printf -v "$__pid_var" '%s' "$!"
}

wait_for_docs() {
  local url="$1"
  local label="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url/docs" >/dev/null; then
      echo "$label ready: $url"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $label at $url" >&2
  return 1
}

PRIMARY_PID=""
SECONDARY_PID=""
PRIMARY_LOG=""
SECONDARY_LOG=""

cleanup() {
  local status=$?
  if [[ -n "${PRIMARY_PID}" ]]; then
    kill "${PRIMARY_PID}" >/dev/null 2>&1 || true
    wait "${PRIMARY_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${SECONDARY_PID}" ]]; then
    kill "${SECONDARY_PID}" >/dev/null 2>&1 || true
    wait "${SECONDARY_PID}" >/dev/null 2>&1 || true
  fi
  if [[ $status -ne 0 ]]; then
    echo "primary log: ${PRIMARY_LOG}" >&2
    echo "secondary log: ${SECONDARY_LOG}" >&2
  fi
  return "$status"
}

trap cleanup EXIT INT TERM

PRIMARY_LOG="$(mktemp /tmp/gulp-primary-XXXX.log)"
SECONDARY_LOG="$(mktemp /tmp/gulp-secondary-XXXX.log)"

echo "starting primary gulp on ${PRIMARY_URL}"
start_gulp PRIMARY_PID "$PRIMARY_LOG" "primary" "$GULP_BIN" --reset-collab --create test_operation

echo "starting secondary gulp on ${SECONDARY_URL}"
start_gulp SECONDARY_PID "$SECONDARY_LOG" "secondary" env GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 "$GULP_BIN"

wait_for_docs "$PRIMARY_URL" "primary"
wait_for_docs "$SECONDARY_URL" "secondary"

echo "running multi-instance tests"
if [[ "$QUIET" == "1" ]]; then
  echo "QUIET=1 enabled: instance logs are captured to temp files only"
fi
GULP_BASE_URL="$PRIMARY_URL" GULP_SECOND_BASE_URL="$SECONDARY_URL" \
  "$PYTEST_BIN" -q "${TEST_FILES[@]}"

echo "multi-instance tests passed"
