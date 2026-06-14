#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_multi_instance_tests.sh [--help|-h]

Run the multi-instance integration tests by starting two local gULP instances
(primary on GULP_BASE_URL / default http://localhost:8080 and secondary on
GULP_SECOND_BASE_URL / default http://localhost:8100), waiting for both
instances to expose /docs, and then executing the three integration tests in
/tests/integration. The multi-instance tests drive actors on both instances
concurrently for query, websocket client-data, and collaboration routing.

Options:
  -h, --help   Show this help message and exit.

Environment variables:
  GULP_BASE_URL
      Primary instance URL to start against. Default: http://localhost:8080
  GULP_SECOND_BASE_URL
      Secondary instance URL. Default: http://localhost:8100
  QUIET
      If set to 1, suppress live instance log streaming and capture logs to
      temp files only. Default: 0.
  GULP_MULTI_INSTANCE_SOAK
      If set to 1, enable soak-mode defaults for pointer stress testing.
  GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT
      Number of pointer-stress fanout iterations in soak mode.
      Default: 20 when SOAK=1.
  GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS
      Duration in seconds for soak-mode pointer stress.
      Default: 0 when SOAK=1. Set this explicitly for time-based soak runs.
  GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT
      Number of large-note collab broadcast iterations.
      Default: 1.
  GULP_MULTI_INSTANCE_RAW_INGEST_DOCS
      Number of raw documents each instance ingests during multi-instance tests.
      Default: 25, or 500 when SOAK=1.
  PROMETHEUS_URL
      Optional Prometheus URL used for observability preflight output.
      Default: http://localhost:9090
  XDG_CACHE_HOME
      Cache directory passed to each gULP process. Default: /tmp/gulp-cache
  GULP_MULTI_INSTANCE_PYTEST_TIMEOUT
      Optional timeout command duration for the pytest run, for example 10m.
      When unset, pytest is not wrapped in a process-level timeout.
EOF
}

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
GULP_BIN="$ROOT_DIR/.venv/bin/gulp"
PYTEST_BIN="$ROOT_DIR/.venv/bin/pytest"
PRIMARY_URL="${GULP_BASE_URL:-http://localhost:8080}"
SECONDARY_URL="${GULP_SECOND_BASE_URL:-http://localhost:8100}"
SOAK="${GULP_MULTI_INSTANCE_SOAK:-0}"
QUIET="${QUIET:-0}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"
url_port() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlparse

parsed = urlparse(sys.argv[1])
if parsed.port is not None:
    print(parsed.port)
elif parsed.scheme == "https":
    print(443)
else:
    print(80)
PY
}

PRIMARY_PORT="$(url_port "$PRIMARY_URL")"
SECONDARY_PORT="$(url_port "$SECONDARY_URL")"
TEST_FILES=(
  "$SCRIPT_DIR/test_multi_instance_query_routing.py"
  "$SCRIPT_DIR/test_multi_instance_collab_broadcast.py"
  "$SCRIPT_DIR/test_multi_instance_client_data.py"
)

if [[ "$SOAK" == "1" ]]; then
  export GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT="${GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT:-20}"
  export GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS="${GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS:-0}"
  export GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT="${GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT:-1}"
  export GULP_MULTI_INSTANCE_RAW_INGEST_DOCS="${GULP_MULTI_INSTANCE_RAW_INGEST_DOCS:-500}"
fi

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

check_metrics_endpoint() {
  local url="$1"
  local label="$2"
  local metrics_url="${url%/}/metrics"
  local tmp_file
  tmp_file="$(mktemp /tmp/gulp-metrics-${label}-XXXX.txt)"
  if curl -fsS "$metrics_url" -o "$tmp_file"; then
    if grep -q '^gulp_' "$tmp_file"; then
      echo "$label metrics ready: $metrics_url"
    else
      echo "$label metrics endpoint responded, but no gulp_* metrics were found: $metrics_url" >&2
    fi
  else
    echo "$label metrics unavailable: $metrics_url" >&2
    echo "  enable prometheus_enabled in gulp_cfg.json and restart gULP if Grafana should show live backend metrics" >&2
  fi
  rm -f "$tmp_file"
}

check_prometheus_targets() {
  python3 - "$PROMETHEUS_URL" <<'PY'
import json
import socket
import sys
import urllib.request


def _local_ipv4_candidates() -> list[str]:
    candidates: list[str] = []
    for token in socket.gethostbyname_ex(socket.gethostname())[2]:
        if token.startswith("127."):
            continue
        candidates.append(token)
    try:
        import subprocess

        output = subprocess.check_output(
            ["hostname", "-I"],
            text=True,
            timeout=2,
        )
        for token in output.split():
            if ":" in token or token.startswith("127."):
                continue
            if token not in candidates:
                candidates.append(token)
    except Exception:
        pass
    return candidates


prometheus_url = sys.argv[1].rstrip("/")
try:
    with urllib.request.urlopen(f"{prometheus_url}/api/v1/targets", timeout=5) as response:
        payload = json.loads(response.read().decode())
except Exception as exc:
    print(f"Prometheus preflight unavailable at {prometheus_url}: {exc}", file=sys.stderr)
    return_code = 0
else:
    targets = payload.get("data", {}).get("activeTargets", [])
    gulp_targets = [
        target
        for target in targets
        if target.get("labels", {}).get("job") == "gulp"
    ]
    if not gulp_targets:
        print(
            f"Prometheus preflight warning: no active job=\"gulp\" targets at {prometheus_url}",
            file=sys.stderr,
        )
    else:
        print("Prometheus gulp targets:")
        unhealthy = []
        for target in gulp_targets:
            labels = target.get("labels", {})
            instance = labels.get("instance", "<unknown>")
            health = target.get("health", "<unknown>")
            scrape_url = target.get("scrapeUrl", "")
            last_error = target.get("lastError") or ""
            suffix = f" error={last_error}" if last_error else ""
            print(f"  {instance} {health} {scrape_url}{suffix}")
            if health != "up":
                unhealthy.append(target)
        scraped_ports = {
            labels.get("instance", "").rsplit(":", 1)[-1]
            for labels in (target.get("labels", {}) for target in gulp_targets)
        }
        missing = {"8080", "8100"} - scraped_ports
        if missing:
            print(
                "Prometheus preflight warning: sample multi-instance run uses ports "
                f"8080 and 8100, but these ports are not both scraped: missing {sorted(missing)}",
                file=sys.stderr,
            )
            print(
                "  update prometheus.yml and restart Prometheus to see both instances in Grafana",
                file=sys.stderr,
            )
        if unhealthy:
            candidates = _local_ipv4_candidates()
            if candidates:
                print(
                    "Prometheus preflight hint: local /metrics may be reachable "
                    "from this shell while Prometheus cannot reach host.docker.internal.",
                    file=sys.stderr,
                )
                print(
                    "  If gULP is running inside a devcontainer, set prometheus.yml targets to one of:",
                    file=sys.stderr,
                )
                for candidate in candidates[:4]:
                    print(
                        f"    - {candidate}:8080 and {candidate}:8100",
                        file=sys.stderr,
                    )
                print(
                    "  then restart Prometheus: docker compose --profile metrics restart prometheus",
                    file=sys.stderr,
                )
return_code = 0
raise SystemExit(return_code)
PY
}

PRIMARY_PID=""
SECONDARY_PID=""
PRIMARY_LOG=""
SECONDARY_LOG=""
TEST_LOG=""
RESULT_STATUS="not_started"
RESULT_DETAIL=""
PYTEST_EXIT=""
START_TS="$(date +%s)"
PHASE="initializing"

print_summary() {
  local status="$1"
  local end_ts
  local duration
  end_ts="$(date +%s)"
  duration=$((end_ts - START_TS))

  echo
  echo "multi-instance test summary"
  echo "  status: ${RESULT_STATUS}"
  if [[ -n "$RESULT_DETAIL" ]]; then
    echo "  detail: ${RESULT_DETAIL}"
  fi
  echo "  exit_code: ${status}"
  if [[ -n "$PYTEST_EXIT" ]]; then
    echo "  pytest_exit_code: ${PYTEST_EXIT}"
  fi
  echo "  duration_seconds: ${duration}"
  echo "  primary_url: ${PRIMARY_URL}"
  echo "  secondary_url: ${SECONDARY_URL}"
  echo "  prometheus_url: ${PROMETHEUS_URL}"
  echo "  primary_log: ${PRIMARY_LOG}"
  echo "  secondary_log: ${SECONDARY_LOG}"
  if [[ -n "$TEST_LOG" ]]; then
    echo "  pytest_log: ${TEST_LOG}"
  fi
  echo "  tests:"
  printf '    - %s\n' "${TEST_FILES[@]}"

  if [[ -n "$TEST_LOG" && -s "$TEST_LOG" ]] && grep -Eiq 'timeout|timed out|TimeoutError' "$TEST_LOG"; then
    echo "  timeout_indicators:"
    grep -Ein 'timeout|timed out|TimeoutError' "$TEST_LOG" | tail -n 20 | sed 's/^/    /'
  fi
}

cleanup() {
  local status=$?
  if [[ "$RESULT_STATUS" == "not_started" ]]; then
    RESULT_STATUS="failed"
    RESULT_DETAIL="failed during ${PHASE}"
  fi
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
  print_summary "$status"
  return "$status"
}

trap cleanup EXIT INT TERM

PRIMARY_LOG="$(mktemp /tmp/gulp-primary-XXXX.log)"
SECONDARY_LOG="$(mktemp /tmp/gulp-secondary-XXXX.log)"
TEST_LOG="$(mktemp /tmp/gulp-multi-instance-tests-XXXX.log)"

echo "starting primary gulp on ${PRIMARY_URL}"
PHASE="starting primary"
start_gulp PRIMARY_PID "$PRIMARY_LOG" "primary" env GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT="$PRIMARY_PORT" "$GULP_BIN" --reset-collab --create test_operation

echo "starting secondary gulp on ${SECONDARY_URL}"
PHASE="starting secondary"
start_gulp SECONDARY_PID "$SECONDARY_LOG" "secondary" env GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT="$SECONDARY_PORT" "$GULP_BIN"

PHASE="waiting for primary readiness"
if ! wait_for_docs "$PRIMARY_URL" "primary"; then
  RESULT_STATUS="startup_timeout"
  RESULT_DETAIL="primary did not expose /docs within 60 seconds"
  exit 1
fi

PHASE="waiting for secondary readiness"
if ! wait_for_docs "$SECONDARY_URL" "secondary"; then
  RESULT_STATUS="startup_timeout"
  RESULT_DETAIL="secondary did not expose /docs within 60 seconds"
  exit 1
fi

check_metrics_endpoint "$PRIMARY_URL" "primary"
check_metrics_endpoint "$SECONDARY_URL" "secondary"
check_prometheus_targets

echo "running multi-instance tests"
if [[ "$QUIET" == "1" ]]; then
  echo "QUIET=1 enabled: instance logs are captured to temp files only"
fi
echo "large-payload fanout count=${GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT:-3} soak_seconds=${GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS:-0} collab_count=${GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT:-1} raw_ingest_docs=${GULP_MULTI_INSTANCE_RAW_INGEST_DOCS:-25}"
PHASE="running pytest"
set +e
if [[ -n "${GULP_MULTI_INSTANCE_PYTEST_TIMEOUT:-}" ]]; then
  GULP_BASE_URL="$PRIMARY_URL" GULP_SECOND_BASE_URL="$SECONDARY_URL" \
    timeout "$GULP_MULTI_INSTANCE_PYTEST_TIMEOUT" "$PYTEST_BIN" -q "${TEST_FILES[@]}" \
    2>&1 | tee "$TEST_LOG"
else
  GULP_BASE_URL="$PRIMARY_URL" GULP_SECOND_BASE_URL="$SECONDARY_URL" \
    "$PYTEST_BIN" -q "${TEST_FILES[@]}" \
    2>&1 | tee "$TEST_LOG"
fi
PYTEST_EXIT="${PIPESTATUS[0]}"
set -e

if [[ "$PYTEST_EXIT" -ne 0 ]]; then
  if [[ "$PYTEST_EXIT" -eq 124 ]] || grep -Eiq 'timeout|timed out|TimeoutError' "$TEST_LOG"; then
    RESULT_STATUS="timeout"
    RESULT_DETAIL="pytest failed with timeout indicators"
  else
    RESULT_STATUS="failed"
    RESULT_DETAIL="pytest failed"
  fi
  exit "$PYTEST_EXIT"
fi

RESULT_STATUS="succeeded"
RESULT_DETAIL="pytest completed successfully"
echo "multi-instance tests passed"
