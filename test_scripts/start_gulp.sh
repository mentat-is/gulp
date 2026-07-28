#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
ui_pid=""
gulp_pid=""

cleanup() {
  local exit_code="${1:-0}"
  trap - EXIT INT TERM

  for pid in "${gulp_pid}" "${ui_pid}"; do
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
    fi
  done

  for pid in "${gulp_pid}" "${ui_pid}"; do
    if [ -n "${pid}" ]; then
      wait "${pid}" 2>/dev/null || true
    fi
  done

  exit "${exit_code}"
}

trap 'cleanup $?' EXIT
trap 'cleanup 130' INT TERM

if [ ! -d "${repo_root}/gulp-ui" ]; then
  echo "[ERROR] missing gulp-ui directory at ${repo_root}/gulp-ui" >&2
  exit 1
fi

if [ ! -f "${repo_root}/.venv/bin/activate" ]; then
  echo "[ERROR] missing virtual environment activation script at ${repo_root}/.venv/bin/activate" >&2
  exit 1
fi

if ! command -v pnpm >/dev/null 2>&1; then
  echo "[ERROR] pnpm is not available in PATH" >&2
  exit 1
fi

(
  cd "${repo_root}/gulp-ui"
  pnpm run start
) < /dev/null &
ui_pid=$!

(
  cd "${repo_root}"
  # shellcheck disable=SC1091
  source "${repo_root}/.venv/bin/activate"
  gulp
) < /dev/null &
gulp_pid=$!

echo "[.] gulp-ui PID: ${ui_pid}"
echo "[.] gulp PID: ${gulp_pid}"
echo "[.] command output is attached to this terminal; press Ctrl+C to stop both"

set +e
wait -n "${ui_pid}" "${gulp_pid}"
wait_status=$?
set -e

exit "${wait_status}"
