#!/usr/bin/env bash
set -euo pipefail

# Start syslog daemon when available; continue if startup fails.
if command -v rsyslogd >/dev/null 2>&1; then
    mkdir -p /run/rsyslog
    if ! pgrep -x rsyslogd >/dev/null 2>&1; then
        rsyslogd || true
    fi
fi

# Drop privileges to runtime user if configured and gosu is available.
if [ -n "${GULP_RUNTIME_USER:-}" ] && [ "$(id -u)" = "0" ] && command -v gosu >/dev/null 2>&1; then
    exec gosu "${GULP_RUNTIME_USER}" "$@"
fi

exec "$@"
