#!/usr/bin/env bash
set -euo pipefail

# Startup model:
# 1) Container starts as root so rsyslogd can initialize /run/rsyslog.
# 2) After bootstrap, main process runs as GULP_RUNTIME_USER via gosu.
# Keep compose aligned with this by starting service as root and setting
# GULP_RUNTIME_USER (default: gulp).
# Start syslog daemon before dropping privileges.
if command -v rsyslogd >/dev/null 2>&1; then
    if ! pgrep -x rsyslogd >/dev/null 2>&1; then
        if [ "$(id -u)" != "0" ]; then
            echo "rsyslogd must be started as root before dropping privileges; remove the container user override." >&2
            exit 1
        fi

        mkdir -p /run/rsyslog
        rsyslogd
    fi
fi

# Drop privileges to runtime user if configured and gosu is available.
if [ -n "${GULP_RUNTIME_USER:-}" ] && [ "$(id -u)" = "0" ] && command -v gosu >/dev/null 2>&1; then
    exec gosu "${GULP_RUNTIME_USER}" "$@"
fi

exec "$@"
