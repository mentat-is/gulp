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
    runtime_home="$(getent passwd "$GULP_RUNTIME_USER" | cut -d: -f6)"
    runtime_uid="$(id -u "$GULP_RUNTIME_USER")"
    runtime_gid="$(id -g "$GULP_RUNTIME_USER")"
    working_dir="${GULP_WORKING_DIR:-$runtime_home/.config/gulp}"

    if [ -z "$runtime_home" ] || [ "$working_dir" = "/" ]; then
        echo "Invalid working directory for GULP_RUNTIME_USER=$GULP_RUNTIME_USER" >&2
        exit 1
    fi

    mkdir -p "$working_dir"
    # Docker can create bind-mount sources as root. Repair only when a mount
    # root has the wrong owner, so large working directories are not scanned
    # on every restart.
    for directory in "$working_dir" "$working_dir/certs" "$runtime_home/.local" "$runtime_home/.cache" "$runtime_home/.cache/pip" "$runtime_home"/.local/lib/python*/site-packages; do
        if [ -d "$directory" ] && [ "$(stat -c '%u:%g' "$directory")" != "$runtime_uid:$runtime_gid" ]; then
            chown -R "$runtime_uid:$runtime_gid" "$directory"
        fi
    done

    exec gosu "$GULP_RUNTIME_USER" "$@"
else
    exec "$@" # execute params set in last Dockerfile CMD
fi
