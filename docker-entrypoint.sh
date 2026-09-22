#!/usr/bin/env bash
set -euo pipefail

# Startup model:
# 1) Container starts as root so rsyslogd can initialize /run/rsyslog, and so
#    it can fix ownership of the bind-mounted working directory (see below).
# 2) After bootstrap, main process runs as GULP_RUNTIME_USER via gosu.
# Keep compose aligned with this by starting service as root and setting
# GULP_RUNTIME_USER (default: gulp).

# Docker creates a bind-mounted host path that doesn't exist yet as root,
# regardless of this container's own user — independently of whether the
# container itself ever runs anything as root. If GULP_WORKING_DIR (default
# ~/.config/gulp) or any path under it was auto-created this way on an
# earlier run, GULP_RUNTIME_USER would be unable to write to it after the
# privilege drop below (e.g. GulpConfig failing to create the logs
# directory). Since we're already root at this point, reclaim ownership of
# the working directory before dropping privileges. Best-effort: never abort
# startup over it, the app's own directory/file creation still fails loudly
# if something remains genuinely unwritable.
if [ "$(id -u)" = "0" ] && [ -n "${GULP_RUNTIME_USER:-}" ]; then
    runtime_home="$(getent passwd "${GULP_RUNTIME_USER}" | cut -d: -f6)"
    working_dir="${GULP_WORKING_DIR:-${runtime_home}/.config/gulp}"
    if [ -d "${working_dir}" ]; then
        chown -R "${GULP_RUNTIME_USER}:${GULP_RUNTIME_USER}" "${working_dir}" 2>/dev/null || \
            echo "warning: could not fully chown ${working_dir} to ${GULP_RUNTIME_USER}; some paths under it may remain unwritable" >&2
    fi
fi

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
else
    exec "$@" # execute params set in last Dockerfile CMD
fi
