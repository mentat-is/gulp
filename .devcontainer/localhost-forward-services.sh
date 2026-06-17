#!/usr/bin/env bash
set -euo pipefail

target_host="${GULP_DOCKER_HOST:-host.docker.internal}"
log_dir="/tmp/gulp-localhost-forwards"
mkdir -p "${log_dir}"

forward_port() {
	local port="$1"

	if ss -H -ltn "sport = :${port}" | grep -q .; then
		echo "[.] localhost:${port} is already listening, skipping"
		return 0
	fi

	echo "[.] forwarding localhost:${port} -> ${target_host}:${port}"
	setsid -f socat \
		"TCP-LISTEN:${port},fork,reuseaddr,bind=127.0.0.1" \
		"TCP:${target_host}:${port}" \
		>"${log_dir}/${port}.log" 2>&1 &
}

for port in 5432 6379 9000 9001 9200; do
	forward_port "${port}"
done
