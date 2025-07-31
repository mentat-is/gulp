#!/usr/bin/env bash
echo "[.] cleaning up opensearch and postgresql containers"
docker compose --profile gulp down
docker compose --profile dev down
docker volume rm --force gulp_opensearch_data
docker volume rm --force gulp_postgres_data

echo "[.] reset first run"

_WORKING_DIR="${GULP_WORKING_DIR:-$HOME/.config/gulp}"
if [ ! -d "$_WORKING_DIR" ]; then
  echo "[!] GULP_WORKING_DIR does not exist: $_WORKING_DIR"
  exit 1
fi
rm $_WORKING_DIR/.first_run_done
echo "[.] done"
