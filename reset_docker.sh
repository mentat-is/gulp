#!/usr/bin/env bash
echo "[.] cleaning up opensearch and postgresql containers"
docker compose --profile gulp down
docker compose --profile dev down
docker volume rm --force gulp_opensearch_data
docker volume rm --force gulp_postgres_data
docker volume rm --force gulp_redis_data
docker volume rm --force opensearch_data
docker volume rm --force postgres_data
docker volume rm --force redis_data


echo "[.] reset first run"

_WORKING_DIR="${GULP_WORKING_DIR:-$HOME/.config/gulp}"
if [ ! -d "$_WORKING_DIR" ]; then
  echo "[!] GULP_WORKING_DIR does not exist: $_WORKING_DIR"
  exit 1
fi
rm $_WORKING_DIR/.first_run_done
echo "[.] done"
