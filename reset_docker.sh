#!/usr/bin/env bash
echo "[.] cleaning up opensearch and postgresql containers"
docker compose --profile gulp down
docker compose --profile dev down
docker volume rm --force gulp_opensearch_data
docker volume rm --force gulp_postgres_data

echo "[.] reset first run"
rm ~/.config/gulp/.first_run_done

#docker compose up -d
