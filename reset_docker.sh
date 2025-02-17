#!/usr/bin/env bash
echo "[.] cleaning up opensearch and postgresql containers"
docker compose --profile gulp down
docker compose --profile dev down
docker volume rm --force gulp_opensearch_data
docker volume rm --force gulp_postgres_data

echo "[.] reset gulp docker configuration"
sudo rm -rf ./gulpconfig

#docker compose up -d
