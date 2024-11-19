#!/usr/bin/env bash
docker compose down
docker volume rm --force gulp_opensearch_data
docker volume rm --force gulp_postgres_data
sudo rm -rf ./opensearch_data && sudo rm -rf ./postgres_data
rm -rf ~/.config/gulp/.first_run_done
#docker compose up -d
