#!/usr/bin/env bash
docker compose down
sudo rm -rf opensearch_data && sudo rm -rf postgres_data
rm -rf ~/.config/gulp/.first_run_done
docker compose up -d
