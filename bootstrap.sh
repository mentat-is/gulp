#!/usr/bin/env bash
# download bootstrap files
echo "[.] Downloading bootstrap files..."
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/.env -o ./.env
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/docker-compose.yml -o ./docker-compose.yml
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/gulp_cfg_template.json -o ./gulp_cfg.json

# edit config as you wish, then
echo "[.] running gulp with default config..."
GULP_CONFIG_PATH=./gulp_cfg.json docker compose --profile full up
