#!/usr/bin/env bash
# download bootstrap files
echo "[.] Downloading bootstrap files..."
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/.env -o ./.env
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/docker-compose.yml -o ./docker-compose.yml
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/gulp_cfg_template.json -o ./gulp_cfg.json
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/run_gulp.sh -o ./run_gulp.sh

# edit config as you wish, then
echo "[.] running gulp with default config..."
chmod 755 ./run_gulp.sh
GULP_CONFIG_PATH=./gulp_cfg.json ./run_gulp.sh
