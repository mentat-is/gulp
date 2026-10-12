#!/usr/bin/env bash
set -euo pipefail

pnpm --dir gulp-ui install --frozen-lockfile --config.confirmModulesPurge=false
pnpm --dir gulp-ui run build
rm -rf src/gulp/webui
cp -R gulp-ui/build src/gulp/webui
