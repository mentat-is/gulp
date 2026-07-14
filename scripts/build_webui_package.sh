#!/usr/bin/env bash
set -euo pipefail

backend_version="$(python -c 'from setuptools_scm import get_version; print(get_version(root="."))' 2>/dev/null || true)"
if [ -z "$backend_version" ]; then
  backend_version="$(PYTHONPATH=src python -c 'from gulp import __version__; print(__version__)' 2>/dev/null || true)"
fi
if [ -z "$backend_version" ] || [ "$backend_version" = "0.0.0" ]; then
  backend_version="$(git describe --tags --always)"
fi

pnpm --dir gulp-ui install --frozen-lockfile --config.confirmModulesPurge=false
BACKEND_VERSION="$backend_version" pnpm --dir gulp-ui run build
rm -rf src/gulp/webui
cp -R gulp-ui/build src/gulp/webui
