#!/usr/bin/env bash
docker exec -u vscode -ti gulp_devcontainer-dev-1 /bin/bash -c "cd gulp && source .venv/bin/activate && exec bash"

