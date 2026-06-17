#!/usr/bin/env bash
set -euo pipefail

#!/usr/bin/env bash
set -euo pipefail

docker exec \
  -u vscode \
  -it \
  --workdir /gulp \
  -e VIRTUAL_ENV=/gulp/.venv \
  -e PATH=/gulp/.venv/bin:/home/vscode/.pyenv/shims:/home/vscode/.pyenv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  gulp_devcontainer-dev-1 \
  zsh -i
