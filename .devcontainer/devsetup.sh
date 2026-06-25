#!/bin/bash
set -euo pipefail

VENV_DIR="./.venv"
PYTHON_BIN="python3"

run_privileged_if_available() {
	if [ "$(id -u)" -eq 0 ]; then
		"$@"
	elif command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
		sudo "$@"
	else
		"$@" 2>/dev/null || echo "[!] Skipping privileged command: $*"
	fi
}

# set up permissions for docker socket
echo "[.] Setting up docker permissions"
if [ -S /var/run/docker.sock ]; then
	run_privileged_if_available chmod 666 /var/run/docker.sock
else
	echo "[!] Docker socket not found, skipping socket permissions"
fi

# set permission for pip cache
mkdir -p /home/vscode/.cache/pip || run_privileged_if_available mkdir -p /home/vscode/.cache/pip
run_privileged_if_available chown -R vscode:vscode /home/vscode/.cache

if [ -d "${VENV_DIR}" ] && [ ! -x "${VENV_DIR}/bin/python" ]; then
	echo "[.] Recreating stale virtual environment"
	"${PYTHON_BIN}" -m venv --clear "${VENV_DIR}"
else
	"${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi
source "${VENV_DIR}/bin/activate"
"${PYTHON_BIN}" -m pip install --upgrade pip

# install development packages
echo "[.] Installing gulp"
"${PYTHON_BIN}" -m pip install --timeout=1000 -e .

echo "[.] Installing muty-python"
"${PYTHON_BIN}" -m pip install --timeout=1000 -e ./muty-python

echo "[.] Installing gulp sdk (for tests, bridges, ...)"
"${PYTHON_BIN}" -m pip install --timeout=1000 -e ./gulp-sdk

echo "[.] Installing gulp-cli"
"${PYTHON_BIN}" -m pip install --timeout=1000 -e ./gulp-cli

echo "[.] development environment setup complete"

# install codex cli and the ponytail plugin (then follow https://github.com/DietrichGebert/ponytail docs
# on how to trust the hooks in codex and enable the plugin)
npm install electron
curl -fsSL https://chatgpt.com/codex/install.sh | sh
codex plugin marketplace add DietrichGebert/ponytail

