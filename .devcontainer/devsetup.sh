#!/bin/bash

# set up permissions for docker socket
echo "[.] Setting up docker permissions"
sudo chmod 666 /var/run/docker.sock

# set permission for pip cache
sudo mkdir -p /home/vscode/.cache/pip && sudo chown -R vscode:vscode /home/vscode/.cache

# install development packages
echo "[.] Installing gulp"
pip3 install --timeout=1000 -e .
if [ $? -ne 0 ]; then
	echo "[-] Failed to install gulp"
	exit 1
fi

echo "[.] Installing muty-ptyhon"
pip3 install --timeout=1000 -e ./muty-python
if [ $? -ne 0 ]; then
	echo "[-] Failed to install muty-python"
	exit 1
fi

echo "[.] Installing gulp sdk (for tests, bridges, ...)"
pip3 install --timeout=1000 -e ./gulp-sdk-python
if [ $? -ne 0 ]; then
	echo "[-] Failed to install gulp-sdk-python"
	exit 1
fi

echo "[.] development environment setup complete"
