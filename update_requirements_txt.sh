#!/usr/bin/env bash

echo "[.] Updating requirements.txt"

# Get pip freeze output, remove -e lines, and add muty package
pip3 freeze | grep -v "^-e" > requirements.txt
echo "muty@git+https://github.com/mentat-is/muty-python.git@refactor" >> requirements.txt

echo "[.] Done. Updated requirements.txt:"
echo "-----------------------------------"
cat requirements.txt