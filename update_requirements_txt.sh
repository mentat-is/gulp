#!/usr/bin/env bash
_requirements_file="requirements.txa"
echo "[.] Updating requirements.txt ..."

# Get pip freeze output, remove -e lines
pip3 freeze | grep -v "^-e" > $_requirements_file

echo "[.] Done. Updated requirements.txt can be committed now!"
echo "-----------------------------------"
cat $_requirements_file