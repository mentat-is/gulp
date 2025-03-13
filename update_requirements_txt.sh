#!/usr/bin/env bash
# this is to be run in the root of the project
# it updates the requirements.txt file with the current installed packages and adds the muty package to it
echo "[.] Updating requirements.txt"

# Get pip freeze output, remove -e lines, and add muty package
pip3 freeze | grep -v "^-e" > requirements.txt
#echo "muty@git+https://github.com/mentat-is/muty-python.git@develop" >> requirements.txt

echo "[.] Done. Updated requirements.txt:"
echo "-----------------------------------"
cat requirements.txt
