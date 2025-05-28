#!/usr/bin/env bash
_BRANCH="develop"
if [ "$1" == "--release" ]; then
    _BRANCH="master"
elif [ "$1" == "--help" ]; then
    echo "Usage: $0 [--release to use the master branch for muty-python]"
    exit 1
fi

echo "[.] Updating requirements.txt, using muty-python branch: $_BRANCH"
exit 0

# Get pip freeze output, remove -e lines, and add muty package
pip3 freeze | grep -v "^-e" > requirements.txt
echo "muty@git+https://github.com/mentat-is/muty-python.git@develop" >> requirements.txt

echo "[.] Done. Updated requirements.txt:"
echo "-----------------------------------"
cat requirements.txt