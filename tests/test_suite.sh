#!/usr/bin/env bash
if [ "$1" == "--help" ]; then
    echo "Usage: [PAID_PLUGINS=1 to include paid plugins] test_suite.sh"
    exit 1
fi
python3 -m pytest -v -s tests/ingest
python3 -m pytest -v -s tests/query
if [ "$PAID_PLUGINS" = "1" ]; then
    python3 -m pytest -v -s ../gulp-paid-plugins/tests/ingest
    python3 -m pytest -v -s ../gulp-paid-plugins/tests/query
fi
