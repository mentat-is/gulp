#!/usr/bin/env bash
if [ "$1" == "--help" ]; then
    echo "Usage: [PATH_PAID_PLUGINS=/path/to/gulp-paid-plugins to include paid plugins] test_suite.sh"
    exit 1
fi

_TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# default tests
echo ". Running default tests from $_TESTS_DIR ..."
python3 -m pytest -v -x -s $_TESTS_DIR/ingest
if [ $? -ne 0 ]; then
    exit 1
fi
python3 -m pytest -v -x -s $_TESTS_DIR/query
if [ $? -ne 0 ]; then
    exit 1
fi
python3 -m pytest -v -x -s $_TESTS_DIR/test_db.py
if [ $? -ne 0 ]; then
    exit 1
fi
python3 -m pytest -v -x -s $_TESTS_DIR/test_operation.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_note.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_link.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_highlight.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_story.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_glyph.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_user.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_user_group.py
if [ $? -ne 0 ]; then
    exit 1
fi

python3 -m pytest -v -x -s $_TESTS_DIR/test_utility.py
if [ $? -ne 0 ]; then
    exit 1
fi

if [ ! -z $PATH_PAID_PLUGINS ]; then
    # paid plugins tests
    echo ". Running paid plugins tests from $PATH_PAID_PLUGINS ..."

    python3 -m pytest -v -x -s $PATH_PAID_PLUGINS/tests/ingest
    if [ $? -ne 0 ]; then
        exit 1
    fi
    python3 -m pytest -v -x -s $PATH_PAID_PLUGINS/tests/query
    if [ $? -ne 0 ]; then
        exit 1
    fi

    python3 -m pytest -v -x -s $PATH_PAID_PLUGINS/tests/test_stored_query.py
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

# all tests passed
echo "TESTS PASSED!"
