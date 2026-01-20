#!/usr/bin/env bash
# this script runs all test_*.py files in the given directory and its subdirectories,
# or runs a list of test files if provided as arguments

run_test() {
  testarg="$1"
  # extract file path before :: if present
  echo "[.] running test: $testarg"
  python3 -m pytest -v -s -x "$testarg"
  if [ $? -ne 0 ]; then
    echo "[FAILED] test: $testarg"
    exit 1
  fi
}

# if no arguments, print usage
if [ "$#" -eq 0 ]; then
  echo "usage: $0 <directory> to run all test files in the given directory"
  echo "   or $0 <test1.py> [test2.py ...] to run specific test files"
  exit 1
fi

all_args_are_tests=true
for arg in "$@"; do
  testfile="${arg%%::*}"
  if [[ ! "$testfile" =~ test_.*\.py$ ]] || [ ! -f "$testfile" ]; then
    all_args_are_tests=false
    break
  fi
done

if [ "$all_args_are_tests" = true ]; then
  for testarg in "$@"; do
    run_test "$testarg"
  done
  echo "[DONE] all tests succeeded!"
  exit 0
fi

# fallback: run all tests in the given directory
find "$1" -type f -name 'test_*.py' | while read -r testfile; do
  run_test "$testfile"
done

echo "[DONE] all tests succeeded!"
