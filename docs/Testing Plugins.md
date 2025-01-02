- [testing gulp](#testing-gulp)
  - [running the test suite](#running-the-test-suite)
  - [running single tests manually](#running-single-tests-manually)
  - [ingestion tool](#ingestion-tool)

[TOC]

# testing gulp

> `GULP_INTEGRATION_TEST` should always be set during tests, since it deactivates debug options even if they are activated in the configuration!

to start tests, start gulp first!

~~~bash
# run gulp on localhost:8080
# (extra paths may be omitted)
GULP_INTEGRATION_TEST=1 PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/mapping_files PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins gulp
~~~

## running the test suite

the test suite tests all the gulp rest API and plugins, including ingestion and query (checking the results too)

~~~bash
# run test suite (covers the whole API, including ingestion and query)
# (PAID_PLUGINS and extra paths may be omitted)
cd tests
PAID_PLUGINS=1 PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/mapping_files PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins ./test_suite.sh

# also test paid plugins
PAID_PLUGINS=1 ./test_suite.sh
~~~

## running single tests manually

single tests in the [test suite](../tests) may also be run manually

~~~bash
# run single api test manually, i.e.
# run windows ingest/query test (including sigma and stored queries)
python3 -m pytest query.py::test_win_evtx

# run collab notes test (including user ACL)
python3 -m pytest note.py
~~~

## ingestion tool

to quickly test ingestion with a particular plugin manually i.e. during plugin dev, you may use [ingest.py](../test_scripts/ingest.py):

> ingest_py script does not check for upload correctness, you must do it manually!

~~~bash
# win_evtx
# 98633 records, 1 record failed, 1 skipped, 98631 ingested
./test_scripts/ingest.py --path ./samples/win_evtx

# csv without mapping
# 10 records, 10 ingested
./test_scripts/ingest.py --path ./samples/mftecmd/sample_record.csv --plugin csv --plugin_params '{"mappings": {
      "test_mapping": {
        "fields": {
          "Created0x10": {
            "ecs": "@timestamp",
            "is_timestamp_chrome": false
          }
        }
      }
    }
  }'
~~~
