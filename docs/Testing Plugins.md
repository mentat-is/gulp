- [testing gulp](#testing-gulp)
  - [running the test suite](#running-the-test-suite)
  - [running single tests manually](#running-single-tests-manually)
  - [ingestion tool](#ingestion-tool)

# testing gulp

> `GULP_INTEGRATION_TEST` should always be set during tests, since it deactivates debug options even if they are activated in the configuration!

to start tests, start gulp first!

> for the devteam: if you are using the `devcontainer`, environment variablaes `PATH_MAPPING_FILES_EXTRA` and `PATH_PLUGINS_EXTRA` are preset with to the paid plugins paths in `$GULP_DIR/../gulp-paid-plugins`.

~~~bash
# run gulp on localhost:8080
# (extra paths may be omitted)
GULP_INTEGRATION_TEST=1 PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins gulp
~~~

## running the test suite

the test suite tests all the gulp rest API and plugins, including ingestion and query (checking the results too)

~~~bash
# run test suite (covers the whole API, including ingestion and query)
cd tests
PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files ./test_suite.sh

# also test paid plugins
PAID_PLUGINS=1 PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins ./test_suite.sh

> setting `PATH_PLUGIN_EXTRA` and `PATH_MAPPING_FILES_EXTRA` is not necessary when using the devcontainer to test.
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

> ingest_py script will spawn CURL processes, and exits once it detects the ingestion is done on the websocket.

~~~bash
# win_evtx
# 98633 records, 1 record failed, 1 skipped, 98632 ingested
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
