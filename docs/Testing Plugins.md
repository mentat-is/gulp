- [testing gulp](#testing-gulp)
  - [running the test suite](#running-the-test-suite)
  - [running single tests manually](#running-single-tests-manually)
  - [ingestion tool](#ingestion-tool)
  - [query external tool](#query-external-tool)

# testing gulp

start gulp first

~~~bash
# run gulp on localhost:8080
# setting extra paths may be omitted if paid plugins are not needed. also, they are automatically set in the devcontainer to ../gulp-paid-plugins/...
export PATH_MAPPING_FILES_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/mapping_files
export PATH_PLUGINS_EXTRA=/home/valerino/repos/gulp-paid-plugins/src/gulp-paid-plugins/plugins

# setting GULP_INTEGRATION_TEST is mandatory when running tests (disables debug features if forgotten activated)
# we also ensure to start in the most clean way (recreate collab db, create test operation, delete all existing data)
GULP_INTEGRATION_TEST=1 gulp --reset-collab --reset test_operation --delete-data
~~~

## running the test suite

the test suite tests all the gulp rest API and plugins, including ingestion and query (checking the results too)

~~~bash
# run test suite (covers the whole API, including ingestion and query)
cd tests
./test_suite.sh

# also test paid plugins
PATH_PAID_PLUGINS=/home/valerino/repos/gulp-paid-plugins ./test_suite.sh
~~~

## running single tests manually

single tests in the [test suite](../tests) may also be run manually

~~~bash
# run single test manually, i.e. run test_queries function inside test_query_api.py
python3 -m pytest -v -s ./tests/query/test_query_api.py::test_queries
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

## query external tool

a similar tool is available to manually test `external queries`: [query_external.py](../test_scripts/query_external.py):

~~~bash
# example for the splunk paid plugin, --ingest also ingest data, 56590 hits
./test_scripts/query_external.py \
    --q 'sourcetype="WinEventLog:Security" Nome_applicazione="\\\\device\\\\harddiskvolume2\\\\program files\\\\intergraph smart licensing\\\\client\\\\islclient.exe"' \
    --plugin splunk --operation_id test_operation --reset --ingest \
    --plugin_params '{
        "custom_parameters":  {
            "uri": "http://localhost:8089",
            "username": "admin",
            "password": "Valerino74!",
            "index": "incidente_183651"
        },
        "override_chunk_size": 200,
        "additional_mapping_files": [[ "windows.json", "windows" ]]
    }'
~~~
