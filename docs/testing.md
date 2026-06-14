- [testing gulp](#testing-gulp)
  - [prerequisites](#prerequisites)
  - [running tests](#running-tests)
  - [ingestion tool](#ingestion-tool)
  - [query external tool](#query-external-tool)
  - [observability](#observability)

# testing gulp

## prerequisites

ensure [gulp api client sdk](https://github.com/mentat-is/gulp-sdk) is installed

~~~bash
pip3 install -e ./gulp-sdk
~~~

start gulp first

~~~bash
gulp --reset-collab --create test_operation
~~~

## running tests

tests are located in the [tests](../tests) folder and can be run independently, i.e.

> [stress_test](../tests/integration/test_stress.py) is a test that runs a subset of critical tests in sequence: it is useful to verify if the main functionalities are working as expected.

~~~bash
# run all
python3 -m pytest -v -s -x ./tests/

# run all with coverage
pytest -v -s -x --cov=gulp_sdk tests/

# run integration tests only
python3 -m pytest -v -s -x ./tests/integration

# run stress tests only (NOTE: the sigma-zip part needs the non-free query_sigma_zip plugin)
python3 -m pytest -v -s -x ./tests/integration/test_stress.py

# when Prometheus is enabled, stress tests also assert critical /metrics families
# are exported after load; tune the scrape wait if collection is slower
GULP_STRESS_METRICS_TIMEOUT=60 python3 -m pytest -v -s -x ./tests/integration/test_stress.py::test_concurrent_ingest_and_query

# run two-instance websocket/pub-sub routing checks; the script starts and stops
# backends on :8080 and :8100 and drives both instances concurrently
./tests/integration/run_multi_instance_tests.sh

# run the same two-instance checks as a visible large-payload fanout soak
# (defaults to 20 pointer iterations, 500 raw docs per instance, and one large-note collab iteration)
GULP_MULTI_INSTANCE_SOAK=1 ./tests/integration/run_multi_instance_tests.sh

# suppress live backend logs while keeping log files in the final summary
QUIET=1 GULP_MULTI_INSTANCE_SOAK=1 ./tests/integration/run_multi_instance_tests.sh

# tune the large-payload fanout manually for heavier pre-release soak runs
GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT=100 \
GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS=600 \
GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT=1 \
GULP_MULTI_INSTANCE_RAW_INGEST_DOCS=1000 \
./tests/integration/run_multi_instance_tests.sh

# run a specific test, use full windows sigma rules set
BIG_SIGMAS=1 python3 -m pytest -v -s -x ./tests/integration/test_stress.py::test_concurrent_ingest_and_query_same_operation
~~~

or use the provided [run_tests.sh](../test_scripts/run_tests.sh) script to run all tests automatically (or a subset of them)

~~~bash
# run all tests
./test_scripts/run_tests.sh ./tests
# run only specific tests
./test_scripts/run_tests.sh ./tests/query/test_query_api.py ./tests/ingest/test_ingest.py::test_win_evtx"
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
            "is_timestamp": "chrome"
          }
        }
      }
    }
  }'
~~~

## query external tool

a similar tool is available to manually test `external queries`: [query_external.py](../test_scripts/query_external.py):

~~~bash
# example for the splunk paid plugin, 56590 hits
./test_scripts/query_external.py \
    --q 'sourcetype="WinEventLog:Security" Nome_applicazione="\\\\device\\\\harddiskvolume2\\\\program files\\\\intergraph smart licensing\\\\client\\\\islclient.exe"' \
    --plugin splunk --operation_id test_operation --reset \
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

## observability

See [docs/observability.md](./observability.md) for details on enabling Prometheus metrics and connecting Grafana for dashboards.
