#!/usr/bin/env bash
# if this test passes, you may be quietly sure at least the basic and advanced ingestion/query features of gulp are working fine.
# anything else is anyway trivially fixable....

# log start time
echo "Starting SMOKE TESTS at $(date)"
start_ts=$(date +%s)

finish_report() {
    # print elapsed time and finishing timestamp
    end_ts=$(date +%s)
    elapsed=$((end_ts - start_ts))
    printf "Elapsed time: %d seconds (%02d:%02d:%02d)\n" "$elapsed" "$((elapsed/3600))" "$((elapsed%3600/60))" "$((elapsed%60))"
    echo "Finished SMOKE TESTS at $(date)"
}
python3 -m pytest -x -v -s ./tests/test_operation.py::test_operation_api
if [ $? -ne 0 ]; then
    echo "test_operation_api failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/ingest/test_ingest.py::test_csv_file_mapping
if [ $? -ne 0 ]; then
    echo "test_csv_file_mapping failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/ingest/test_ingest.py::test_raw
if [ $? -ne 0 ]; then
    echo "test_raw failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/ingest/test_ingest.py::test_ws_raw
if [ $? -ne 0 ]; then
    echo "test_ws_raw failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/test_db.py::test_rebase_by_query
if [ $? -ne 0 ]; then
    echo "test_rebase_by_query failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/query/test_query_api.py::test_query_raw
if [ $? -ne 0 ]; then
    echo "test_query_raw failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/query/test_query_api.py::test_query_sigma_group
if [ $? -ne 0 ]; then
    echo "test_query_sigma_group failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/query/test_query_api.py::test_sigma_preview
if [ $? -ne 0 ]; then
    echo "test_sigma_preview failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./tests/ingest/test_ingest.py::test_ingest_preview
if [ $? -ne 0 ]; then
    echo "test_ingest_preview failed"
    goto __fail
fi

python3 -m pytest -x -v -s ./tests/enrich/test_enrich_whois.py
if [ $? -ne 0 ]; then
    echo "test_ingest_preview failed"
    goto __fail
fi

python3 -m pytest -x -v -s ./tests/test_tag_documents.py
if [ $? -ne 0 ]; then
    echo "test_ingest_preview failed"
    goto __fail
fi

BIG_SIGMAS=1 python3 -m pytest -x -v -s ./gulp-paid-plugins/tests/extension/test_query_sigma_zip.py::test_sigma_zip
if [ $? -ne 0 ]; then
    echo "test_sigma_zip failed"
    goto __fail
fi

# the following needs the splunk paid plugin with the internal test data (just remove it if you don't have it)
python3 -m pytest -x -v -s ./gulp-paid-plugins/tests/query/test_query_external_splunk.py::test_splunk &&
if [ $? -ne 0 ]; then
    echo "test_splunk failed"
    goto __fail
fi
python3 -m pytest -x -v -s ./gulp-paid-plugins/tests/query/test_query_external_splunk.py::test_splunk_preview
if [ $? -ne 0 ]; then
    echo "test_splunk_preview failed"
    goto __fail
fi

echo "SUCCESS! SMOKE TESTS"
finish_report
exit 0

__fail:
echo "FAILED! SMOKE TESTS"
finish_report
exit 1
