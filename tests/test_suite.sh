#!/usr/bin/env bash
if [ "$1" == "--help" ]; then
    echo "Usage: [PAID_PLUGINS=1 to include paid plugins] test_suite.sh"
    exit 1
fi

# win_evtx full test (tests ingestion + raw/gulp/single/sigma single/sigma group queries)
# NOTE: must be run in sequence (query needs ingest data)
python3 -m pytest ingest.py::test_win_evtx &&
    python3 -m pytest query.py::test_win_evtx &&

    # external plugins
    if [ "$PAID_PLUGINS" = "1" ]; then
        # splunk full test (test raw external query without ingestion, sigma group with ingestion)
        # TODO: currently needs private test data, just for the devteam reference...
        python3 -m pytest /query.py::test_splunk
    fi &&

    # others (will test also query_operations/query_max_min)
    python3 -m pytest ingest.py::test_csv_file_mapping &&
    python3 -m pytest ingest.py::test_csv_custom_mapping &&
    python3 -m pytest ingest.py::test_csv_stacked &&
    python3 -m pytest ingest.py::test_raw &&
    python3 -m pytest ingest.py::test_zip &&

    # test collab
    python3 -m pytest user.py &&
    python3 -m pytest link.py &&
    python3 -m pytest highlight.py &&
    python3 -m pytest story.py &&
    python3 -m pytest note.py &&
    python3 -m pytest glyph.py &&
    python3 -m pytest stored_query.py &&
    python3 -m pytest user_group.py &&

    # various
    python3 -m pytest utility.py &&
    python3 -m pytest db.py &&

    # !!! this must be run in the end, since will delete the whole data (both on opensearch and on collab db)
    python3 -m pytest operation.py
