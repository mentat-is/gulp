#!/usr/bin/env bash
# win_evtx full test (tests ingestion + raw/gulp/single/sigma single/sigma group queries)
# NOTE: must be run in sequence (query needs ingest data)
python3 -m pytest ./tests/ingest.py::test_win_evtx &&
    python3 -m pytest ./tests/query.py::test_win_evtx &&

    # external plugins
    #
    # splunk full test (test raw external query without ingestion, sigma group with ingestion)
    # TODO: this actually needs paid plugin + private test data, just for the devteam reference...
    python3 -m pytest tests/query.py::test_splunk &&

    # others (will test also query_operations/query_max_min)
    python3 -m pytest tests/ingest.py::test_csv_file_mapping &&
    python3 -m pytest tests/ingest.py::test_csv_custom_mapping &&
    python3 -m pytest tests/ingest.py::test_csv_stacked &&
    python3 -m pytest tests/ingest.py::test_raw &&
    python3 -m pytest tests/ingest.py::test_zip &&

    # test collab
    python3 -m pytest tests/user.py &&
    python3 -m pytest tests/link.py &&
    python3 -m pytest tests/highlight.py &&
    python3 -m pytest tests/story.py &&
    python3 -m pytest tests/note.py &&
    python3 -m pytest tests/glyph.py &&
    python3 -m pytest tests/stored_query.py &&
    python3 -m pytest tests/user_group.py &&

    # various
    python3 -m pytest tests/utility.py &&
    python3 -m pytest tests/db.py &&

    # !!! this must be run in the end, since will delete the whole data (both on opensearch and on collab db)
    python3 -m pytest tests/operation.py
