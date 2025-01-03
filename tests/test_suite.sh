#!/usr/bin/env bash
if [ "$1" == "--help" ]; then
    echo "Usage: [PAID_PLUGINS=1 to include paid plugins] test_suite.sh"
    exit 1
fi

python3 -m pytest ingest.py::test_apache_access_clf &&
    python3 -m pytest ingest.py::test_apache_error_clf &&
    python3 -m pytest ingest.py::test_chrome_history &&
    python3 -m pytest ingest.py::test_chrome_webdata &&
    python3 -m pytest ingest.py::test_csv_standalone_and_query_operations &&
    python3 -m pytest ingest.py::test_csv_file_mapping &&
    python3 -m pytest ingest.py::test_csv_stacked &&
    python3 -m pytest ingest.py::test_eml &&
    python3 -m pytest ingest.py::test_mbox &&
    python3 -m pytest ingest.py::test_pcap &&
    python3 -m pytest ingest.py::test_pfsense &&
    python3 -m pytest ingest.py::test_raw &&
    python3 -m pytest ingest.py::test_systemd_journal &&
    python3 -m pytest ingest.py::test_teamviewer_regex_stacked &&
    # win_evtx is the most complete test, we test both ingestion and query (raw/gulp/single/sigma single/sigma group queries)
    # NOTE: this should be valid also for other ingestion types, since query code is shared
    python3 -m pytest ingest.py::test_win_evtx &&
    python3 -m pytest query.py::test_win_evtx &&
    # this is preliminary since we are just using gulp's opensearch as external source (anyway, should mostly work...)
    python3 -m pytest query.py::test_elasticsearch &&
    python3 -m pytest ingest.py::test_win_reg &&
    python3 -m pytest ingest.py::test_ingest_zip &&

    # external plugins
    if [ "$PAID_PLUGINS" = "1" ]; then
        # TODO: currently needs private test data, just for the devteam reference...
        python3 -m pytest query.py::test_paid_plugins &&
            python3 -m pytest ingest.py::test_paid_plugins
        # && python3 -m pytest extension.py::test_paid_plugins
    fi &&

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

    # this must be run in the end (will delete the whole data (both on opensearch and on collab db)
    python3 -m pytest operation.py
