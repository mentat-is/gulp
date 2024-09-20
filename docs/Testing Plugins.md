[TOC]

# Intro
Testing plugins can be achieved by 2 methods, the simplest is to just use the plugin via the GUI and check data is being ingested and treated correcly,
the second way is to use the [test_ingest.sh](https://github.com/mentat-is/gulp/test_scripts/test_ingest.sh), a simple bash script which user `curl` to perform ingestion onto the host.

## Usage
Here's the `-h` output of the `test_ingest.sh` script:
```
ingest SINGLE file, zipfile or directory in GULP.

usage: ./test_scripts/test_ingest.sh -p <path/to/file/zipfile/directory> [-l to test raw ingestion, -p is ignored]
 [-q to use the given req_id] [-o <offset> to start file transfer at offset, ignored for raw]
 [-f to filter during ingestion (TEST_INGESTION_FILTER json env variable is used if set)]
 [-i to use ingest_zip_simple instead of ingest_zip for zip file (hardcoded win_evtx plugin, *.evtx mask)]
 [-x to reset collaboration/elasticsearch first]

host,operation,context,client_id,plugin,index,user,password are set respectively to "localhost:8080", 1 (test operation), "testcontext", 1 (test client), "gi_win_evtx", "testidx", "ingest", "ingest".

most of the parameters can be changed via environment variables:
 TEST_HOST, TEST_OPERATION, TEST_CONTEXT, TEST_CLIENT, TEST_PLUGIN, TEST_INDEX, TEST_USER, TEST_PASSWORD,
 TEST_PLUGIN_PARAMS, TEST_MASK, TEST_WS_ID.

a token can also be provided to avoid login via environment variable TEST_TOKEN.
```

# Examples

Here are a few examples on how to use the [test_ingest.sh](https://github.com/mentat-is/gulp/test_scripts/test_ingest.sh) script.

## win_evtx plugin

Source: [gi_win_evtx.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_win_evtx.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_win_evtx ./test_scripts/test_ingest.sh -p samples/gi_win_evtx
```

## win_reg plugin

Source: [gi_win_reg.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_win_reg.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_win_reg ./test_scripts/test_ingest.sh -p samples/win_reg
```

## apache_access_clf plugin

Source: [gi_apache_access_clf.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_apache_access_clf.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_apache_access_clf ./test_scripts/test_ingest.sh -p samples/apache_access_clf/access.log.sample
```

## apache_error_clf plugin

Source: [gi_apache_error_clf.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_apache_error_clf.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_apache_error_clf ./test_scripts/test_ingest.sh -p samples/apache_error_clf/error.log
```

## chrome_history_sqlite_stacked plugin

Source: [gi_chrome_history_sqlite_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_chrome_history_sqlite_stacked.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_chrome_history_sqlite_stacked ./test_scripts/test_ingest.sh -p $HOME/.config/chromium/Profile\ 1/History
```

## chrome_webdata_sqlite_stacked plugin

Source: [gi_chrome_webdata_sqlite_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_chrome_webdata_sqlite_stacked.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_chrome_webdata_sqlite_stacked ./test_scripts/test_ingest.sh -p $HOME/.config/chromium/Profile\ 1/Web\ Data
```

## eml plugin

Source: [gi_eml.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_eml.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_eml ./test_scripts/test_ingest.sh -p samples/eml/
```

This plugin supports the following options:
- `decode`: if set to `True` it automatically decodes multipart eml messages (e.g. base64, etc)

## mbox plugin

Source: [gi_mbox.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_mbox.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_mbox ./test_scripts/test_ingest.sh -p samples/mbox/
```

This plugin supports the same options as the `eml` plugin.

## teamviewer_regex_stacked plugin

Source: [gi_teamviewer_regex_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_teamviewer_regex_stacked.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=teamviewer_regex_stacked ./test_scripts/test_ingest.sh -p samples/teamviwer/connections_incoming.txt
```

## regex plugin

Source: [gi_regex.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_regex.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN_PARAMS='{"extra":{"regex":"(?P<test>.*)-(?P<timestamp>.*)"}}' TEST_PLUGIN=gi_regex ./test_scripts/test_ingest.sh -x -p samples/regex/test.sample
```

This plugin requires the following options to be set in order to work:
- `regex`: the regex to match
- `flags`: regex flags to apply (refer to [flags](https://docs.python.org/3/library/re.html#flags) for the values)

The `regex` parameter must satisfy the following requirements:
- must contain **named groups**
- must contain one named group named **timestamp** (case-sensitive)

## systemd_journal plugin

Source: [gi_systemd_journal.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_systemd_journal.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_systemd_journal ./test_scripts/test_ingest.sh -p ./samples/systemd_journal/system.journal
```

## csv plugin

Source: [gi_csv.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_csv.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_csv TEST_PLUGIN_PARAMS='{"mapping_file": "mftecmd_csv.json", "mapping_id": "record"}' ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_record.csv
```

## sqlite plugin

Source: [gi_sqlite.py](https://github.com/mentat-is/gulp/src/gulp/plugins/gi_sqlite.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=gi_sqlite TEST_PLUGIN_PARAMS='{"mapping_file": "chrome_webdata.json"}' ./test_scripts/test_ingest.sh -p ./samples/chrome-webdata/webdata.sqlite
```
