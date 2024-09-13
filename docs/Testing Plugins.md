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

host,operation,context,client_id,plugin,index,user,password are set respectively to "localhost:8080", 1 (test operation), "testcontext", 1 (test client), "win_evtx", "testidx", "ingest", "ingest".

most of the parameters can be changed via environment variables:
 TEST_HOST, TEST_OPERATION, TEST_CONTEXT, TEST_CLIENT, TEST_PLUGIN, TEST_INDEX, TEST_USER, TEST_PASSWORD,
 TEST_PLUGIN_PARAMS, TEST_MASK, TEST_WS_ID.

a token can also be provided to avoid login via environment variable TEST_TOKEN.
```

# Examples
Here are a few examples on how to use the [test_ingest.sh](https://github.com/mentat-is/gulp/test_scripts/test_ingest.sh) script.

## win_evtx plugin
Source: [win_evtx.py](https://github.com/mentat-is/gulp/src/gulp/plugins/win_evtx.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=win_evtx ./test_scripts/test_ingest.sh -p samples/win_evtx
```

## win_reg plugin
Source: [win_reg.py](https://github.com/mentat-is/gulp/src/gulp/plugins/win_reg.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=win_reg ./test_scripts/test_ingest.sh -p samples/win_reg
```

## apache_access_clf plugin
Source: [apache_access_clf.py](https://github.com/mentat-is/gulp/src/gulp/plugins/apache_access_clf.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=apache_access_clf ./test_scripts/test_ingest.sh -p samples/apache_access_clf/access.log.sample
```

## apache_error_clf plugin
Source: [apache_error_clf.py](https://github.com/mentat-is/gulp/src/gulp/plugins/apache_error_clf.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=apache_error_clf ./test_scripts/test_ingest.sh -p samples/apache_error_clf/error.log
```

## chrome_history_sqlite_stacked plugin
Source: [chrome_history_sqlite_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/chrome_history_sqlite_stacked.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=chrome_history_sqlite_stacked ./test_scripts/test_ingest.sh -p $HOME/.config/chromium/Profile\ 1/History
```

## chrome_webdata_sqlite_stacked plugin
Source: [chrome_webdata_sqlite_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/chrome_webdata_sqlite_stacked.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=chrome_webdata_sqlite_stacked ./test_scripts/test_ingest.sh -p $HOME/.config/chromium/Profile\ 1/Web\ Data
```

## eml plugin
Source: [eml.py](https://github.com/mentat-is/gulp/src/gulp/plugins/eml.py)

```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=eml ./test_scripts/test_ingest.sh -p samples/eml/
```

This plugin supports the following options:
- `decode`: if set to `True` it automatically decodes multipart eml messages (e.g. base64, etc)

## mbox plugin
Source: [mbox.py](https://github.com/mentat-is/gulp/src/gulp/plugins/mbox.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=mbox ./test_scripts/test_ingest.sh -p samples/mbox/
```

This plugin supports the same options as the `eml` plugin.

## teamviewer_regex_stacked plugin
Source: [teamviewer_regex_stacked.py](https://github.com/mentat-is/gulp/src/gulp/plugins/teamviewer_regex_stacked.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN=teamviewer_regex_stacked ./test_scripts/test_ingest.sh -p samples/teamviwer/connections_incoming.txt
```

## regex plugin
Source: [regex.py](https://github.com/mentat-is/gulp/src/gulp/plugins/regex.py)
```sh
TEST_WS_ID="websocket_id" TEST_PLUGIN_PARAMS='{"extra":{"regex":"(?P<test>.*)-(?P<timestamp>.*)"}}' TEST_PLUGIN=regex ./test_scripts/test_ingest.sh -x -p samples/regex/test.sample
```

This plugin requires the following options to be set in order to work:
- `regex`: the regex to match
- `flags`: regex flags to apply (refer to [flags](https://docs.python.org/3/library/re.html#flags) for the values)

The `regex` parameter must satisfy the following requirements:
- must contain **named groups**
- must contain one named group named **timestamp** (case-sensitive)

## systemd_journal plugin
Source: [systemd_journal.py](https://github.com/mentat-is/gulp/src/gulp/plugins/systemd_journal.py)
TODO

## csv plugin
Source: [csv.py](https://github.com/mentat-is/gulp/src/gulp/plugins/csv.py)
TODO

## sqlite plugin
Source: [sqlite.py](https://github.com/mentat-is/gulp/src/gulp/plugins/sqlite.py)
TODO
