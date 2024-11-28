#!/usr/bin/env bash
_TEST_INGESTION_FILTER='{}'
_TEST_PLUGIN_PARAMS='{}'
_script_dir=$(dirname $0)
source $_script_dir/cecho.sh

function usage {
  echo 'ingest SINGLE file, zipfile or directory in GULP.'
  echo
  echo 'usage:' "$0" '-p <path/to/file/zipfile/directory> [-l to test raw ingestion, -p is ignored]'
  echo ' [-q to use the given req_id] [-o <offset> to start file transfer at offset, ignored for raw]'
  echo ' [-f to filter during ingestion (TEST_INGESTION_FILTER json env variable is used if set)]'
  echo ' [-i to use ingest_zip_simple instead of ingest_zip for zip file (hardcoded win_evtx plugin, *.evtx mask)]'
  echo ' [-x to reset collaboration/elasticsearch first]'
  echo
  echo 'host,operation,context,client_id,plugin,index,user,password are set respectively to "localhost:8080", 1 (test operation), "testcontext", 1 (test client), "win_evtx", "testidx", "ingest", "ingest".'
  echo
  echo 'most of the parameters can be changed via environment variables:'
  echo ' TEST_HOST, TEST_OPERATION, TEST_CONTEXT, TEST_CLIENT, TEST_PLUGIN, TEST_INDEX, TEST_USER, TEST_PASSWORD,'
  echo ' TEST_PLUGIN_PARAMS, TEST_MASK, TEST_WS_ID.'
  echo
  echo 'a token can also be provided to avoid login via environment variable TEST_TOKEN.'
}

_ZIP=0
_ZIPSIMPLE=0
_FILTER=0
_PATH=""
_REQ_ID=
_RESET=0
_OFFSET=
_RAW=0
_DIRECTORY=0
_TEST_WS_ID=""

# default values or from env
if [ ! -z "$TEST_INGESTION_FILTER" ]; then
  _TEST_INGESTION_FILTER="$TEST_INGESTION_FILTER"
fi
if [ ! -z "$TEST_PLUGIN_PARAMS" ]; then
  _TEST_PLUGIN_PARAMS="$TEST_PLUGIN_PARAMS"
fi
if [ ! -z "$TEST_WS_ID" ]; then
  _TEST_WS_ID="$TEST_WS_ID"
fi

_MASK="${TEST_MASK:-*.evtx}"
_CONTEXT="${TEST_CONTEXT:-testcontext}"
_PLUGIN="${TEST_PLUGIN:-win_evtx}"
_INDEX="${TEST_INDEX:-testidx}"
_OPERATION="${TEST_OPERATION:-1}"
_CLIENT="${TEST_CLIENT:-1}"
_USER="${TEST_USER:-ingest}"
_PASSWORD="${TEST_PASSWORD:-ingest}"

_TOKEN="${TEST_TOKEN:-}"
_HOST="${TEST_HOST:-http://localhost:8080}"
while getopts "xslfip:q:o:" arg; do
  case $arg in
  x)
    _RESET=1
    ;;
  f)
    _FILTER=1
    ;;
  i)
    _ZIPSIMPLE=1
    ;;
  l)
    _RAW=1
    ;;
  p)
    _PATH=$(realpath "${OPTARG}")
    ;;
  q)
    _REQ_ID="${OPTARG}"
    ;;
  o)
    _OFFSET="${OPTARG}"
    ;;
  *)
    usage "$0"
    exit 1
    ;;
  esac
done

if [ -z "$_PATH" ] && [ "$_RAW" -eq 0 ]; then
  usage "$0"
  exit 1
fi

if [ "$_RAW" -ne 1 ]; then
  # no raw, check path
  if [ -d "$_PATH" ]; then
    _DIRECTORY=1
  fi

  if [[ $_PATH = *.zip ]]; then
    _ZIP=1
  fi
fi

if [ "$_FILTER" -eq 0 ]; then
  # reset filter
  _TEST_INGESTION_FILTER='{}'
fi

_INGESTION_PAYLOAD="{
  \"flt\": $_TEST_INGESTION_FILTER,
  \"plugin_params\": $_TEST_PLUGIN_PARAMS
}"

information "_DIRECTORY=$_DIRECTORY"
information "_RESET=$_RESET"
information "_QUERY=$_QUERY"
information "_OFFSET=$_OFFSET"
information "_REQ_ID=$_REQ_ID"
information "_FILTER=$_FILTER"
information "_TEST_WS_ID=$_TEST_WS_ID"
information "_HOST=$_HOST"
information "_INGESTION_PAYLOAD=$_INGESTION_PAYLOAD"
information "_MASK=$_MASK"
information "path=$_PATH"
information "user=$_USER"
information "password=$_PASSWORD"
information "operation=$_OPERATION"
information "context=$_CONTEXT"
information "client=$_CLIENT"
information "plugin=$_PLUGIN"
information "index=$_INDEX"
echo

if [ -z "$_REQ_ID" ]; then
  # generate reqid
  _REQ_ID=$(uuidgen)
fi

if [ "$_RESET" -eq 1 ]; then
  # reset first
  warning "[.] resetting redis/elastic ..."
  "$_script_dir/reinit.sh" -h $_HOST -i $_INDEX
  echo
  echo
fi

if [ ! -z "$_TOKEN" ]; then
  warning "[.] token provided=$_TOKEN (will not login)"
else
  # login
  warning "[.] logging in with $_USER, pwd=$_PASSWORD"
  _TOKEN=$($_script_dir/login.sh "$_USER" "$_PASSWORD")
  if [ "$?" -ne "0" ]; then
    exit 1
  fi
  warning "[.] got token=$_TOKEN"
fi
echo
echo

_CURL="curl"
set -- "$_CURL"
set -- "$@" -v -X PUT
if [ ! -z "$_OFFSET" ] && [ -z "$_RAW" ]; then
  warning "[.] transfer starts at offset=$_OFFSET"
  set -- "$@" --continue-at $_OFFSET
fi

if [ "$_ZIP" -eq 1 ]; then
  warning "[.] ingesting zip: $_PATH"
  _fname=$(basename $_PATH)
  if [ "$(uname)" = "Darwin" ]; then
    _fsize=$(stat -f%z "$_PATH")
  else
    _fsize=$(du -b "$_PATH" | cut -f1)
  fi

  if [ "$_ZIPSIMPLE" -eq 1 ]; then
    warning "[.] using ingest_zip_simple"
    set -- "$@" "$_HOST/ingest_zip_simple?index=$_INDEX&client_id=$_CLIENT&operation_id=$_OPERATION&context=$_CONTEXT&req_id=$_REQ_ID&plugin=$_PLUGIN&mask=$_MASK&ws_id=$_TEST_WS_ID" \
      -k \
      -H "token: $_TOKEN" \
      -H "size: $_fsize" \
      -H "continue_offset: $_OFFSET" \
      -F payload="$_INGESTION_PAYLOAD; type=application/json" \
      -F f="@$_PATH; type=application/octet-stream"
  else
    # slurp zip with metadata.json
    set -- "$@" "$_HOST/ingest_zip?index=$_INDEX&client_id=$_CLIENT&operation_id=$_OPERATION&context=$_CONTEXT&req_id=$_REQ_ID&ws_id=$_TEST_WS_ID" \
      -k \
      -H "token: $_TOKEN" \
      -H "size: $_fsize" \
      -H "continue_offset: $_OFFSET" \
      -F payload="$_INGESTION_PAYLOAD; type=application/json" \
      -F f="@$_PATH; type=application/octet-stream"
  fi

elif [ "$_DIRECTORY" -eq 1 ]; then
  warning "[.] ingesting LOCAL directory (dev only): $_PATH"
  set -- "$@" "$_HOST/ingest_directory?index=$_INDEX&directory=$_PATH&plugin=$_PLUGIN&client_id=$_CLIENT&operation_id=$_OPERATION&context=$_CONTEXT&ws_id=$_TEST_WS_ID" \
    -k \
    -H "token: $_TOKEN" \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d "$_INGESTION_PAYLOAD"

elif [ "$_RAW" -eq 1 ]; then
  # TODO: rework
  warning "[.] ingesting raw!"
  set -- "$@" "$_HOST/ingest_live?index=$_INDEX&client_id=$_CLIENT&operation_id=$_OPERATION&context=$_CONTEXT&req_id=$_REQ_ID&ws_id=$_TEST_WS_ID" \
    -k \
    -H "token: $_TOKEN" \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "events": [
        {
          "_id": "live-machine_name-6c4da9be73614368ec5b55bf3c255c47b805d7ffe37a73366d210633322f54b2-0",
          "source.domain": "machine_name",
          "@timestamp": 1467213874346,
          "event.id": "319457771",
          "log.level": 5,
          "log.file.path": "Security_short_selected.evtx",
          "event.category": "System",
          "event.code": "5152",
          "message": null,
          "event.ev_hash": "4c4da9be73614368ec5b55bf3c255c47b805d7ffe37a73366d210633322f54b2",
          "event.original": "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Event xmlns=\"http://schemas.microsoft.com/win/2004/08/events/event\">\n  <System>\n    <Provider Name=\"Microsoft-Windows-Security-Auditing\" Guid=\"54849625-5478-4994-A5BA-3E3B0328C30D\">\n    </Provider>\n    <EventID>5152</EventID>\n    <Version>0</Version>\n    <Level>0</Level>\n    <Task>12809</Task>\n    <Opcode>0</Opcode>\n    <Keywords>0x8010000000000000</Keywords>\n    <TimeCreated SystemTime=\"2016-06-29T15:24:34.346000Z\">\n    </TimeCreated>\n    <EventRecordID>319457771</EventRecordID>\n    <Correlation>\n    </Correlation>\n    <Execution ProcessID=\"4\" ThreadID=\"80\">\n    </Execution>\n    <Channel>Security</Channel>\n    <Computer>temporal</Computer>\n    <Security>\n    </Security>\n  </System>\n  <EventData>\n    <Data Name=\"ProcessId\">0</Data>\n    <Data Name=\"Application\">-</Data>\n    <Data Name=\"Direction\">%%14592</Data>\n    <Data Name=\"SourceAddress\">23.94.153.202</Data>\n    <Data Name=\"SourcePort\">59639</Data>\n    <Data Name=\"DestAddress\">169.46.6.101</Data>\n    <Data Name=\"DestPort\">3389</Data>\n    <Data Name=\"Protocol\">6</Data>\n    <Data Name=\"FilterRTID\">67607</Data>\n    <Data Name=\"LayerName\">%%14597</Data>\n    <Data Name=\"LayerRTID\">13</Data>\n  </EventData>\n</Event>"
        },
        {
          "_id": "live-machine_name-aa87a8b2552a8f02f707c40e412cb3a2de8ff29f122a6b5c73cc8380f5a9fbca-1",
          "source.domain": "machine_name",
          "@timestamp": 1467213876686,
          "event.id": "319457831",
          "log.level": 5,
          "log.file.path": "Security_short_selected.evtx",
          "event.category": "System",
          "event.code": "4776",
          "message": null,
          "event.ev_hash": "8a87a8b2552a8f02f707c40e412cb3a2de8ff29f122a6b5c73cc8380f5a9fbca",
          "event.original": "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Event xmlns=\"http://schemas.microsoft.com/win/2004/08/events/event\">\n  <System>\n    <Provider Name=\"Microsoft-Windows-Security-Auditing\" Guid=\"54849625-5478-4994-A5BA-3E3B0328C30D\">\n    </Provider>\n    <EventID>4776</EventID>\n    <Version>0</Version>\n    <Level>0</Level>\n    <Task>14336</Task>\n    <Opcode>0</Opcode>\n    <Keywords>0x8010000000000000</Keywords>\n    <TimeCreated SystemTime=\"2016-06-29T15:24:36.686000Z\">\n    </TimeCreated>\n    <EventRecordID>319457831</EventRecordID>\n    <Correlation>\n    </Correlation>\n    <Execution ProcessID=\"768\" ThreadID=\"2764\">\n    </Execution>\n    <Channel>Security</Channel>\n    <Computer>temporal</Computer>\n    <Security>\n    </Security>\n  </System>\n  <EventData>\n    <Data Name=\"PackageName\">MICROSOFT_AUTHENTICATION_PACKAGE_V1_0</Data>\n    <Data Name=\"TargetUserName\">Administrator</Data>\n    <Data Name=\"Workstation\">TEMPORAL</Data>\n    <Data Name=\"Status\">0xc000006a</Data>\n  </EventData>\n</Event>"
        },
        {
          "_id": "live-machine_name-g47c838332b0557d39fe9b2e00a6fde3721dfae452f20d7f54fccc2762a5483a-2",
          "source.domain": "machine_name",
          "@timestamp": 1668001571759,
          "event.id": "3",
          "log.level": 5,
          "log.file.path": "security.evtx",
          "event.category": "System",
          "event.code": "4902",
          "message": null,
          "event.ev_hash": "d47c838332b0557d39fe9b2e00a6fde3721dfae452f20d7f54fccc2762a5483a",
          "event.original": "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Event xmlns=\"http://schemas.microsoft.com/win/2004/08/events/event\">\n  <System>\n    <Provider Name=\"Microsoft-Windows-Security-Auditing\" Guid=\"54849625-5478-4994-A5BA-3E3B0328C30D\">\n    </Provider>\n    <EventID>4902</EventID>\n    <Version>0</Version>\n    <Level>0</Level>\n    <Task>13568</Task>\n    <Opcode>0</Opcode>\n    <Keywords>0x8020000000000000</Keywords>\n    <TimeCreated SystemTime=\"2016-07-08T18:12:51.759640Z\">\n    </TimeCreated>\n    <EventRecordID>3</EventRecordID>\n    <Correlation>\n    </Correlation>\n    <Execution ProcessID=\"456\" ThreadID=\"536\">\n    </Execution>\n    <Channel>Security</Channel>\n    <Computer>37L4247F27-25</Computer>\n    <Security>\n    </Security>\n  </System>\n  <EventData>\n    <Data Name=\"PuaCount\">0</Data>\n    <Data Name=\"PuaPolicyId\">0x35cb0</Data>\n  </EventData>\n</Event>"
        },
        {
          "_id": "live-testcontext-e47c838332b0557d39fe9b2e00a6fde3721dfae452f20d7f54fccc2762a5483a-2",
          "source.domain": "testcontext",
          "@timestamp": 1568001571759,
          "event.id": "3",
          "log.level": 5,
          "log.file.path": "test.evtx",
          "event.category": "System",
          "event.code": "4732",
          "message": null,
          "event.ev_hash": "d47c838332b0557d39fe9b2e00a6fde3721dfae452f20d7f54fccc2762a5483a",
          "event.original": "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Event xmlns=\"http://schemas.microsoft.com/win/2004/08/events/event\">\n  <System>\n    <Provider Name=\"Microsoft-Windows-Security-Auditing\" Guid=\"54849625-5478-4994-A5BA-3E3B0328C30D\">\n    </Provider>\n    <EventID>4902</EventID>\n    <Version>0</Version>\n    <Level>0</Level>\n    <Task>13568</Task>\n    <Opcode>0</Opcode>\n    <Keywords>0x8020000000000000</Keywords>\n    <TimeCreated SystemTime=\"2016-07-08T18:12:51.759640Z\">\n    </TimeCreated>\n    <EventRecordID>3</EventRecordID>\n    <Correlation>\n    </Correlation>\n    <Execution ProcessID=\"456\" ThreadID=\"536\">\n    </Execution>\n    <Channel>Security</Channel>\n    <Computer>37L4247F27-25</Computer>\n    <Security>\n    </Security>\n  </System>\n  <EventData>\n    <Data Name=\"PuaCount\">0</Data>\n    <Data Name=\"PuaPolicyId\">0x35cb0</Data>\n  </EventData>\n</Event>"
        }

    ]
  }'
else
  warning "[.] ingesting file: $_PATH"
  _fname=$(basename $_PATH)
  if [ "$(uname)" = "Darwin" ]; then
    _fsize=$(stat -f%z "$_PATH")
  else
    _fsize=$(du -b "$_PATH" | cut -f1)
  fi

  set -- "$@" "$_HOST/ingest_file?index=$_INDEX&plugin=$_PLUGIN&client_id=$_CLIENT&operation_id=$_OPERATION&context=$_CONTEXT&req_id=$_REQ_ID&ws_id=$_TEST_WS_ID" \
    -k \
    -H "token: $_TOKEN" \
    -H "size: $_fsize" \
    -H "continue_offset: $_OFFSET" \
    -F payload="$_INGESTION_PAYLOAD; type=application/json" \
    -F f="@$_PATH; type=application/octet-stream"
fi

echo "running: $@"
echo
"$@"
