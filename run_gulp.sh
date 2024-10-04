#!/usr/bin/env bash
# call docker-compose with optional arguments to:
# - reset elasticsearch
# - reset collab
# - set the port
# - set the interface
# - set the path to store opensearch data
# - set the path to store postgres data

EXTRA_ARGS=""
PORT=8080
IFACE=0.0.0.0
OPENSEARCH_DATA_PATH=./opensearch_data
POSTGRES_DATA_PATH=./postgres_data
BACKGROUND=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --reset-collab)
      EXTRA_ARGS="$EXTRA_ARGS --reset-collab"
      shift
      ;;
    --reset-elastic)
      EXTRA_ARGS="$EXTRA_ARGS --reset-elastic $2"
      shift 2
      ;;
    --port)
      PORT=$2
      shift 2
      ;;
    --iface)
      IFACE=$2
      shift 2
      ;;
    --opensearch-data-path)
      OPENSEARCH_DATA_PATH=$2
      shift 2
      ;;
    --postgres-data-path)
      POSTGRES_DATA_PATH=$2
      shift 2
      ;;
    -d)
      BACKGROUND="-d"
      shift
      ;;
    *)
      EXTRA_ARGS="$EXTRA_ARGS $1"
      shift
      ;;
  esac
done

# show help if --help or -h is provided
if [[ "$EXTRA_ARGS" == *"--help"* || "$EXTRA_ARGS" == *"-h"*  || -z "$GULP_CONFIG_PATH" ]]; then
  echo "Usage: GULP_CONFIG_PATH=/path/to/gulp_cfg.json $0 [--reset-collab] [--reset-elastic <index>] [--iface <iface>] [--port <port>] [--opensearch-data-path <path>] [--postgres-data-path <path>] [-d]"
  echo "  --help, -h: show this help"
  echo "  --reset-collab: reset collab database"
  echo "  --reset-elastic: reset elasticsearch (index name must be provided as the 2nd argument.)"
  echo "  --iface: set the interface (default: 0.0.0.0)"
  echo "  --port: set the port (default: 8080)"
  echo "  --opensearch-data-path: set the path to store opensearch data (default: ./opensearch_data1)"
  echo "  --postgres-data-path: set the path to store postgres data (default: ./postgres_data)"
  echo "  -d: run in background"
  exit 0
fi

# ensure directories exists
mkdir -p $OPENSEARCH_DATA_PATH
mkdir -p $POSTGRES_DATA_PATH
mkdir ./gulpconfig
export ELASTIC_DATA=$OPENSEARCH_DATA_PATH
export POSTGRES_DATA=$POSTGRES_DATA_PATH

# run docker-compose with the given arguments
COMPOSE_PROFILES=full EXTRA_ARGS="$EXTRA_ARGS" IFACE=$IFACE PORT=$PORT docker-compose up $BACKGROUND
