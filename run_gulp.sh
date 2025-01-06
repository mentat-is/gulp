#!/usr/bin/env bash
# managess the docker-compose environment for GULP

EXTRA_ARGS=""
PORT=8080
IFACE=0.0.0.0
BACKGROUND=""

while [[ $# -gt 0 ]]; do
  case $1 in
  --reset-collab)
    EXTRA_ARGS="$EXTRA_ARGS --reset-collab"
    shift
    ;;
  --reset-index)
    EXTRA_ARGS="$EXTRA_ARGS --reset-index $2"
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
  --stop)
    docker compose --profile full down
    docker compose down
    shift
    exit 0
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
if [[ "$EXTRA_ARGS" == *"--help"* || "$EXTRA_ARGS" == *"-h"* || -z "$GULP_CONFIG_PATH" ]]; then
  echo "Usage: GULP_CONFIG_PATH=/path/to/gulp_cfg.json $0 [--iface iface] [--port port] [--reset-collab] [--reset-index index] [-d] | --stop"
  echo "  --help, -h: show this help"
  echo "  --reset-collab: reset collab database"
  echo "  --iface: specify the interface to bind to (default=0.0.0.0)"
  echo "  --port: specify the port to bind to (default=8080)"
  echo "  --reset-index: reset opensearch index (index name must be provided as the 2nd argument.)"
  echo "  --stop: stop the running containers (everything else is ignored)"
  echo "  -d: run in background"
  exit 0
fi

# run docker-compose with the given arguments
EXTRA_ARGS="$EXTRA_ARGS" BIND_TO_ADDR=$IFACE BIND_TO_PORT=$PORT docker compose --profile full up $BACKGROUND
