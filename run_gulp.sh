#!/usr/bin/env bash
# call docker-compose with optional arguments to:
# - reset elasticsearch
# - reset collab
# - set the port
# - set the interface

EXTRA_ARGS=""
PORT=8080
IFACE=0.0.0.0

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
    *)
      EXTRA_ARGS="$EXTRA_ARGS $1"
      shift
      ;;
  esac
done

# show help if --help or -h is provided
if [[ "$EXTRA_ARGS" == *"--help"* || "$EXTRA_ARGS" == *"-h"* ]]; then
  echo "Usage: $0 [--reset-collab] [--reset-elastic <index>] [--iface <iface>] [--port <port>]"
  echo "  --reset-collab: reset collab database"
  echo "  --reset-elastic: reset elasticsearch (index name must be provided as the 2nd argument.)"
  echo "  --iface: set the interface (default: 0.0.0.0)"
  echo "  --port: set the port (default: 8080)"
  exit 0
fi

# run docker-compose with the given arguments
COMPOSE_PROFILES=full EXTRA_ARGS="$EXTRA_ARGS" IFACE=$IFACE PORT=$PORT docker-compose up
