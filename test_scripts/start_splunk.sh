#!/usr/bin/env bash
# start splunk container mounting our own volume
#
reset_perm() {
    echo "resetting permission on $SPLUNK_BASE, user=$USER ..."
    sudo chown -R $USER:$USER $SPLUNK_BASE
    sudo chmod 777 -R $SPLUNK_BASE
}

if [ -z "$SPLUNK_PASSWORD" ]; then
    SPLUNK_PASSWORD="Valerino74!"
fi
if [ -z "$SPLUNK_BASE" ]; then
    SPLUNK_BASE="/home/valerino/repos/splunk_volume"
fi
if [ ! -e "$SPLUNK_BASE" ]; then
    echo "ERROR: $SPLUNK_BASE do not exist!"
    exit 1
fi
echo "$SPLUNK_BASE exists..."

if [ "$1" == "--help" ]; then
    echo "usage: $0 [--reset-permission to just reset permission on SPLUNK_BASE directory."
    echo "  env:"
    echo "  SPLUNK_BASE: base splunk directory with /var to be mounted"
    echo "  SPLUNK_PASSWORD: splunk admin password"
    exit 0
fi

if [ "$1" == "--reset-permission" ]; then
    reset_perm
    exit 0
fi

reset_perm
echo "running splunk ..."
# ensure it is stopped first
docker stop splunk
docker run -d --rm --name splunk -p 8000:8000 -p 8065:8065 -p 8089:8089 -e "SPLUNK_PASSWORD=${SPLUNK_PASSWORD}" -e "SPLUNK_START_ARGS=--accept-license" \
    -v $SPLUNK_BASE/var:/opt/splunk/var -v $SPLUNK_BASE/etc:/opt/splunk/etc \
    splunk/splunk:latest
