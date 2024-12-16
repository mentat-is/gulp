#!/usr/bin/env sh
SPLUNK_PASSWORD="Valerino74!"
docker run --name splunk -p 8000:8000 -e "SPLUNK_PASSWORD=${SPLUNK_PASSWORD}" -e "SPLUNK_START_ARGS=--accept-license" \
    -v /home/valerino/repos/splunk_volume/var:/opt/splunk/var -v /home/valerino/repos/splunk_volume/etc:/opt/splunk/etc \
    splunk/splunk:latest
