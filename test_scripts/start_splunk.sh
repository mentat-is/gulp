#!/usr/bin/env sh
SPLUNK_PASSWORD="Valerino74!"
sudo chown -R valerino:valerino /home/valerino/repos/splunk_volume
sudo chmod -R 777 /home/valerino/repos/splunk_volume
docker run -d --rm --name splunk -p 8000:8000 -e "SPLUNK_PASSWORD=${SPLUNK_PASSWORD}" -e "SPLUNK_START_ARGS=--accept-license" \
    -v /home/valerino/repos/splunk_volume/var:/opt/splunk/var -v /home/valerino/repos/splunk_volume/etc:/opt/splunk/etc \
    splunk/splunk:latest
