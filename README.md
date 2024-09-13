<div align="center">

<picture>
 <source media="(prefers-color-scheme: dark)" srcset="./image.png">
 <source media="(prefers-color-scheme: light)" srcset="./image.png">
 <img alt="gULP" src="./image.png" width="30%" height="30%">
</picture>

the generic, universal Log processor for incident response!

_made with :heart: by Mentat._

</div>

[![GitHub followers](https://img.shields.io/github/followers/mentat-is.svg?style=social&label=Follow&maxAge=2592000)](https://github.com/mentat-is?tab=followers)

<div align="center">

[Description](#Description) - [Architecture](#Architecture) - [API](#API) - [Installation](#Installation) - [Run & examples](#Run) - [Troubleshoot](#Troubleshoot)

</div>

## Description

Gulp is a powerful software tool designed to streamline incident response and analysis. Its core features include:

### Current features

- **Data Ingestion Plugins**: Gulp can ingest data from a variety of sources, thanks to its versatile plugin system.
- **OpenSearch and ECS**: Gulp is built on OpenSearch and uses the *Elasticsearch Common Scheme (ECS)* as its ingestion format, ensuring compatibility and ease of use.
- **High-Speed Multiprocessing Engine**: Gulp's engine is designed for speed, offering fast ingestion and querying capabilities through multiprocessing.
- **Query using SIGMA rules**: Gulp supports querying using Sigma Rules, allowing for easy, one-click queries with thousands of rules in parallel.
- **Collaboration Platform**: Gulp includes a collaboration platform, enabling teams to work together on the same incident. Features include note-taking, highlighting, and link adding.
- **Innovative UI**: Gulp's user interface includes a zoomable timeline for visualizing events, making it easier to understand and analyze incidents.
- **Scalable**: Gulp is designed with scalability in mind. As your data and team grow, you can simply add more cores to increase parallel ingestion and query capabilities, and more OpenSearch and PostgreSQL hosts. This makes Gulp a flexible solution that can adapt to your evolving needs!
- **Python based**: Gulp is written in Python, leveraging open-source libraries whenever possible. This maximizes ease of adoption from the community, as Python is widely used and understood.

### Roadmap

the following features are planned:

#### Soon

- **Slurp**: Our proprietary modular agent to ease log collections from a variety of sources.

- **Machine Learning**: Enhances Gulp's analysis capabilities using ML plugins, providing automated detection of anomalies and patterns, automated incident reports, and more.

#### Then

- **SIEM Integration**: integrate with popular SIEMs (*SIEM bridges*).

## Architecture

[GULP architecture](./docs/Architecture.md)

## Installation

### Prerequisites

- docker
- docker-compose-v2
- github token

  - ❗❗ a [github token](https://github.com/settings/tokens) must be created and setup to access the private repositories during development:

  ```bash
  # install github-cli, then
  gh auth login
  # then choose "https login" and enter your token.
  ```

- [docker](./install_docker.md)
- [install from sources](./install_dev.md)

### Environment variables

the following environment variables may be set to override configuration options.

- `PATH_CONFIG`: if set, will be used as path for the configuration file (either, `~/.config/gulp/gulp_cfg.json` will be used)
- `PATH_PLUGINS`: if set, will be used as path for `plugins` directory (either, the default `$INSTALLDIR/plugins` will be used)
- `PATH_MAPPING_FILES`: if set, will be used as path for the mapping files to be used by plugins (either, the default is `$INSTALLDIR/mapping_files`)
- `PATH_CERTS`: if set, overrides `path_certs` in the configuration (for HTTPS).
- `ELASTIC_URL`: if set, overrides `elastic_url` in the configuration.
- `POSTGRES_URL`: if set, overrides `postgres_url` in the configuration.
- `SMTP_SERVER`: if set, overrides `smtp_server` in the configuration.
- `SMTP_USERNAME`: if set, overrides `stmp_username` in the configuration.
- `SMTP_PASSWORD`: if set, overrides `stmp_password` in the configuration.
- `SMTP_FROM`: if set, overrides `smtp_from` in the configuration.
- `GULP_INTEGRATION_TEST`: **TEST ONLY**, this must be set to 1 during integration testing (i.e. client api) to disable debug features which may interfere.

### SSL

to use HTTPS, the following certificates must be available:

> client certificates for `opensearch` and `postgresql` are used if found, `opensearch` key password is not supported.

- opensearch
  - `elastic_verify_certs: false` may be used to skip server verification
  - `$PATH_CERTS/opensearch-ca.pem`: path to the CA certificate for the Opensearch server
  - `$PATH_CERTS/opensearch.pem`: client certificate to connect to Opensearch server
  - `$PATH_CERTS/opensearch.key`: certificate key

- postgresql
  - `postgres_ssl: true` mut be set in the configuration
  - `postgres_verify_certs: false` may be used to skip server verification
  - `$PATH_CERTS/postgres-ca.pem`: path to the CA certificate for the PostgreSQL server
  - `$PATH_CERTS/postgres.pem` client certificate to connect to PostgreSQL server
  - `$PATH_CERTS/postgres.key`: certificate key

- gulp server
  - to connect gulp clients, use `https_enforce` to prevent HTTP connections, `https_enforce_client_certs` to enforce client certificates signed by `gulp-ca.pem`CA
  - `$PATH_CERTS/gulp-ca.pem`
  - `$PATH_CERTS/gulp.pem`
  - `$PATH_CERTS/gulp.key`

### Exposed services

- _http://localhost:8080_: gulp, swagger page [here](http://localhost:8080/docs)
- _localhost:5432_: postgres (**user/pwd: `postgres/Gulp1234!`**)
  - _http://localhost:8001_: adminer (**server/user/pwd: `postgres/postgres/Gulp1234!`**)
- _http://localhost:9200_: opensearch (**user/pwd: `admin/Gulp1234!`**)
  - _http://localhost:8082_: elasticvue
  - _http://localhost:5601_: opensearch dashboards (**user/pwd: `admin/Gulp1234!`**)

### Run

[with docker](./install_docker.md#run) or [with install from sources](./install_dev.md#3-run)

#### Test ingestion

```bash
# ingest single file
TEST_WS_ID="websocket_id" ./test_scripts/test_ingest.sh -p ./samples/win_evtx/Application_no_crc32.evtx

# ingest zip
TEST_WS_ID="websocket_id" ./test_scripts/test_ingest.sh -p ./test_scripts/test_upload_with_metadata_json.zip

# ingest with filter (GulpIngestFilter)
TEST_WS_ID="websocket_id" TEST_INGESTION_FILTER='{"level":[2]}' ./test_scripts/test_ingest.sh -p ./test_scripts/test_upload_with_metadata_json.zip -f

## multiple concurrent ingestion (using 2 target websockets)
TEST_WS_ID="websocket_id1" TEST_TOKEN=80d5ed1e-c30f-4926-872d-92bcc5a2235d ./test_scripts/test_ingest.sh -p ./samples/win_evtx && TEST_WS_ID="websocket_id2" TEST_TOKEN=80d5ed1e-c30f-4926-872d-92bcc5a2235d TEST_CONTEXT=context2 TEST_OPERATION=testop2 ./test_scripts/test_ingest.sh -p ./samples/win_evtx

# ingest with csv plugin without mapping file ("@timestamp" in the document is mandatory, so in one way or another we must know at least how to obtain a timestamp for ingested event)
TEST_WS_ID="websocket_id" TEST_PLUGIN_PARAMS='{"timestamp_field": "UpdateTimestamp"}' TEST_PLUGIN=csv ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv

# ingest with csv plugin with mapping file
TEST_WS_ID="websocket_id" TEST_PLUGIN_PARAMS='{"mapping_file": "mftecmd_csv.json", "mapping_id": "j"}' TEST_PLUGIN=csv ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv

# example overriding a configuration parameter (will stay overridden until gulp restarts)
TEST_WS_ID="websocket_id" TEST_PLUGIN_PARAMS='{"mapping_file": "mftecmd_csv.json", "mapping_id": "j", "config_override": { "debug_allow_any_token_as_admin": true }' TEST_PLUGIN=csv ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv

# ingest local directory (just for testing, not available in production code and not available when gulp runs in docker)
TEST_WS_ID="websocket_id" ./test_scripts/test_ingest.sh -x -p ./samples/win_evtx

# sample ingestion with filter
TEST_INGESTION_FILTER='{"start_msec":1475719436055, "end_msec": 1475719436211}' TEST_WS_ID=abc ./test_scripts/test_ingest.sh -p ./samples/win_evtx -f

# same as above, send filtered chunks on websocket but store data anyway on database (do not apply filter on-store)
TEST_INGESTION_FILTER='{"start_msec":1475719436055, "end_msec": 1475719436211, "store_all_documents": true}' TEST_WS_ID=abc ./test_scripts/test_ingest.sh -p ./samples/win_evtx -f

```

> - for testing websocket, a browser extension i.e. [chrome websockets browser extension](https://chromewebstore.google.com/detail/browser-websocket-client/mdmlhchldhfnfnkfmljgeinlffmdgkjo) may be used.
>
>   - if TEST_WS_ID is not specified, ingestion happens anyway, but results are not broadcasted.
>
> - resuming ingestion is supported **if the `req_id` parameter is the same across requests**.


## Troubleshoot

- if you want to be able to run the docker CLI command as a non-root user, add your user to the `docker` user group, re-login, and restart `docker.service` (https://wiki.archlinux.org/title/Users_and_groups#Group_management)

- environment variables are defined in [.env](./.env) which is automatically read by docker-compose when starting the containers.

- ensure data persistence directories for postgres and OpenSearch (`ELASTIC_DATA`, `POSTGRES_DATA` defined in `.env`) exists `AND are owned by the user`.

- on linux, usually `vm.max_map_count` should be increased for OpenSearch, i.e. you see something like this in the error log when OpenSearch is starting up:

  ```json
  {
    "@timestamp": "2023-10-30T13:26:11.175Z",
    "log.level": "ERROR",
    "message": "node validation exception\n[1] bootstrap checks failed. You must address the points described in the following [1] lines before starting Elasticsearch.\nbootstrap check failure [1] of [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]",
    "ecs.version": "1.2.0",
    "service.name": "ES_ECS",
    "event.dataset": "elasticsearch.server",
    "process.thread.name": "main",
    "log.logger": "org.elasticsearch.bootstrap.Elasticsearch",
    "elasticsearch.node.name": "a17e0512fed1",
    "elasticsearch.cluster.name": "docker-cluster"
  }
  ```

  to fix this, set `vm.max_map_count=262144` in `/etc/sysctl.conf (or in a file in /etc/sysctl.d)`

  or set it temporarly with `sysctl vm.max_map_count=262144`.

  ```bash
  # activate without reboot
  sudo sysctl --system
  ```

  or reboot.

- if you get errors like `failed to obtain node-lock` while booting OpenSearch, ensure that the data directory i.e. `./elastic_data` is NOT owned by root and is writable (so, chown it again to your user in case).

- error `too many connections already` from postgres usually happens when ingesting too many files at once, and should be handled by tuning the configuration parameters:
  - in gulp configuration, check `multiprocessing_batch_size`: it is advised to keep it 0 to perform operation in batches of *number of cores*, raising this value may speed up ingestion a lot but it is more prone to errors.
  - in postgres configuration, increase `max_connections`
  - **better solution is to scale up (increase cores and/or postgres cluster size)**

- error `elastic_transport.ConnectionTimeout: Connection timed out` usually means your opensearch istance is not keeping up with ingestion:
  - increase `ingestion_request_timeout` (**almost always this is the easiest solution**) OR
  - scale up OpenSearch nodes OR
  - reduce parallelism with `parallel_processes_max` OR
  - tune `ingestion_buffer_size` configuration parameter (i.e. default is 1000, try with 2000 to reduce parallel chunks)

- in case opensearch fails to successfully bootstrap because of errors such as:

```
elasticsearch  | {"@timestamp":"2024-07-22T15:53:30.889Z", "log.level": "WARN", "message":"flood stage disk watermark [95%] exceeded on [2Mf_sAxtRua8tLTU865kPA][3b369ef109f9][/usr/share/elasticsearch/data] free: 39.7gb[4.3%], all indices on this node will be marked read-only", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[3b369ef109f9][management][T#2]","log.logger":"org.elasticsearch.cluster.routing.allocation.DiskThre
```

a possible (temporary) solution is to disable disk thresholds in opensearch's configuration after the node starts:

```bash
curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'

curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
```
