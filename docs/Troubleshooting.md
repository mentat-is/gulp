
- [troubleshooting](#troubleshooting)
  - [devcontainer](#devcontainer)
  - [docker](#docker)
  - [general](#general)
  - [os](#os)
  - [opensearch / elasticsearch](#opensearch--elasticsearch)
  - [postgreSQL](#postgresql)
  - [websocket](#websocket)

[TOC]

# troubleshooting

**before opening issues**, please check the following:

## devcontainer

if you see an error like the following:

~~~
Error response from daemon: Conflict. The container name "/elasticvue" is already in use by container "some_container_id". You have to remove (or rename) that container to be able to reuse that name.
~~~

remove the container with `docker container rm some_container_id` and retry.

## docker

- if you want to be able to run the docker CLI command as a non-root user, add your user to the `docker` user group, re-login, and restart `docker.service` [check here](https://wiki.archlinux.org/title/Users_and_groups#Group_management)

- if you get any issue starting container/s as [per docs](./Install%20Docker.md), try to reset gulp's docker volumes with [this](../reset_docker.sh) script.

## general

- environment variables to customize postgresql and opensearch installations are defined in [.env](./.env) which is automatically read by docker-compose when starting the containers.

## os

- on linux, usually `vm.max_map_count` should be increased for OpenSearch, i.e. you see something like this in the error log when OpenSearch is starting up:

  ~~~json
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
  ~~~

  to fix this, set `vm.max_map_count=262144` in `/etc/sysctl.conf (or in a file in /etc/sysctl.d)`

  or set it temporarly with `sysctl vm.max_map_count=262144`.

  ~~~bash
  # activate without reboot
  sudo sysctl --system
  ~~~

  or reboot.

## opensearch / elasticsearch

- couple of errors about cannot connecting to opensearch **are normal if gulp is run immediately right after `docker compose up -d`**: no worries, it just notifies it is retrying to connect until it is up, it will retry for up to 2 minutes.
  - **just wait a bit to allow full opensearch startup, the very first time it starts it may take up to ~1 minute...**

- if you get errors like `failed to obtain node-lock` while booting OpenSearch, ensure that the data directory i.e. `./opensearch_data` is NOT owned by root and is writable (so, chown it again to your user in case).

- error `elastic_transport.ConnectionTimeout: Connection timed out` usually means your opensearch istance is not keeping up with ingestion:
  - increase `ingestion_request_timeout` (**almost always this is the easiest solution**) OR
  - scale up OpenSearch nodes OR
  - reduce parallelism with `parallel_processes_max` OR
  - tune `documents_chunk_size` configuration parameter (i.e. default is 1000, try with 2000 to reduce parallel chunks)
    - keep in mind, though, that a too big `documents_chunk_size` may cause client websocket disconnections (`PayloadTooBig`)

- in case opensearch fails to successfully bootstrap because of errors such as:

  ~~~bash
  elasticsearch  | {"@timestamp":"2024-07-22T15:53:30.889Z", "log.level": "WARN", "message":"flood stage disk watermark [95%] exceeded on [2Mf_sAxtRua8tLTU865kPA][3b369ef109f9][/usr/share/elasticsearch/data] free: 39.7gb[4.3%], all indices on this node will be marked read-only", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[3b369ef109f9][management][T#2]","log.logger":"org.elasticsearch.cluster.routing.allocation.DiskThre
  ~~~

a possible (temporary) solution is to disable disk thresholds in opensearch's configuration after the node starts:

  ~~~bash
  curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'

  curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
  ~~~

## postgreSQL

- **always recreate the whole database with `--reset-collab` if startup fails because of updated tables.**
  - for developers, an example [migration script](../example_migrate_collab.py) is provided to show how to migrate existing data (i.e. `notes`) to a new database schema.

- error `too many connections already` from postgres usually happens when ingesting too many files at once, and should be handled by tuning the configuration parameters:
  - in gulp configuration, check `multiprocessing_batch_size`: it is advised to keep it 0 to perform operation in batches of *number of cores*, raising this value may speed up ingestion a lot but it is more prone to errors.
  - in postgres configuration, increase `max_connections`
  - **better solution is to scale up (increase cores and/or postgres cluster size)**

## websocket

- if too big messages on websocket causes the client to disconnect with something like the following:

  ~~~text
  websockets.exceptions.ConnectionClosedError: sent 1009 (message too big); no close frame received
  ~~~

  diminish the websocket chunk size in the configuration, which by default is 1000 (i.e. set it to 500)

  ~~~json
  {
    // size of the documents chunk when ingesting/querying (default=1000). if you're getting websocket disconnections (PayloadTooBig), try lowering this value or use GulpPluginParameters.override_chunk_size
    "documents_chunk_size": 1000  
  }
  ~~~

  > you may do it per-request by setting `GulpPluginParameters.override_chunk_size` when calling a plugin