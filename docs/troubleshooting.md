
- [troubleshooting](#troubleshooting)
  - [docker](#docker)
    - [devcontainer](#devcontainer)
  - [general](#general)
  - [os](#os)
  - [opensearch / elasticsearch](#opensearch--elasticsearch)
    - [startup errors](#startup-errors)
    - [runtime errors](#runtime-errors)
      - [ingestion](#ingestion)
      - [query](#query)
  - [collab database (postgreSQL)](#collab-database-postgresql)
  - [websocket](#websocket)

[TOC]

# troubleshooting

**before opening issues**, please check the following:

## docker

- **never** use docker as root (i.e. with `sudo), ensure your user has proper permissions to run docker commands.
- 
  ~~~
  sudo groupadd docker
  sudo usermod -aG docker $USER

  # also, you may set proper permissions on the docker socket
  sudo chmod 666 /var/run/docker.sock

  # then logout/login or restart your system
  ~~~

- if you want to be able to run the docker CLI command as a non-root user, add your user to the `docker` user group, re-login, and restart `docker.service` [check here](https://wiki.archlinux.org/title/Users_and_groups#Group_management)

- if you get any issue starting gulp, try to reset gulp's docker volumes with [this](../reset_docker.sh) script.

- beware of vscode `Forwarded Ports`: sometimes, you try to start a container that binds to a port already forwarded by vscode, causing conflicts.

  i.e. if you see something like:
  ~~~bash
  ./test_scripts/start_splunk.sh

  /home/valerino/repos/splunk_volume exists...
  resetting permission on /home/valerino/repos/splunk_volume, user=valerino ...
  [sudo] password for valerino: 
  running splunk ...
  Error response from daemon: No such container: splunk
  3e1c39bc84753c3a09d0a66313c4f39b2cdc7c44ba5d90f9542cba9e7941f051
  docker: Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint splunk (77cf82e507797a5cc68212a260891fe3f5153acd532fe2b4e552365c290f9f70): failed to bind host port for 0.0.0.0:8089:172.17.0.2:8089/tcp: address already in use
  ~~~

  simply go to vscode's `Ports` tab and remove the port forwarding for `8089`.

- if you have issues starting gulp in docker environment, doublecheck each service's URL in gulp_cfg.json : i.e. it has been reported that redis doesn't like localhost but wants the container name in the URL string.
Specifically, replace i.e.redis://:Gulp1234!@localhost:6379/0 with redis://:Gulp1234!@redis:6379/0  in redis_url.

### devcontainer

if you see an error like the following:

~~~
Error response from daemon: Conflict. The container name "/elasticvue" is already in use by container "some_container_id". You have to remove (or rename) that container to be able to reuse that name.
~~~

remove the container with `docker container rm some_container_id` and retry.


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

> it is **strongly advised** to review `request_stats` table on PostgreSQL and docker logs to better pinpoint errors!
>
### startup errors

- couple of errors about cannot connecting to opensearch **are normal if gulp is run immediately right after `docker compose up -d`**: no worries, it just notifies it is retrying to connect until it is up, it will retry for up to 2 minutes.
  - **just wait a bit to allow full opensearch startup, the very first time it starts it may take up to ~1 minute...**

- if you get errors like `failed to obtain node-lock` while booting OpenSearch, ensure that the data directory i.e. `./opensearch_data` is NOT owned by root and is writable (so, chown it again to your user in case).

- in case opensearch fails to successfully bootstrap because of errors such as:

  ~~~bash
  elasticsearch  | {"@timestamp":"2024-07-22T15:53:30.889Z", "log.level": "WARN", "message":"flood stage disk watermark [95%] exceeded on [2Mf_sAxtRua8tLTU865kPA][3b369ef109f9][/usr/share/elasticsearch/data] free: 39.7gb[4.3%], all indices on this node will be marked read-only", "ecs.version": "1.2.0","service.name":"ES_ECS","event.dataset":"elasticsearch.server","process.thread.name":"elasticsearch[3b369ef109f9][management][T#2]","log.logger":"org.elasticsearch.cluster.routing.allocation.DiskThre
  ~~~

  a possible solution is to disable disk thresholds in opensearch's configuration after the node starts:

    ~~~bash
    curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'

    curl -k -u "opensearch:Gulp1234!" -XPUT -H "Content-Type: application/json" https://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
    ~~~

### runtime errors

#### ingestion

- error `elastic_transport.ConnectionTimeout: Connection timed out` usually means your opensearch istance is not keeping up with ingestion:
  - increase `ingestion_request_timeout` (**almost always this is the easiest solution**) **OR**
  - scale up OpenSearch nodes and set the `concurrency_adaptive_num_tasks` configuration parameter in the gulp configuration **OR**
  - reduce parallelism with `parallel_processes_max` **AND/OR** `concurrency_num_tasks` **OR**
  - tune `ingestion_documents_chunk_size` configuration parameter (i.e. default is 1000, try with 2000 to reduce parallel chunks)
    - keep in mind, though, that a too big `ingestion_documents_chunk_size` may cause client websocket disconnections (`PayloadTooBig`)

#### query

- errors like

  ~~~text
  `opensearchpy.exceptions.TransportError: TransportError(500, 'search_phase_execution_exception', 'Query contains too many nested clauses; maxClauseCount is set to 1024'` or `opensearchpy.exceptions.RequestError: RequestError(400, 'search_phase_execution_exception', 'failed to create query: field expansion for [*] matches too many fields, limit: 1024, got: 1741')` 
  ~~~
  
  usually happens when the query issued is **REALLY BIG (i mean REALLY, with hundreds/thousands of clauses with or without wildcards)** and one may attempt to fix this using something like the following in the `os01/environment` section of the `docker-compose.yml` used to start OpenSearch (or, directly in its configuration file)

  ~~~text
  # increase max clause count to 16k (default is 1024)
  indices.query.bool.max_clause_count: 16384
  ~~~

  **NOTE**: `this is discouraged`, and usually means the query should be reworked to include less statements.

- errors like

  ~~~text
  opensearchpy.exceptions.TransportError: TransportError(500, 'search_phase_execution_exception', 'CircuitBreakingException: [parent] Data too large, data for [<transport_request>] would be [1053240832/1003mb], which is larger than the limit of [1048576000/1000mb], real usage: [1053240832/1003mb], new bytes reserved: [0/0mb], usages [request=0/0mb, fielddata=0/0mb, in_flight_requests=1053240832/1003mb, accounting=0/0mb]')`
  ~~~

  during querying usually means that OpenSearch ran out of memory while processing the query.

  you can attempt to tweak the following configuration options to mitigate the issue, or simply scale up the OpenSearch nodes (**recommended**):

  ~~~json
    // number of times to retry on opensearch query circuit breaker exception (default=3)
    "query_circuit_breaker_backoff_attempts": 3,
    // minimum query chunk size limit for circuit breaker backoff (default=100)
    "query_circuit_breaker_min_limit": 100,
    // if set, disables highlights when query circuit breaker is triggered (default=true)
    "query_circuit_breaker_disables_highlights": true,
    
  ~~~
  
  > you may also try to lower `concurrency` settings in the configuration and/or diminish `limit` in `GulpQueryOptions` when querying... but this is a clear indication that OpenSearch is struggling.

- frequent errors like `"opensearchpy.exceptions.ConnectionError: ConnectionError(Cannot connect to host localhost:9200 ssl:default [Multiple exceptions: [Errno 111] ..."` may indicate OpenSearch crashing underneath, which can be verified inspecting Docker logs.

  ~~~
  ...
  [2025-07-23T13:44:31,736][WARN ][o.o.s.b.SearchBackpressureService] [os01] [monitor_only mode] cancelling task [51390] due to high resource consumption [heap usage exceeded [989.1mb >= 10.2mb]]

  [2025-07-23T13:44:31,702][ERROR][o.o.b.OpenSearchUncaughtExceptionHandler] [os01] fatal error in thread [opensearch[os01][search][T#8]], exiting

  java.lang.OutOfMemoryError: Java heap space

    at org.apache.lucene.util.ArrayUtil.growExact(ArrayUtil.java:320) ~[lucene-core-10.1.0.jar:10.1.0 884954006de769dc43b811267230d625886e6515 - 2024-12-17 16:15:44]
  ...
  ~~~

  if this is the case, one may attempt to fix this tweaking the `JVM heap reserved memory` using something like the following in the `os01/environment` section of the `docker-compose.yml` used to start OpenSearch (or, directly in its configuration file)

  ~~~
  # assign 2gb heap memory (default is 512mb)
  OPENSEARCH_JAVA_OPTS: "-Xms2G -Xmx2G"
  ~~~

  **NOTE**: as above, `this is discouraged` since it is most likely a problem with the query which is **way too big** and should be reworked.

  either, this may mean that Opensearch is not keeping up with the query rate from gulp: this may be solved in the following way, similar to ingestion issues:

  - scale up opensearch nodes and set the `concurrency_adaptive_num_tasks` configuration parameter in the gulp configuration **OR**
  - also reducing `parallel_processes_max` **AND/OR** `concurrency_num_tasks` as for ingestion may help

## collab database (postgreSQL)

- **always recreate the whole database with `--reset-collab` if startup fails because of updated tables.**
  - look for migration scripts from/to specific versions in the `collab_migrate` folder: an example [migration script](../collab_migrate/example_migrate_collab.py) is also provided to show how to migrate existing data (i.e. `notes`) to a new database schema.
  - **always backup your data first is wise :)**
  - **do not forget to restart gulp after the migration is done!**
- error `too many connections already` from postgres usually happens when ingesting too many files at once, and should be handled by tuning the configuration parameters:
  - in postgres configuration, increase `max_connections`
  - scale up postgreSQL nodes and set the `concurrency_adaptive_num_tasks` configuration parameter in the gulp configuration
  
## websocket

- if too big messages on websocket causes the client to disconnect with something like the following:

  ~~~text
  websockets.exceptions.ConnectionClosedError: sent 1009 (message too big); no close frame received
  ~~~

  diminish the ingestion chunk size in the configuration, which by default is 1000 (i.e. set it to 500)

  ~~~json
  {
    // size of the documents chunk when ingesting/querying (default=1000). if you're getting websocket disconnections (PayloadTooBig), try lowering this value or use GulpPluginParameters.override_chunk_size
    "ingestion_documents_chunk_size": 1000
  }
  ~~~

  > you may do it per-request by setting `GulpPluginParameters.override_chunk_size` when calling a plugin