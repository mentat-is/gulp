
## Troubleshooting

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
