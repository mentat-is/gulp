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

[Description](#description) - [Architecture](#architecture) - [Installation](#installation) - [Run & examples](#commandline-examples) - [GUI](#clients) - [Troubleshooting](./docs/troubleshooting.md)

</div>

## description

Gulp is a powerful software tool designed to streamline incident response and analysis. Its core features includes:

- **Data Ingestion Plugins**: Gulp can ingest data from a variety of sources, thanks to its versatile plugin system.
- **OpenSearch and ECS**: Gulp is built on OpenSearch and uses the _Elasticsearch Common Scheme (ECS)_ as its ingestion format, ensuring compatibility and ease of use.
- **High-Speed Multiprocessing Engine**: Gulp's engine is designed for speed, offering fast ingestion and querying capabilities through multiprocessing.
- **Query using SIGMA rules**: Gulp supports querying using Sigma Rules, allowing for easy, one-click queries with thousands of rules in parallel.
- **Collaboration Platform**: Gulp includes a collaboration platform, enabling teams to work together on the same incident. Features include note-taking, highlighting, and link adding.
- [**An innovative UI**](https://github.com/mentat-is/gulpui-web): Gulp's user interface includes multiple on-screen per-context(i.e. a log source) zoomable timelines for visualizing events, making it easier to understand and analyze incidents.
- **Scalable**: Gulp is designed with scalability in mind. As your data and team grow, you can simply add more cores to increase parallel ingestion and query capabilities, and more OpenSearch and PostgreSQL hosts. This makes Gulp a flexible solution that can adapt to your evolving needs!
- **Python based**: Gulp is written in Python, leveraging open-source libraries whenever possible. This maximizes ease of adoption from the community, as Python is widely used and understood.

[here is a detailed datasheet](./DATASHEET.md)

## architecture

- [GULP architecture](./docs/architecture.md)

### plugins development

- [plugins and mapping](./docs/plugins_and_mapping.md)
- [testing guidelines](./docs/testing.md)

### integration with other applications

gulp can be of course [integrated with other applications](./docs/integration.md) !

> both websocket and REST API is available!

## installation

- [docker](<./docs/install_docker.md>)
- [install from sources/dev setup](./docs/install_dev.md)
- [installing extra plugins](./docs/install_dev.md#7-optional-installing-extra-plugins)

### clients

[gulp web ui](https://github.com/mentat-is/gulpui-web)

### environment variables

the following environment variables may be set to override configuration options.

- `GULP_BIND_TO_ADDR`, `GULP_BIND_TO_PORT` : if set, gulp will listen to this interface and port (either, the default `0.0.0.0`, `8080` is used).
  - for the override to work, both `GULP_BIND_TO_ADDR` and `GULP_BIND_TO_PORT` must be specified, either the value of one alone is ignored.

- `GULP_WORKING_DIR`: this is the **working directory** for gulp (defaults to `~/.config/gulp`), which contains:
  - `gulp_cfg.json`: the configuration, initialized with [template](./gulp_cfg_template.json) if not present
  - `plugins`: optional extra plugins (have precedence over `$INSTALLDIR/plugins`)
  - `mapping_files`: optional extra mapping files (have precedence over `$INSTALLDIR/mapping_files`)
  - `certs`: optional [SSL](#ssl) certificates for HTTPS
  - `ingest_local` directory to store big files for quick ingestion (`ingest_local` API)
  - `tmp_upload` folder to cache partial uploads during ingestion

- `GULP_OPENSEARCH_URL`: if set, overrides `opensearch_url` in the configuration to.
- `GULP_POSTGRES_URL`: if set, overrides `postgres_url` in the configuration.
- `GULP_INTEGRATION_TEST`: **TEST ONLY**, this must be set to 1 during integration testing (i.e. client api) to disable debug features which may interfere.

### exposed services

> using the default [docker-compose.yml](./docker-compose.yml)

- [gulp swagger page on http://localhost:8080/docs](http://localhost:8080/docs)
- [gulp web UI on http://localhost:3000](http://localhost:3000)
  - **user/pwd: `admin/admin`** (default gulp admin user)

- postgreSQL on **localhost:5432**
  - **user/pwd: `postgres/Gulp1234!`**

- [adminer on http://localhost:8001](http://localhost:8081) to manage postgreSQL.
  - **server/user/pwd: `postgres/postgres/Gulp1234!`**

- [opensearch on http://localhost:9200](http://localhost:9200)
  - **user/pwd: `admin/Gulp1234!`**

- [elasticvue on http://localhost:8082](http://localhost:8082) to visualize OpensSearch indexes.
- [opensearch dashboards on http://localhost:5001](http://localhost:5601) for a more comprehensive OpenSearch management.

- [vsftpd on port 21(ftp) or 21000 (sftp)](ftp://localhost:21) to manage files in `$GULP_WORKING_DIR`
  - **user/pwd: `gulp/Gulp1234!`**

### SSL

to use SSL, the following configuration options and environment variables may be provided:

#### OpenSearch

- Gulp configuration
  - `opensearch_verify_certs`: set to `false` to skip server verification
- environment variables
  - `$GULP_WORKING_DIR/certs/opensearch-ca.pem`: CA certificate for Gulp to connect to the Opensearch server
  - `$GULP_WORKING_DIR/certs/opensearch.pem`: client certificate for Gulp to connect to the Opensearch server
  - `$GULP_WORKING_DIR/certs/opensearch.key`: ***passwordless*** client certificate key

#### PostgreSQL

- Gulp configuration
  - `postgres_ssl`: use SSL for postgres connection, set to `false` to not use.
  - `postgres_verify_certs`: set to `false` to skip server verification
- environment variables
  - `$GULP_WORKING_DIR/certs/postgres-ca.pem`: CA certificate for Gulp to connect to the PostgreSQL server
  - `$GULP_WORKING_DIR/certs/postgres.pem`: client certificate for Gulp to connect to PostgreSQL server
  - `$GULP_WORKING_DIR/certs/postgres.key`: client certificate key

# gulp

- Gulp configuration
  - `https_enforce`: set to `true` to enforce connection to Gulp only through HTTPS
  - `https_enforce_client_certs`: set to `true` to enforce check of client certificates signed by `gulp-ca.pem` CA
- environment variables
  - `$GULP_WORKING_DIR/certs/gulp-ca.pem`: Gulp CA
  - `$GULP_WORKING_DIR/certs/gulp.pem`: Gulp server certificate
  - `$GULP_WORKING_DIR/certs/gulp.key`: Gulp server certificate key

# sftpd

- `$GULP_WORKING_DIR/certs/sftpd.pem`: server certificate + CA
- `$GULP_WORKING_DIR/certs/sftpd.key`: server certificate key

## commandline examples

default startup, creates collab database with an operation names `test_operation` on the very first run.

~~~bash
gulp
~~~

> to detect if gulp has already run once check for `~/.config/gulp/.first_run_done` and delete it to revert to first run on the next run.

deletes data related to `ALL` existing operations, both on collaboration database and OpenSearch.

~~~bash
gulp --reset-collab
~~~

deletes data related to `ALL` existing operations, both on collaboration database and OpenSearch, in the end creates/recreates `my_operation`.

~~~bash
gulp --reset-collab --create my_operation
~~~

acts only on `my_operation`: creates/recreates operation, deletes all related data both on collaboration database and OpenSearch.

~~~bash
gulp --create my_operation
~~~
