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

[Description](#description) - [Architecture](#architecture) - [Installation](#installation) - [Run & examples](#run) - [GUI](#clients) - [Troubleshooting](./docs/Troubleshooting.md)

</div>

## description

Gulp is a powerful software tool designed to streamline incident response and analysis. Its core features include:

## architecture

[GULP architecture](./docs/Architecture.md)

### current features

- **Data Ingestion Plugins**: Gulp can ingest data from a variety of sources, thanks to its versatile plugin system.
- **OpenSearch and ECS**: Gulp is built on OpenSearch and uses the _Elasticsearch Common Scheme (ECS)_ as its ingestion format, ensuring compatibility and ease of use.
- **High-Speed Multiprocessing Engine**: Gulp's engine is designed for speed, offering fast ingestion and querying capabilities through multiprocessing.
- **Query using SIGMA rules**: Gulp supports querying using Sigma Rules, allowing for easy, one-click queries with thousands of rules in parallel.
- **Collaboration Platform**: Gulp includes a collaboration platform, enabling teams to work together on the same incident. Features include note-taking, highlighting, and link adding.
- **Innovative UI**: Gulp's user interface includes multiple on-screen per-context(i.e. a log source) zoomable timelines for visualizing events, making it easier to understand and analyze incidents.
- **Scalable**: Gulp is designed with scalability in mind. As your data and team grow, you can simply add more cores to increase parallel ingestion and query capabilities, and more OpenSearch and PostgreSQL hosts. This makes Gulp a flexible solution that can adapt to your evolving needs!
- **Python based**: Gulp is written in Python, leveraging open-source libraries whenever possible. This maximizes ease of adoption from the community, as Python is widely used and understood.
- **WEB UI client**: [a full fledged web-ui](https://github.com/mentat-is/gulpui-web) is available!

## timeline

Here's the rough timeline we put together, it is subject to change:

![timeline](.images/timeline.png)

## MOCA 2024 presentation

[here is our presentation](https://docs.google.com/presentation/d/e/2PACX-1vTynDHQqr2hN6d3Mlq7UGADR-SAePDUD_M9CdPd2VrS5n11JkrrWMj00KQb9flhG8i2VUlKOn2tr5Ny/pub?start=false&loop=false&delayms=3000) at [MOCA2024](https://moca.camp).

## installation

### TLDR ;)

| :warning: WARNING                                                                          |
| :----------------------------------------------------------------------------------------- |
| [READ THIS TO INSTALL until we have a stable solution](https://github.com/mentat-is/gulp/issues/37) |

<del>
#### docker
this will start the provided [docker-compose.yml](./docker-compose.yml) in the current directory and uses [gulp_cfg_template.json](./gulp_cfg_template.json) and [default .env](./.env) as base.
~~~bash
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/bootstrap.sh -o ./bootstrap.sh && chmod 755 ./bootstrap.sh && ./bootstrap.sh
~~~
#### from source
this will install from sources and create a `gulp` folder, inside the current directory.
~~~bash
curl https://raw.githubusercontent.com/mentat-is/gulp/refs/heads/develop/setup.sh | sudo bash
~~~
</del>

### exposed services

> using the default configuration

- [gulp swagger page on http://localhost:8080/docs](http://localhost:8080/docs)
- [gulp web UI on http://localhost:3000](http://localhost:3000)
  - **user/pwd: `admin/admin`** (default gulp admin user)
- postgreSQL on **localhost:5432**
  - **user/pwd: `postgres/Gulp1234!`**
- [adminer on http://localhost:8001, to manage postgreSQL](http://localhost:8081)
  - **server/user/pwd: `postgres/postgres/Gulp1234!`**
- [opensearch on http://localhost:9200](http://localhost:9200)
  - **user/pwd: `admin/Gulp1234!`**
  - [elasticvue on http://localhost:8082](http://localhost:8082)

### installation details

- [docker](<./docs/Install Docker.md>)
- [install from sources](<./docs/Install Dev.md>)
- [installing extra plugins](<./docs/Install Dev.md/#-installing-extra-plugins>)

### environment variables

the following environment variables may be set to override configuration options.

- `BIND_TO_ADDR`, `BIND_TO_PORT` : if set, gulp will listen to this interface and port (either, the default `0.0.0.0`, `8080` is used.
  - for the override to work, both `BIND_TO_ADDR` and `BIND_TO_PORT` must be specified, either the value of one alone is ignored.

- `PATH_CONFIG`: if set, will be used as path for the configuration file (either, `~/.config/gulp/gulp_cfg.json` will be used)

- `PATH_PLUGINS_EXTRA`: if set, an extra directory where to search plugins into.
  - also have an `extension` subdirectory for `extension` plugins
  - plugins are loaded by default from `$INSTALLDIR/plugins`
  - if a plugin exists in both directories, `$PATH_PLUGINS_EXTRA`have precedence (i.e. to load newer version)

- `PATH_MAPPING_FILES_EXTRA`: if set, an extra directory where to search mapping files into.
  - mapping files are loaded by default from `$INSTALLDIR/mapping_files`
  - if a mapping file exists in both directories, `$PATH_MAPPING_FILES_EXTRA` has precedence (i.e. to allow newer mapping versions)  
  -
- `PATH_INDEX_TEMPLATE`: if set, path to load the index template used when setting up new indexes (either, the [default](./src/gulp/api/mapping/index_template/template.json) template is used).

- `PATH_CERTS`: if set, overrides `path_certs` in the configuration to specify a path to load SSL certificates from (for HTTPS).
- `OPENSEARCH_URL`: if set, overrides `opensearch_url` in the configuration to.
- `POSTGRES_URL`: if set, overrides `postgres_url` in the configuration.
- `GULP_INTEGRATION_TEST`: **TEST ONLY**, this must be set to 1 during integration testing (i.e. client api) to disable debug features which may interfere.

### SSL

to use HTTPS, the following certificates must be available:

> client certificates for `opensearch` and `postgresql` are used if found, `opensearch` key password is not supported.

- opensearch
  - `opensearch_verify_certs: false` may be used to skip server verification
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

## clients

### web UI

get it [here](https://github.com/mentat-is/gulpui-web) !

- usage demo, uses the old .NET UI

  [![.NET ui demo](https://img.youtube.com/vi/3WWzySRQZK8/0.jpg)](https://youtu.be/3WWzySRQZK8?t=1349)

## run

[with docker](<./docs/Install Docker.md#run-with-docker-compose>) or [with install from sources](<./docs/Install Dev.md#7-run>)

> currently, we recommend to install from sources!

Updates soon...

## test

[read here](./docs/Testing%20Plugins.md)
