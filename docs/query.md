- [querying gulp](#querying-gulp)
  - [overview](#overview)
  - [debugging query issues](#debugging-query-issues)

# querying gulp

this section documents the main query endpoints of the Gulp REST API: `query_gulp`, `query_raw`, `query_sigma`, and `query_external`. 

these endpoints provide flexible and powerful ways to query data in Gulp, leveraging OpenSearch as the backend.

understanding their relationships and the underlying query mechanics is essential for advanced usage and troubleshooting.

## overview

- **`query_gulp`**: for simple queries using pre-defined filters: recommended for most use cases where you want to leverage Gulp's built-in filtering logic.

- **`query_raw`**: the core endpoint for advanced users, also used internally by the [gulp web ui](https://github.com/mentat-is/gulpui-web): accepts raw [OpenSearch Query DSL](https://opensearch.org/docs/latest/query-dsl/) queries, giving you full control over the search logic.

    the following is an example of what you may use as the `q` parameter in `query_raw`:

    ```json
        [{
            "query": {
                "query_string": {
                    "query": "event.code: 4625 AND gulp.operation_id: test_operation"
                }
            }
        }]  
    ```

- **`query_sigma`**: accepts [Sigma rules](https://github.com/SigmaHQ/sigma), translates them to OpenSearch queries, and executes them: useful for threat detection and security analytics.

- **`query_external`**: for querying external sources via plugins: relies on plugin-specific logic to query the external source (i.e. a SIEM) using its specific DSL/API.

> **NOTE:** except for `query_external`, all the above endpoints ultimately resolve to `query_raw` internally.

## debugging query issues

for advanced debugging or to understand exactly what is being executed, be sure to run gulp with debug logs enabled using `--log-level debug` command line paramter!
