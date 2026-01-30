# Query Elasticsearch/OpenSearch

This plugin implements an extension for Gulp that enables querying and extracting data from Elasticsearch or OpenSearch indices. It transforms the query results into standardized GulpDocument objects with proper timestamp handling, context mapping, and source information. This allows users to ingest data from their Elasticsearch/OpenSearch instances directly into Gulp for analysis and correlation.

## Overview

The query_elasticsearch plugin provides a bridge between Gulp and Elasticsearch/OpenSearch clusters. It handles:

- Connection management to both Elasticsearch and OpenSearch instances
- Authentication with username and password
- Field mapping and transformation to ECS (Elastic Common Schema) format
- Context and source identification and extraction
- Timestamp processing and normalization
- Pagination and chunked processing of large result sets
- Preview mode for testing queries before full ingestion

> **This plugin can be used with both plain elasticsearch/opensearch  or other platforms using them (i.e. wazuh, ...)**

## Configuration Parameters

All parameters must be provided in the `custom_parameters` object when configuring the plugin. The following parameters are required:

### Required Parameters

#### `uri`
- **Type**: `string`
- **Required**.
- **Description**: The connection URI for the Elasticsearch or OpenSearch instance. This should include the protocol (http/https) and the port number.
- **Example**: `http://localhost:9200` or `https://opensearch.example.com:9200`

#### `index`
- **Type**: `string`
- **Required**.
- **Description**: The name of the Elasticsearch/OpenSearch index to query. This is the target index where the DSL query will be executed.
- **Example**: `logs-2025.01`, `security-events`

#### `context_field`
- **Type**: `string`
- **Required**.
- **Description**: The name of the field in your data that represents the context (e.g., hostname, server name, or system identifier). This field value is used to identify or create a context within Gulp.
- **Example**: `hostname`, `server_name`, `source_system`

#### `source_field`
- **Type**: `string`
- **Required**.
- **Description**: The name of the field in your data that represents the log source (e.g., log file name, collector name, or data source identifier). This field value is used to identify or create a source within Gulp.
- **Example**: `source.file`, `data_source`, `log_name`

### Optional Parameters

#### `username`
- **Type**: `string`
- **Optional**.
- **Default**: `None`
- **Description**: The username for authentication with the Elasticsearch/OpenSearch instance. Leave empty if the instance does not require authentication.
- **Example**: `admin`, `elasticsearch_user`

#### `password`
- **Type**: `string`
- **Optional**.
- **Default**: `None`
- **Description**: The password for authentication with the Elasticsearch/OpenSearch instance. Leave empty if the instance does not require authentication.
- **Example**: Your secure password

#### `is_elasticsearch`
- **Type**: `boolean`
- **Optional**.
- **Default**: `True`
- **Values**: `True` (Elasticsearch) or `False` (OpenSearch)
- **Description**: Determines whether to connect to Elasticsearch or OpenSearch. Set to `True` for Elasticsearch clusters, `False` for OpenSearch clusters.
- **Note**: The connection method and client library differs between the two systems, so this parameter is critical.

#### `context_type`
- **Type**: `string`
- **Optional**.
- **Default**: `context_id`
- **Values**: `context_id` or `context_name`
- **Description**: Specifies how the context value should be interpreted:
  - `context_id`: The `context_field` value is treated as a unique identifier for an existing context
  - `context_name`: The `context_field` value is a descriptive name; Gulp will create or retrieve the context by name
- **Example**: Use `context_id` if your field contains numeric IDs, use `context_name` if it contains human-readable names

#### `source_type`
- **Type**: `string`
- **Optional**.****
- **Default**: `source_id`
- **Values**: `source_id` or `source_name`
- **Description**: Specifies how the source value should be interpreted:
  - `source_id`: The `source_field` value is treated as a unique identifier for an existing source
  - `source_name`: The `source_field` value is a descriptive name; Gulp will create or retrieve the source by name
- **Example**: Use `source_id` if your field contains numeric IDs, use `source_name` if it contains file paths or names
  
#### `context_field`
- **Type**: `string`
- **Required**.
- **Description**: Specifies the name of field representing the context. In according with context_type the field value will used as context_id or context_name inside Gulp.
- **Example**: In gulp document the name of filed is gulp.context_id

#### `source_field`
- **Type**: `string`
- **Required**.
- **Description**: Specifies the name of field representing the source. In according with source_type the field value will used as source_id or source_name inside Gulp.
- **Example**: In gulp document the name of filed is gulp.source_id
  
## Query Format

The query parameter must be a valid Elasticsearch/OpenSearch Query DSL JSON object, passed as a string with single quotes converted to double quotes.

## Input Parameters for Query Execution

When executing a query, the following parameters are provided:

### Parameter Explanations

- **preview-mode**: When enabled, the plugin returns a sample of results without full ingestion into Gulp
- **q**: The Elasticsearch/OpenSearch DSL query as a JSON string
- **plugin**: The plugin name (`query_elasticsearch`)
- **operation_id**: The Gulp operation identifier for this query
- **plugin_params**: Configuration object containing:
  - **custom_parameters**: The parameters listed in the Configuration Parameters section above
  - **mapping_parameters**: (Optional) The mapping parameters in according with gulp documentation.
