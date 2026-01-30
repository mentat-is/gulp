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
  - **override_chunk_size**: (Optional) Number of records to process per chunk/batch

## Processing Behavior

### Record Transformation

For each record returned from Elasticsearch/OpenSearch:

1. **Field Mapping**: All fields are processed through the active field mapping configuration, transforming them to ECS schema
2. **Context Extraction**: The `context_field` value is extracted and used to identify or create a context in Gulp
3. **Source Extraction**: The `source_field` value is extracted and used to identify or create a source in Gulp
4. **Timestamp Handling**: The `@timestamp` field is automatically included and processed for proper time ordering
5. **Document Creation**: A GulpDocument is created with all mapped fields, context, source, and timestamp information

### Preview Mode Behavior

When preview mode is enabled:

- A sample of records is returned without full database ingestion
- Useful for validating query configuration before processing large datasets

### Normal Mode Behavior

In normal (non-preview) mode:

- All records are processed and ingested into Gulp
- Context and source are created or retrieved from cache based on configuration
- Returns tuple of (processed_count, total_hits) indicating ingestion results

## Data Flow

1. **Connection**: Plugin connects to Elasticsearch/OpenSearch using provided credentials
2. **Query Execution**: Executes the DSL query with pagination support
3. **Chunked Processing**: Results are processed in configurable chunks for memory efficiency
4. **Record Transformation**: Each record is converted to GulpDocument format
5. **Ingestion**: Transformed documents are ingested into Gulp for storage and analysis

## Error Handling

- **Connection Failures**: If unable to connect to Elasticsearch/OpenSearch, the plugin returns (0, 0) unless in preview mode (raises exception)
- **Missing Fields**: If required `context_field` or `source_field` values are missing from records, an exception is raised
- **Invalid Query**: Invalid Query DSL will result in Elasticsearch/OpenSearch API errors
- **Pagination Issues**: Automatic pagination ensures large result sets are processed reliably

## Timestamp Handling

The plugin automatically ensures the `@timestamp` field exists in the field mapping:

- If not present in the mapping configuration, a default mapping is created
- This field is critical for proper event ordering and timeline analysis
- All ingested records must have timestamp information for correct processing

## Limitations and Considerations

- **Maximum Records**: Results are paginated and chunked for memory efficiency
- **Chunk Size**: Can be customized via `override_chunk_size` parameter (default varies)
- **Authentication**: Currently supports only basic username/password authentication
- **Network**: Requires network connectivity to the Elasticsearch/OpenSearch instance
- **Field Mapping**: All field transformations depend on the active ECS mapping configuration
- **Preview Mode**: Returns sample data; full ingestion only occurs in normal mode
