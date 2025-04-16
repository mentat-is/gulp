# csv.py

## Overview

The [csv plugin](../../src/gulp/plugins/csv.py) ingests CSV (Comma-Separated Values) files into the system. CSV is a simple text format for storing tabular data where each line represents a row and values are separated by a delimiter (typically a comma). The plugin can be used in standalone mode or as a base for other specialized plugins (in [stacked](../Creating%20Plugins.md#stacked%20plugins) mode).

> **NOTE**: Since each document in the system requires a "@timestamp" field, it's strongly recommended to provide a mapping file that includes the "@timestamp" field mapping.

## CSV Format Brief

CSV (Comma-Separated Values) is a simple, widely-used format for storing tabular data.
Key characteristics:

- Each row represents a record and is terminated by a newline
- Fields/columns are separated by a delimiter (commonly a comma, but can be tabs, semicolons, etc.)
- Fields containing the delimiter, newlines, or quotes may be enclosed in double quotes
- Different CSV variations exist (hence the dialect parameter)

## Standalone Mode

When used in standalone mode (called directly during ingestion), you simply need to provide a CSV file along with appropriate configuration settings and a mapping file.

### Parameters

The CSV plugin supports the following custom parameters in the `custom_parameters` dictionary:

- `encoding`: Specifies the character encoding to use when opening the file (default="utf-8")
- `date_format`: Specifies to timestamp field format, if not specified attempts to autoparse (default=None)
- `delimiter`: Defines the character used to separate values in the CSV file (default=",")
- `dialect`: Specifies which of Python's supported CSV dialects to use:
  - 'excel': Standard Excel CSV format (default)
  - 'excel-tab': Excel's tab-delimited format
  - 'unix': Uses '\n' as line terminator and quotes all fields

Additional standard parameters can be specified in the `mapping_parameters` dictionary:

- `mappings`: Dictionary of field mappings to apply
- `mapping_file`: Path to the JSON file containing the mapping
- `mapping_id`: Identifier of the mapping to use from the mapping file

## Stacked Mode

In stacked mode, the stacked plugin runs the `csv` plugin first which sequentially calls the stacked plugin's `record_to_gulp_document` function.

This is useful to avoid re-writing the csv processing logic, and focus on the prasing of the data instead. 
Other common use cases for using csv as a stacked plugin include:

- The stacked plugin requires specific configuration parameters
- Files need preprocessing before CSV parsing (e.g., header injection/manipulation, character replacement)
- Custom post-processing of parsed CSV data is required

### Stacked Plugin Examples

Here are examples of plugins that stack on top of the CSV plugin:

- *Currently no examples available*

## Example Usage

Here's an example of testing the plugin using the `test_scripts/ingest.py` script:

```bash
python test_scripts/ingest.py \
  --plugin csv \
  --path samples/ \
  --plugin_params '{
    "mapping_parameters":{
      "mapping_file":"mftecmd_csv.json", 
      "mapping_id":"j"
    }, 
    "custom_parameters":{
      "dialect":"excel"
    }
  }'
```

## Common Issues and Solutions

- **Character encoding problems**: If you see garbled text or errors, try specifying a different `encoding` value
- **Field separation issues**: Use the `delimiter` parameter to match your file's actual separator
- **Complex CSV formats**: Choose the appropriate `dialect` or consider creating a stacked plugin for preprocessing