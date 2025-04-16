# regex.py

## Overview

The [regex plugin](../../src/gulp/plugins/regex.py) is designed to process text files using regular expressions with named capture groups. It analyzes files line by line, applying a specified regex pattern and extracting matched groups into structured `GulpDocument` objects.

> **NOTE**: The plugin requires a 'timestamp' named group in your regex pattern as each document must have a "@timestamp" field.

## Regular Expression Format Brief

Regular expressions (regex) are powerful pattern-matching tools used to identify and extract specific text patterns. In this plugin:

- Named capture groups (`(?P<name>pattern)`) are used to extract and label data
- Each line of the input file is matched against the provided regex pattern
- Only successfully matched lines are processed into documents
- The plugin requires at least one named capture group called `timestamp`
- Non-matching lines are logged as warnings and counted as failed records

## Standalone Mode

The regex plugin is primarily designed to be used in standalone mode, applying regex patterns directly to input files.

### Parameters

The regex plugin supports the following custom parameters in the `custom_parameters` dictionary:

- `encoding`: Specifies the character encoding to use when opening the file (default=None, which typically falls back to system default)
- `date_format`: Specifies to timestamp field format, if not specified attempts to autoparse (default=None)
- `regex`: The regular expression pattern to apply to each line (must use named groups)
- `flags`: Integer representing regex compilation flags (default=0)
  - Example flags: 2 (`re.IGNORECASE`), 8 (`re.MULTILINE`), 16 (`re.DOTALL`)

Additional parameters can be specified in the `mapping_parameters` dictionary:

- `mappings`: Dictionary of field mappings to apply
- `mapping_file`: Path to the JSON file containing the mapping
- `mapping_id`: Identifier of the mapping to use from the mapping file

## Stacked Mode

In stacked mode, the stacked plugin runs the `regex` plugin first which sequentially calls the stacked plugin's `record_to_gulp_document` function.
This is useful to avoid re-writing the regex processing logic, and focus on the parsing of the data instead.
Other common use cases for using regex as a stacked plugin include:

- The stacked plugin requires specific configuration parameters
- Files need preprocessing before regex parsing (e.g., line filtering, character replacement)
- Custom post-processing of regex-matched data is required

## Example Usage
Here's an example of testing the plugin using the `test_scripts/ingest.py` script:

```bash
python test_scripts/ingest.py \
  --plugin regex \
  --path samples/logfile.txt \
  --plugin_params '{
    "custom_parameters":{
      "regex":"(?P<timestamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}) (?P<level>\\w+) (?P<message>.*)",
      "flags":0
    }'
```

## Common Issues and Solutions

- **No matches found**: Make sure your regex pattern correctly matches the format of your file lines
- **Missing timestamp**: Ensure your regex includes a named group called 'timestamp'
- **Timestamp parsing errors**: Ensure timestamp is in a format recognized by gulp, if not make a stacked plugin to convert the time format
- **Performance issues**: Overly complex regex patterns may slow down processing for large files

## Regular Expression Tips

- Test your regex pattern with a tool like [regex101.com](https://regex101.com/) before using it
- Use non-capturing groups `(?:pattern)` for groups you don't need to extract
- Remember to escape backslashes in JSON configuration (e.g., `\\d` instead of `\d`)
- Use appropriate flags for case sensitivity, multiline matching, etc.
