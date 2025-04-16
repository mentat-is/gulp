# zip.py

## Overview

The [zip plugin](../../src/gulp/plugins/zip.py) provides functionality to ingest ZIP archive files. It extracts files from ZIP archives, processes their metadata, and converts them into `GulpDocument` objects for further processing.

> **NOTE**: The plugin generates a timestamp for each extracted file based on the file's date_time information.

## ZIP Format Brief

ZIP is a widely-used archive file format that supports lossless data compression, encryption, and storage of multiple files.
In this plugin:

- Files are extracted from the ZIP archive
- File metadata (timestamps, permissions, etc.) is processed
- MIME types are detected for extracted files
- Hashes can be calculated for file contents
- Password-protected archives are supported
- Content can be kept or discarded based on configuration

## Standalone Mode

When used in standalone mode, the ZIP plugin processes archive files directly, converting extracted files and their metadata to `GulpDocument` objects.

### Parameters

The ZIP plugin supports the following custom parameters in the `custom_parameters` dictionary:

- `password`: Password to decrypt the zip file (default=None)
- `encoding`: Encoding to use to decode strings (default="utf8")
- `hashes`: Algorithms to use to calculate hash of zip files content (default=["sha1"])
- `chunk_size`: Size of chunks used when reading files (default=2048)
- `keep_files`: If True, event.original will contain the file extracted from the zip (default=False)

Additional parameters can be specified in the `mapping_parameters` dictionary:

- `mappings`: Dictionary of field mappings to apply
- `mapping_file`: Path to the JSON file containing the mapping
- `mapping_id`: Identifier of the mapping to use from the mapping file

## Stacked Mode

In stacked mode, the stacked plugin runs the `zip` plugin first which sequentially calls the stacked plugin's `record_to_gulp_document` function.
This is useful to avoid re-writing the ZIP processing logic, and focus on the parsing of the data instead.
Other common use cases for using zip as a stacked plugin include:

- The stacked plugin requires specific configuration parameters
- Files need preprocessing before ZIP extraction (e.g., password discovery, integrity validation)
- Custom post-processing of extracted files is required

## Example Usage

Here's an example of testing the plugin using the `test_scripts/ingest.py` script:

```bash
python test_scripts/ingest.py \
 --plugin zip \
 --path samples/archive.zip \
 --plugin_params '{
   "mapping_parameters":{
     "mapping_file":"zip_mapping.json",
     "mapping_id":"default"
   },
   "custom_parameters":{
     "password":"secret",
     "hashes":["sha1", "md5"],
     "keep_files":true
   }
 }'
```

## Common Issues and Solutions

- **Incorrect password**: Ensure the password is correct for encrypted archives
- **Character encoding problems**: If you see garbled text or errors, try specifying a different `encoding` value
- **Memory issues with large files**: Adjust the `chunk_size` parameter for better memory efficiency
- **Missing file content**: Set `keep_files` to true if you need the actual file content
- **Decompression fails**: This plugin uses python's [ziplib](https://docs.python.org/3/library/zipfile.html) library, hence the same limitations apply

## ZIP Processing Tips

- For large archives, process them with `keep_files` set to `False` to reduce memory and disk usage
- Use multiple hash algorithms for better file identification and integrity verification