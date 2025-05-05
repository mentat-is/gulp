# encrich_circl_hash.py

## Overview

The [encrich_circl_hash plugin](../../src/gulp/plugins/encrich_circl_hash.py) provides enrichment functionalities for hashes using the [circl.lu hashlookup API](https://www.circl.lu/services/hashlookup/).

> **NOTE**: The plugin uses the free service from [circl.lu](https://www.circl.lu/services/hashlookup/), please do not abuse it.

## About circl.lu hashlookup API

From their website:

> CIRCL hash lookup is a public API to lookup hash values against known database of files. NSRL RDS database is included and many others are also included. The API is accessible via HTTP ReST API and the API is also described as an OpenAPI. The service is free and served as a best-effort basis.

## Enrich mode

This is a plugin of type `GulpPluginType.ENRICHMENT` it can only be used to enrich existing documents.

### Parameters

The plugin supports the following custom parameters in the `custom_parameters` dictionary:

- `hash_fields`: a list of the document's fields containing the hashes to lookup
- `hash_type`: the type of hash to lookup, if None detects it from field's name.
- `compute`: if set to `True` treats `hash_fields` as containing a hexstring of data and computes its `hash_type`, then looks up the resulting hash

## Example Usage

Here's an example of testing the plugin using the `test_scripts/.py` script:

```bash

```

## GulpDocument

Here's an example `GulpDocument` with some enriched fields:

```json

```



Fields:
- ...

## Common Issues and Solutions

