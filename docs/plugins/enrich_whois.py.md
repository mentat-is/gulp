# encrich_circl_hash.py

## Overview

The [enrich_whois plugin](../../src/gulp/plugins/enrich_whois.py) enriches documents by performing `rdap` (whois replacement) queries on the specified fields.

## About WHOIS

WHOIS is a protocol and database that stores information about domain names, including who is registered as the owner, contact information, and the domain's registrar. It's like a public directory for domain names, providing transparency about who owns and manages them.

A WHOIS record typically includes the registrant's name, address, email, phone number, and the registrar's details, registration date and other relevant details.

## Enrich mode

This is a plugin of type `GulpPluginType.ENRICHMENT` it can only be used to enrich existing documents.

### Parameters

The plugin supports the following custom parameters in the `custom_parameters` dictionary:

- `host_fields`: a list of ip fields to enrich
- `whois_fields`: list of whois fields to keep (only used if full_dump is set to false)
- `full_dump`: get all the whois information (ignore whois_fields)
- `unify_dump`: keep all results in a single field

> **NOTE**: for a complete list of whois_fields take a look at the [ipwhois documentation](https://ipwhois.readthedocs.io/en/latest))

> **NOTE**: `whois_fields` support simple wildcards so you can for example specify `objects.*` to get all childs of the `objects` whois field

## GulpDocument

Here's an example `GulpDocument` with some enriched fields:

```json
{
  "gulp.timestamp": 1608420324000000000,
  "gulp.enrich_whois.source_ip.network.country": "DE",
  "gulp.enrich_whois.source_ip.asn_description": "CLOUVIDER Clouvider - Global ASN, GB",
  "gulp.enrich_whois.source_ip.asn_country_code": "SC",
  "gulp.enrich_whois.source_ip.network.end_address": "176.222.58.255",
    ...
  "source.ip": "176.222.58.90",
}
```


## Common Issues and Solutions

- If enriching many documents using `full_dump` it might be better to also use `unify_dump`, to reduce the number of objects on elastic/opensearch
- Using `unify_dump` may reduce the querying capabilities of gulp, so for complex queries it is better to extract the wanted fields and keep `unify_dump` set to `False`  