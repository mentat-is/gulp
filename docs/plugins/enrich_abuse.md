# enrich_abuse.py

## Overview ✅

The `enrich_abuse` plugin queries the abuse.ch (URLhaus) API to enrich documents containing URLs or file hashes with information about whether they are known to be malicious or have been reported to abuse.ch.

This plugin supports:
- URL lookups against the URLhaus database
- Hash lookups (payload lookup) using MD5 / SHA256 (autodetect supported)
- Configurable targeting of document fields and plugin parameters


> **NOTE**: abuse.ch provides a public API (URLhaus). Use this service responsibly and avoid high-volume or abusive requests.

further information at:

- URLhaus API and docs: `https://urlhaus.abuse.ch/` and `https://urlhaus-api.abuse.ch`

---

## Parameters ⚙️

The plugin exposes custom parameters in `custom_parameters`:

- `query_type` (str) — `url` or `hash`. Default: `url`. Determines whether the plugin queries URLhaus by URL or by hash.
- `hash_type` (str or None) — `md5`, `sha256`, or `None` to autodetect by hash length. Only used when `query_type` is `hash`. Default: `None`.
- `auth_key` (str, optional) — abuse.ch auth key. If not supplied as a plugin parameter the plugin will check the GULP config for `enrich_abuse.auth_key`. If missing, the plugin raises an exception.

Example plugin parameters:

```json
{
  "query_type": "url",
  "auth_key": "<YOUR_ABUSECH_AUTH_KEY>"
}
```

or, if you prefer to pass the api key in the GULP configuration, just create the following key:

```json
"enrich_abuse": {
  "auth_key": "<YOUR_ABUSECH_AUTH_KEY>"
}
```
