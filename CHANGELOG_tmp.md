
- collab: MAJOR internal refactor (session handling is correct now, no more leaked AsyncSessions)
- collab/stats: MAJOR refactor, check new GulpRequestStats struct: GulpRequestStats.req_type, GulpRequestStats.data. GulpRequestStats.data may be GulpIngestionStats (req_type="ingest"), or GulpQueryStats(req_type="query"), or GulpUpdateDocumentsStats (req_type="enrich" or req_type="rebase"). also check for "tracking progress" in the API docs for details.
- core: improved plugin load()/unload()
- core: removed duplications and generic cleanup
- core: removed "enrichment during ingestion" capability (not supported by the UI, anyway doesn't make sense with the current workflow ... maybe rethink and reimplement later with proper support)
- core/GulpPluginParameters: added "preview_mode" there (no more passed separately in API, i.e. ingest_file, etc...)
- core/ws_api: WSDATA_COLLAB_CREATE_UPDATE splitted in WSDATA_COLLAB_CREATE (ws_api/GulpCollabCreatePacket) and WSDATA_COLLAB_UPDATE (ws_api/GulpCollabUpdatePacket), and format a bit revised (i.e. type=GulpCollabCreatePacket.obj.type)
- core/ws_api: ugly data.data when parsing GulpWsData is gone (GulpWsData.data renamed to `payload`)
- core/user: GulpUserDataQueryHistoryEntry.query, query_options renamed to q, q_options (consistent with everything else regarding queries)

- plugins/all: plugin.type() is a string, not list (unneded, a plugin may be only of a single type)
- plugins/enrich_whois: made really asyncio compatible (before it was blocking the event loop!)

- rest_api: generic audit of all parameters (mandatory/optional)
- rest_api/operation: removed "context_id", "operation_id" in source_delete (not needed, source object has it)
- rest_api/operation: context_list,source_list returns 404 when operation is not found
- rest_api/operation: context_delete, context_update: added ws_id
- rest_api/operation: source_delete, source_update: added_ws_id
- rest_api/operation: operation_delete, operation_update: added ws_id
- rest_api/operation: operation_create, removed index (index is always derived by name), added "operation_data" (forgot before)
- rest_api/operation: added operation_cleanup to delete collab objects (i.e. requests, notes, ...) without deleting/recreating the operation
- rest_api/collab: "list" API for note/highlight/link/story (and generally all collab objects related to an operation) have "operation_id" parameter added
- rest_api/query: query_external, "q" is no more a list (just a plain string)
- rest_api/query: query_sigma, q_options.create_notes must be set to True manually by the caller (as in every other query api, by default it is False)
- rest_api/user: user_create "email" is mandatory, added "user_data", "glyph_id" (as in user_update)
- rest_api/utility: gulp_reset, plugin_get (ui_plugin_get remains, for the ui), trigger_gc: REMOVED
- rest_api/utility: request_get_by_id, request_cancel, request_set_completed: operation_id arg removed (not needed)

- removed setup.sh/bootstrap.sh (installation should be made manually or with docker, following the docs)
