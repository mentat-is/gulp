"""
definitions for the REST api
"""

API_DESC_REQ_ID = (
    "request id, will be replicated in the response. leave empty to autogenerate."
)
API_DESC_WS_ID = "websocket id to send the response/s to."
API_DESC_PRIVATE = "if set, the object will be private (only the owner can see it)."
API_DESC_COLLAB_LEVEL = "if set, one of the GulpCollabLevel levels to indicate the importance of the collab object (DEFAULT, WARNING, ERROR)."
API_DESC_PLUGIN = (
    "`filename with or without .py/.pyc` of the plugin to process the request with."
)
API_DESC_PLUGIN_TYPE = "the plugin type (ingestion, sigma, extension, query)."
API_DESC_UPLOADFILE = "file to be uploaded."
API_DESC_COUNT = "returns count only (limit, skip, sort are ignored if set)."
API_DESC_INDEX = "the target opensearch index or datastream."
API_DESC_INDEX_TEMPLATE = (
    "an optional index template json to override the default when creating new indexes."
)
API_DESC_TOKEN = "authentication token."
API_DESC_ADMIN_TOKEN = "an authentication token with ADMIN permission."
API_DESC_DELETE_EDIT_TOKEN = "an authentication token with EDIT permission if owner of the object, or DELETE (or ADMIN) permission."
API_DESC_DELETE_TOKEN = "an authentication token with DELETE (or ADMIN) permission."
API_DESC_EDIT_TOKEN = "an authentication token with EDIT permission."
API_DESC_INGEST_TOKEN = "an authentication token with INGEST (or ADMIN) permission."
API_DESC_INGEST_PLUGIN = "plugin to be used for ingestion."
API_DESC_OPERATION_ID = 'the id of an operation registered with operation_create(): this will be set in ingested events as "gulp.operation_id.id".'
API_DESC_CONTEXT_ID = "id of a context to assign to the operation."
API_DESC_SOURCE_ID = "id of a source to assign to the context."

API_DESC_PLUGIN_PARAMETERS = """plugin parameters.

for additional parameters, refer to the plugin documentation.
"""
API_DESC_INGESTION_FILTER = "a filter to be used for ingestion."
API_DESC_QUERY_FILTER = (
    "a filter to be used for querying OpenSearch with common parameters."
)
API_DESC_SIGMA_PLUGIN_PARAMS = "additional parameters for the sigma plugin."
API_DESC_GLYPH = "the id of a glyph registered via glyph_create() on the collab DB."
API_DESC_HEADER_SIZE = "the size of the header in bytes."
API_DESC_HEADER_CONTINUE_OFFSET = "the offset in the file to continue the upload from."

EXAMPLE_INDEX = "test_idx"
EXAMPLE_OPERATION_ID = "test_operation"
EXAMPLE_CONTEXT_ID = "test_context"
EXAMPLE_SOURCE_ID = "test_source"
EXAMPLE_WS_ID = "test_ws"
EXAMPLE_TOKEN = "test_token"
EXAMPLE_REQ_ID = "test_req"
EXAMPLE_PLUGIN = "win_evtx"

"""
5-16 characters length
Only letters, numbers, underscore, dot, dash allowed
"""
REGEX_USERNAME = "^([a-zA-Z0-9_.-]).{4,16}$"

"""
8-32 characters length
At least one uppercase letter
At least one lowercase letter
At least one digit
At least one special character
"""
REGEX_PASSWORD = "^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*-]).{8,32}$"
