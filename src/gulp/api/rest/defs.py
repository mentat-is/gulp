"""
definitions for the REST api
"""

API_DESC_REQ_ID = (
    "the request id, will be replicated in the response. leave empty to autogenerate."
)
API_DESC_WS_ID = "the websocket id to communicate with the client."
API_DESC_PRIVATE = (
    "if set, only the object's owner (and administrator) can access the object."
)
API_DESC_PLUGIN = "the plugin to process the request with, must be the `filename with or without .py/.pyc`."
API_DESC_PLUGIN_PARAMETERS = """plugin parameters.

for additional parameters, refer to the plugin documentation.
"""
API_DESC_COUNT = "returns count only (limit, skip, sort are ignored if set)."
API_DESC_INDEX_TEMPLATE = (
    "an optional index template json to override the default when creating new indexes."
)
API_DESC_TOKEN = "an authentication token."
API_DESC_INDEX = "the target opensearch index or datastream, usually Gulp's index."
API_DESC_INGEST_PLUGIN = "the plugin to be used."
API_DESC_OPERATION_ID = "the id of an operation on the collab database: an operation may have multiple *contexts* and *collaboration* objects."
API_DESC_CONTEXT_ID = "id of a context on the collab database: a *context* may have multiple *sources* related to an *operation*."
API_DESC_OBJECT_ID = "id of an object of the requested type on the collab database."
API_DESC_SOURCE_ID = "id of a *source* on the collab database."
API_DESC_INGESTION_FILTER = "a filter to be used for ingestion."
API_DESC_QUERY_FILTER = (
    "a filter to be used for querying OpenSearch with common parameters."
)
API_DESC_GLYPH_ID = "id of a glyph on the collab database."
API_DESC_HEADER_SIZE = "the size of the header in bytes."
API_DESC_HEADER_CONTINUE_OFFSET = "the offset in the file to continue the upload from."

EXAMPLE_INDEX = "test_idx"
EXAMPLE_OPERATION_ID = "test_operation"
EXAMPLE_CONTEXT_ID = "test_context"
EXAMPLE_SOURCE_ID = "test_source"
EXAMPLE_WS_ID = "test_ws"
EXAMPLE_TOKEN = "token_editor"
EXAMPLE_REQ_ID = "test_req"
EXAMPLE_PLUGIN = "win_evtx"
EXAMPLE_GLYPH_ID = "glyph_id"

# 5-16 characters length, only letters, numbers, underscore, dot, dash allowed
REGEX_CHECK_USERNAME = "^([a-zA-Z0-9_.-]).{4,16}$"

# 8-64 characters length, at least one uppercase, one lowercase, one digit, one special char
REGEX_CHECK_PASSWORD = (
    r"^(?=.*[A-Z])"
    r"(?=.*[a-z])"
    r"(?=.*[0-9])"
    r"(?=.*[!@#$%^&*()_+\-])"
    r"[A-Za-z0-9!@#$%^&*()_+\-]{8,64}"
    r"$"
)

# regex for checking email
REGEX_CHECK_EMAIL = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
