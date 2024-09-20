"""Gulp global definitions."""

from enum import IntEnum, StrEnum

import muty.jsend

# common api descriptions for swagger
API_DESC_REQID = muty.jsend.API_DESC_REQID
API_DESC_WS_ID = "websocket id to send the response/s to."
API_DESC_PRIVATE = "if set, the object will be private (only the owner can see it)."
API_DESC_COLLAB_LEVEL = "if set, one of the GulpCollabLevel levels to indicate the importance of the collab object (DEFAULT, WARNING, ERROR)."
API_DESC_PLUGIN = "name of the plugin to process the request with."
API_DESC_UPLOADFILE = "file to be uploaded."
API_DESC_COUNT = "returns count only (limit, skip, sort are ignored if set)."
API_DESC_INDEX = "the target elasticsearch index or datastream."
API_DESC_TOKEN = "authentication token."
API_DESC_ADMIN_TOKEN = "an authentication token with ADMIN permission."
API_DESC_DELETE_EDIT_TOKEN = "an authentication token with EDIT permission if owner of the object, or DELETE (or ADMIN) permission."
API_DESC_DELETE_TOKEN = "an authentication token with DELETE (or ADMIN) permission."
API_DESC_EDIT_TOKEN = "an authentication token with EDIT permission."
API_DESC_INGEST_TOKEN = "an authentication token with INGEST (or ADMIN) permission."
API_DESC_INGEST_PLUGIN = "plugin to be used for ingestion."
API_DESC_INGEST_OPERATION = 'the id of an operation registered with operation_create(): this will be set in ingested events as "operation_id".'
API_DESC_INGEST_CONTEXT = '(optional) string to be set in ingested events as "context".'
API_DESC_INGEST_IGNORE_ERRORS = (
    "ignore errors instead of stopping (current file) ingestion at first error."
)
API_DESC_INGESTION_PLUGIN_PARAMS = "additional parameters for the ingestion plugin."
API_DESC_SIGMA_PLUGIN_PARAMS = "additional parameters for the sigma plugin."
API_DESC_CLIENT = "the id of a client registered via client_create() on the collab DB."
API_DESC_OPERATION = (
    "the id of an operation registered via operation_create() on the collab DB."
)
API_DESC_GLYPH = "the id of a glyph registered via glyph_create() on the collab DB."
API_DESC_SYNC = 'if set, the request will be processed synchronously (but concurrently in the event loop) and a *GulpStats* dictionary is returned. either, this API returns "pending" and the result will be available via the *stats_list* API using the same *req_id* (or via the */ws_stats* websocket).'
API_DESC_WORKFLOW_ID = (
    "optional id of a workflow (in the shared-data table) to assign to the operation."
)
API_DESC_ALL_TAGS_MUST_MATCH = 'If True, stored rules are selected only if ALL tags (i.e. ["windows", "process"]) matches. Either, just one tag match is enough for the query to be selected. Defaults to False (just one tag is enough).'
API_DESC_CUSTOM_MAPPINGS_FILE = 'an optional JSON filename (i.e. "custom_mapping.json") in the "mapping_files" directory, containing specific mappings. if not present, default for the plugin will be used.'
EXAMPLE_INDEX = {"example": {"value": "testidx"}}

EXAMPLE_OPERATION_ID = {"example": {"value": 1}}
EXAMPLE_CONTEXT = {"example": {"value": "testcontext"}}

EXAMPLE_CLIENT_ID = {"example": {"value": 1}}

EXAMPLE_PLUGIN = {"example": {"value": "gi_win_evtx"}}

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


class InvalidArgument(Exception):
    pass


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class IngestionFailed(Exception):
    pass


class GulpPluginType(StrEnum):
    """plugin types"""

    INGESTION = "ingestion"
    SIGMA = "sigma"
    EXTENSION = "extension"

class GulpLogLevel(IntEnum):
    """Gulp log levels status codes"""

    UNEXPECTED = 16
    CUSTOM_10 = 15
    CUSTOM_9 = 14
    CUSTOM_8 = 13
    CUSTOM_7 = 12
    CUSTOM_6 = 11
    CUSTOM_5 = 10
    CUSTOM_4 = 9
    CUSTOM_3 = 8
    CUSTOM_2 = 7
    CUSTOM_1 = 6
    ALWAYS = 5
    VERBOSE = 4
    CRITICAL = 3
    ERROR = 2
    WARNING = 1
    INFO = 0


class GulpEventFilterResult(IntEnum):
    """wether if the event should be accepted or skipped during ingestion."""

    ACCEPT = 0
    SKIP = 1


class SortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASCENDING = "asc"
    DESCENDING = "desc"
