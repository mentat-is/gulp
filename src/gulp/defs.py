"""Gulp global definitions."""

import json
from enum import IntEnum, StrEnum, auto
from typing import override

import muty.jsend
from pydantic import BaseModel, Field, model_validator


# common api descriptions for swagger
API_DESC_REQID = muty.jsend.API_DESC_REQID
API_DESC_WS_ID = "websocket id to send the response/s to."
API_DESC_PRIVATE = "if set, the object will be private (only the owner can see it)."
API_DESC_COLLAB_LEVEL = "if set, one of the GulpCollabLevel levels to indicate the importance of the collab object (DEFAULT, WARNING, ERROR)."
API_DESC_PLUGIN = (
    "`filename with or without .py/.pyc` of the plugin to process the request with."
)
API_DESC_PLUGIN_TYPE = "the plugin type (ingestion, sigma, extension, query)."
API_DESC_UPLOADFILE = "file to be uploaded."
API_DESC_COUNT = "returns count only (limit, skip, sort are ignored if set)."
API_DESC_INDEX = "the target elasticsearch index or datastream."
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
API_DESC_INGEST_OPERATION = 'the id of an operation registered with operation_create(): this will be set in ingested events as "gulp.operation.id".'
API_DESC_INGEST_CONTEXT = '(optional) string to be set in ingested events as "context".'
API_DESC_INGEST_IGNORE_ERRORS = (
    "ignore errors instead of stopping (current file) ingestion at first error."
)
API_DESC_INGESTION_PLUGIN_PARAMS = "additional parameters for the ingestion plugin."
API_DESC_PYSYGMA_PLUGIN = "fallback pysigma plugin `filename with or without .py/.py`. Defaults to None (use 'logsource.product' from sigma rule if present)."
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
API_DESC_CUSTOM_MAPPINGS_FILE = 'an optional JSON filename (i.e. "custom_mapping.json") in the "mapping_files" directory, containing specific mappings. if not present, default for the plugin will be used.'
EXAMPLE_INDEX = {"example": {"value": "testidx"}}

EXAMPLE_OPERATION_ID = {"example": {"value": 1}}
EXAMPLE_CONTEXT = {"example": {"value": "testcontext"}}
EXAMPLE_INDEX_TEMPLATE = {
    "example": {
        "value": {
            "index_patterns": ["testidx-*"],
            "settings": {"number_of_shards": 1},
            "mappings": {"properties": {"field1": {"type": "text"}}},
        }
    }
}
EXAMPLE_CLIENT_ID = {"example": {"value": 1}}

EXAMPLE_PLUGIN = {"example": {"value": "win_evtx"}}

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


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class IngestionFailed(Exception):
    pass


class GulpPluginType(StrEnum):
    """ 
    specifies the plugin types

    - INGESTION: support ingestion
    - QUERY_EXTERNAL: support query to external sources
    - EXTENSION: extension plugin
    """
    INGESTION = "ingestion"
    EXTENSION = "extension"
    QUERY_EXTERNAL = "query_external"


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


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"


class GulpAPIContext(BaseModel):
    """
    operation, client, context are used to identify the request in the collab database.
    """

    operation: str = Field(
        None,
        description="optional operation in the collab database to which the request belongs to.",
    )
    operation_id: str = Field(
        None,
        description="optional operation id in the collab database to which the request belongs to.",
    )
    client: str = Field(
        None,
        description="optional client in the collab database to identify who is making the request.",
    )
    client_id: str = Field(
        None,
        description="optional client id in the collab database to identify who is making the request.",
    )
    context: str = Field(
        None,
        description="optional `gulp.context` to which the request refer to.",
    )
    context_id: str = Field(
        None,
        description="optional `gulp.context` id to which the request refer to.",
    )

    @staticmethod
    def from_dict(d: dict) -> "GulpAPIContext":
        """
        Create a GulpAPIContext object from a dictionary.

        Args:
            d (dict): The dictionary containing the context attributes.

        Returns:
            GulpAPIContext: The created GulpAPIContext object.
        """
        return GulpAPIContext(**d)

    def to_dict(self) -> dict:
        """
        returns a dictionary representation of the context.
        """
        d = {
            "operation": self.operation,
            "operation_id": self.operation_id,
            "client_id": self.client_id,
            "client": self.client,
            "context": self.context,
            "context_id": self.context_id,
        }
        return d

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return GulpAPIContext().to_dict()

        if isinstance(data, dict):
            return data
        return json.loads(data)
