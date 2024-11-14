"""Gulp global definitions."""

from enum import StrEnum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from gulp.api.mapping.models import GulpMapping


# common api descriptions for swagger
API_DESC_REQ_ID = "request id, will be replicated in the response. leave empty to autogenerate."
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


class GulpPluginParameters(BaseModel):
    """
    common parameters for a plugin, to be passed to ingest and query API.

    this may also include GulpPluginAdditionalParameter.name entries specific to the plugin
    """

    model_config = ConfigDict(extra="allow")

    mapping_file: Optional[str] = Field(
        None,
        description="used for ingestion only: mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).",
    )

    mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="used for ingestion only: a dictionary of one or more { mapping_id: GulpMapping } to use directly (`mapping_file` is ignored if set).",
    )

    mapping_id: Optional[str] = Field(
        None,
        description="used for ingestion only: the GulpMapping to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )


class GulpPluginAdditionalParameter(BaseModel):
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.

    `name` may also be a key in the `GulpPluginParameters` object, to list additional parameters specific for the plugin.
    """
    name: str = Field(..., description="option name.")
    type: Literal['bool', 'str', 'int', 'float', 'dict', 'list'] = Field(..., description="option type.")
    default_value: Optional[Any] = Field(None, description="default value.")
    desc: Optional[str] = Field(None, description="option description.")
    required: Optional[bool] = Field(False, description="is the option required ?")


class GulpPluginSigmaSupport(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](https://github.com/SigmaHQ/sigma-cli) for parameters (backend=-t, pipeline=-p, output=-f).
    """
    backend: list[str] = Field(..., description="one or more pysigma backend supported by the plugin.")
    pipelines: list[str] = Field(..., description="one or more pysigma pipelines supported by the plugin.")
    output: list[str] = Field(..., description="one or more output formats supported by the plugin. ")




