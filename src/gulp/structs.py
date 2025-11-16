"""Gulp global definitions.

This module contains core data structures and exceptions used throughout Gulp.
It defines models for API parameters, plugin configuration, and common utility classes.

Key components:
- Exception classes for object management
- API parameter and method models
- Plugin parameter handling
- Sorting and configuration enums
"""

from enum import StrEnum
from typing import Any, Literal, Optional, Annotated, Protocol

from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.mapping.models import GulpMapping, GulpSigmaMapping


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class GulpAPIParameter(BaseModel):
    """
    describes a parameter for a Gulp API method.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "ignore_mapping",
                    "type": "bool",
                    "default_value": False,
                    "desc": "ignore mapping file and leave the field as is.",
                    "required": True,
                }
            ]
        }
    )

    name: Annotated[str, Field(description="the parameter.")]
    type: Annotated[
        Literal["bool", "str", "int", "float", "dict", "list"],
        Field(description="parameter type."),
    ]
    default_value: Annotated[Optional[Any], Field(description="default value.")] = None
    desc: Annotated[Optional[str], Field(description="parameter description.")] = None
    location: Annotated[
        Optional[Literal["query", "header", "body"]],
        Field(description="where the parameter is located, for API requests."),
    ] = "query"
    required: Annotated[
        bool, Field(False, description="is the parameter required ?")
    ] = False
    example: Annotated[
        Optional[Any], Field(description="an example value for the parameter, if any.")
    ] = None


class GulpAPIMethod(BaseModel):
    """
    describes a Gulp API method.
    """

    method: Annotated[
        Literal["PUT", "GET", "POST", "DELETE", "PATCH"],
        Field(description="the method to be used"),
    ]
    url: Annotated[
        str, Field(..., description="the endpoint url, relative to the base host")
    ]
    params: Annotated[
        list[GulpAPIParameter],
        Field(description="list of parameters for the method"),
    ] = Field(default_factory=list)
    


class GulpMappingParameters(BaseModel):
    """
    describes mapping parameters for API methods.
    - `mapping_file` and `additional_mapping_files` are used to load mappings from files.
    - `mappings` is used to pass a dictionary of mappings directly.
    - `mapping_id` is used to select a specific mapping from the file or dictionary.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mapping_file": "mftecmd_csv.json",
                    "mappings": {
                        "the_mapping_id": autogenerate_model_example_by_class(
                            GulpMapping
                        ),
                    },
                    "mapping_id": "record",
                }
            ]
        }
    )

    # mapping file name in the mapping files directory (main or extra) to read GulpMapping entries from
    mapping_file: Annotated[
        Optional[str],
        Field(
            description=(
                "mapping file name in the mapping files directory (main or extra) to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).\n"
                "- `mappings` is ignored if this is set.\n"
                "- `additional_mapping_files` and `additional_mappings` can be used to load further mappings from other files or directly from a dictionary."
            ),
        ),
    ] = None

    # the GulpMapping to select in mapping_file or mappings object
    mapping_id: Annotated[
        Optional[str],
        Field(
            description="the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
        ),
    ] = None

    # a dictionary of one or more { mapping_id: GulpMapping } to use directly
    mappings: Annotated[
        dict[str, GulpMapping],
        Field(
            description=(
                "a dictionary of one or more { mapping_id: GulpMapping } to use directly.\n"
                "- `mapping_file`, `additional_mapping_files`, `additional_mappings` are ignored if this is set."
            ),
        ),
    ] = Field(default_factory=dict)

    # specify further mappings from other mapping files
    additional_mapping_files: Annotated[
        list[tuple[str, str]],
        Field(
            description=(
                "if this is set, it allows to specify further mappings from other mapping files.\n"
                "each tuple is defined as (other_mapping_file, mapping_id): each `mapping_id` from `other_mapping_file` will be loaded and merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`."
            ),
        ),
    ] = Field(default_factory=list)

    # pass additional mappings as a dictionary
    additional_mappings: Annotated[
        dict[str, GulpMapping],
        Field(
            description=(
                "same as `additional_mapping_files`, but used to pass additional mappings as a dictionary of { mapping_id: GulpMapping }.\n"
                "each `mapping_id` GulpMapping defined will be merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`."
            ),
        ),
    ] = Field(default_factory=dict)

    # internal use, only for sigma queries
    sigma_mappings: Annotated[
        dict[str, GulpSigmaMapping],
        Field(
            description=(
                "internal use, only for sigma queries: if set, rules to map `logsource` in sigma rules when using the mapping previously stored for each GulpSource.\n"
                'each key corresponds to `logsource.service` in the sigma rule: basically, we want to use the sigma rule only if a (mapped) "logsource.service" is defined in the sigma rule (or no `logsource` is defined at all in the sigma rule).'
            ),
        ),
    ] = Field(default_factory=dict)

    def is_empty(self) -> bool:
        """
        check if mapping parameters are empty.

        Returns:
            bool: True if all parameters are None, False otherwise
        """
        if (
            not self.mappings
            or not self.mapping_file
            or not self.sigma_mappings
            or not self.additional_mapping_files
            or not self.additional_mappings
        ):
            return False

        MutyLogger.get_instance().warning("mapping parameters are empty")
        return True

    def _stringify(self) -> tuple:
        return (
            str(self.mapping_file)
            + str(self.mapping_id)
            + str(self.mappings)
            + str(self.additional_mapping_files)
            + str(self.additional_mappings)
            + str(self.sigma_mappings)
        )

    def __eq__(self, other):
        if not isinstance(other, GulpMappingParameters):
            return NotImplemented
        return self._stringify() == other._stringify()

    def __hash__(self):
        return hash(self._stringify())


class GulpPluginParameters(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query_external API.

    additional custom parameters defined in GulpPlugin.custom_parameters may be added to the "model_extra" field, they will be passed to the plugin as is.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "mapping_parameters": autogenerate_model_example_by_class(
                        GulpMappingParameters
                    ),
                    "override_chunk_size": 1000,
                    "custom_parameters": {
                        "some_custom_param": "some_value",
                        "some_custom_param_2": "some_value_2",
                    },
                }
            ]
        },
    )
    mapping_parameters: Annotated[
        Optional[GulpMappingParameters],
        Field(
            description="mapping parameters for the plugin.",
        ),
    ] = Field(default_factory=GulpMappingParameters)
    override_chunk_size: Annotated[
        Optional[int],
        Field(
            description="""this is used to override the bufferized size of chunk before flushing to OpenSearch and possibly send to websocket.

        by default, this is set as configuration 'documents_chunk_size' and can be overridden here i.e. when OpenSearch or websocket complains about too big chunks.""",
        ),
    ] = None
    timestamp_offset_msec: Annotated[
        int,
        Field(
            description="if not 0, this is used to offset document `@timestamp` (and `gulp.timestamp`) by the given number of milliseconds (positive or negative).",
        ),
    ] = 0
    custom_parameters: Annotated[
        dict,
        Field(
            description="additional plugin-specific custom parameters.",
        ),
    ] = Field(default_factory=dict)
    preview_mode: Annotated[
        bool,
        Field(
            description="if True, the plugin should run in preview mode (return synchronously a chunk of data)"
        ),
    ] = False

    def is_empty(self) -> bool:
        """
        check if **ALL** plugin parameters are empty.

        Returns:
            bool: True if all parameters are None/empty, False otherwise
        """
        if (
            self.mapping_parameters.is_empty()
            and not self.custom_parameters
            and not self.override_chunk_size
            and not self.timestamp_offset_msec
        ):
            return True
        return False


class GulpPluginCustomParameter(GulpAPIParameter):
    """
    this is used **by the UI only** through the `plugin_list` API, which calls each plugin `custom_parameters()` entrypoint to get custom parameters name/type/description/default if defined.

    to pass custom parameters to a plugin via GulpPluginParameters, just use the `name` field as the key in the `GulpPluginParameters.custom_parameters` dictionary:

    ~~~js
    {
        // example GulpPluginParameters
        "mapping_file": "mftecmd_csv.json",
        "mappings": { "record": { "fields": { "timestamp": { "type": "datetime" } } } },
        "mapping_id": "record",
        "custom_parameters": {
            "some_custom_param": "some_value",
            "some_custom_param_2": "some_value_2"
        }
    }
    ~~~

    after `_initialize()` is called, the custom parameters will be available in `self._plugin_params.custom_parameters` field.

    ~~~python
    v = self._plugin_params.custom_parameters["some_custom_param"]
    ~~~
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "name": "ignore_mapping",
                    "type": "bool",
                    "default_value": False,
                    "desc": "ignore mapping file and leave the field as is.",
                    "required": True,
                }
            ]
        },
    )


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"


class GulpProgressCallback(Protocol):
    """
    callback protocol for generic progress updates
    """

    async def __call__(
        self,
        sess: AsyncSession,
        total: int,
        current: int,
        req_id: str,
        last: bool = False,
        **kwargs,
    ) -> None:
        """
        callback function to report progress.

        Args:
            sess (AsyncSession): the current database session
            total (int): total number of items to process
            current (int): current number of processed items
            last (bool, optional): True if this is the last progress update. Defaults to False.
            req_id (str): originating request id
            **kwargs: additional arguments passed to the callback

        Returns:
            None
        """
        ...


class GulpDocumentsChunkCallback(Protocol):
    """
    callback protocol for chunk processing
    """

    async def __call__(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        index: str = None,
        last: bool = False,
        req_id: str = None,
        q_name: str = None,
        q_group: str = None,
        **kwargs,
    ) -> list[dict]:
        """
        callback function to process a chunk of documents.

        Args:
            sess (AsyncSession): the current database session
            chunk (list[dict]): one or more GulpDocument dictionaries
            chunk_num (int): current chunk number (starting from 0)
            total_hits (int): total number of hits for the query
            index (str|None): the index (may be different from operation_id), if any. Defaults to None.
            last (bool): True if this is the last chunk. Defaults to False.
            req_id (str|None): the originating request id, if any. Defaults to None.
            q_name (str|None): query name, if any. Defaults to None.
            q_group (str|None): query group, if any. Defaults to None.
            **kwargs: additional arguments passed to the callback
        Returns:
            list[dict]: the processed chunk of documents
        """
        ...
