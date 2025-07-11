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
from typing import Any, Literal, Optional

from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

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

    name: str = Field(..., description="the parameter.")
    type: Literal["bool", "str", "int", "float", "dict", "list"] = Field(
        ..., description="parameter type."
    )
    default_value: Optional[Any] = Field(None, description="default value.")
    desc: Optional[str] = Field(None, description="parameter description.")
    location: Optional[Literal["query", "header", "body"]] = Field(
        default="query", description="where the parameter is located, for API requests."
    )
    required: Optional[bool] = Field(False, description="is the parameter required ?")
    example: Optional[Any] = Field(
        None, description="an example value for the parameter, if any."
    )


class GulpAPIMethod(BaseModel):
    """
    describes a Gulp API method.
    """

    method: Literal["PUT", "GET", "POST", "DELETE", "PATCH"] = (
        Field(..., description="the method to be used"),
    )
    url: str = (Field(..., description="the endpoint url, relative to the base host"),)
    params: Optional[list[GulpAPIParameter]] = Field(
        [], description="list of parameters for the method"
    )


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
    mapping_file: Optional[str] = Field(
        None,
        description="""
mapping file name in the mapping files directory (main or extra) to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).

- `mappings` is ignored if this is set.
""",
    )
    mapping_id: Optional[str] = Field(
        None,
        description="the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )
    mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="""
used for ingestion only: a dictionary of one or more { mapping_id: GulpMapping } to use directly.

- `mapping_file` is ignored if this is set.
""",
    )
    additional_mapping_files: Optional[list[tuple[str, str]]] = Field(
        None,
        description="""
if this is set, it allows to specify further mappings from other mapping files.

each tuple is defined as (other_mapping_file, mapping_id): each `mapping_id` from `other_mapping_file` will be loaded and merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`.
""",
    )
    additional_mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="""
        same as `additional_mapping_files`, but used to pass additional mappings as a dictionary of { mapping_id: GulpMapping }.

        each `mapping_id` GulpMapping defined will be merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`.
        """,
    )
    # NOTE: should this be exposed to the query api ?
    sigma_mappings: Optional[GulpSigmaMapping] = Field(
        None,
        description="internal use only with sigma queries: if set, rules to map sigma rules `logsource`, in reference to the stored mapping for the given source.",
    )

    def is_empty(self) -> bool:
        """
        check if mapping parameters are empty.

        Returns:
            bool: True if all parameters are None, False otherwise
        """
        if (
            self.mappings is not None
            or self.mapping_file is not None
            or self.sigma_mappings is not None
            or self.additional_mapping_files is not None
            or self.additional_mappings is not None
        ):
            return False
        MutyLogger.get_instance().warning("mapping parameters are empty")
        return True


class GulpPluginParameters(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query API.

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
    mapping_parameters: Optional[GulpMappingParameters] = Field(
        GulpMappingParameters(),
        description="mapping parameters for the plugin.",
    )
    override_chunk_size: Optional[int] = Field(
        None,
        description="""this is used to override the bufferized size of chunk before flushing to OpenSearch and possibly send to websocket.

        by default, this is set as configuration 'documents_chunk_size' and can be overridden here i.e. when OpenSearch or websocket complains about too big chunks.""",
    )

    custom_parameters: Optional[dict] = Field(
        {},
        description="additional custom parameters for the plugin.",
    )


class GulpPluginCustomParameter(GulpAPIParameter):
    """
    this is used by the UI through `plugin_list` API, which calls each plugin `custom_parameters()` entrypoint to get custom parameters name/type/description/default if defined.

    to pass custom parameters to a plugin, just use the `name` field as the key in the `GulpPluginParameters.custom_parameters` dictionary:

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


class GulpNameDescriptionEntry(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "opensearch",
                    "description": "the one to use to query Gulp.",
                }
            ]
        }
    )
    name: str = Field(
        ...,
        description="name for the entry.",
    )
    description: Optional[str] = Field(
        None,
        description="a description",
    )


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"
