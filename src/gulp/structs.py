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
        description="used for ingestion only: mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).",
    )
    mapping_id: Optional[str] = Field(
        None,
        description="used for ingestion only: the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )
    sigma_mappings: Optional[GulpSigmaMapping] = Field(
        None,
        description="if set, rules to map `lgosource` for sigma rules referring to this mapping.",
    )
    mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="""
used for ingestion only: a dictionary of one or more { mapping_id: GulpMapping } to use directly.
- `mapping_file` and `additional_mapping_files` are ignored if this is set.
""",
    )
    additional_mapping_files: Optional[list[tuple[str, str]]] = Field(
        None,
        description="""
if this is set, allows to specify further mapping files and mapping IDs with a tuple of (mapping_file, mapping_id) to load and merge additional mappings from another file.

- each mapping loaded from `additional_mapping_files` will be merged with the main `mapping file.mapping_id` fields.
- ignored if `mappings` is set.
""",
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
        description="this is used to override the websocket chunk size for the request, which is normally taken from configuration 'documents_chunk_size'.",
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
