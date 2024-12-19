"""Gulp global definitions."""

from enum import StrEnum
from typing import Any, Literal, Optional, override

from pydantic import BaseModel, ConfigDict, Field

from gulp.api.mapping.models import GulpMapping
from muty.pydantic import (
    autogenerate_model_example_by_class,
)


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class GulpPluginParameters(BaseModel):
    """
    common parameters for a plugin, to be passed to ingest and query API.

    this may also include GulpPluginCustomParameter.name entries specific to the plugin
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "mapping_file": "mftecmd_csv.json",
                    "mappings": autogenerate_model_example_by_class(GulpMapping),
                    "mapping_id": "record",
                    "additional_mapping_files": [
                        ("mftecmd_csv.json", "record"),
                        ("mftecmd_csv.json", "file"),
                    ],
                }
            ]
        },
    )
    mapping_file: Optional[str] = Field(
        None,
        description="used for ingestion only: mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).",
    )
    additional_mapping_files: Optional[list[tuple[str, str]]] = Field(
        None,
        description="""
if this is set, allows to specify further mapping files and mapping IDs with a tuple of (mapping_file, mapping_id) to load and merge additional mappings from another file.

- each mapping loaded from `additional_mapping_files` will be merged with the main `mapping file.mapping_id` fields.
- ignored if `mappings` is set.
""",
    )
    mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="""
used for ingestion only: a dictionary of one or more { mapping_id: GulpMapping } to use directly.
- `mapping_file` and `additional_mapping_files` are ignored if this is set.
""",
    )

    mapping_id: Optional[str] = Field(
        None,
        description="used for ingestion only: the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )

    def is_empty(self) -> bool:
        """
        a mapping is empty if mappings or mapping_file or mapping_id is empty

        Returns:
            bool: True if all parameters are None, False otherwise
        """
        if self.mappings is not None or self.mapping_file is not None:
            return False
        return True


class GulpPluginCustomParameter(BaseModel):
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.

    `name` may also be a key in the `GulpPluginParameters` object, to list additional parameters specific for the plugin.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "ignore_mapping",
                    "type": "bool",
                    "default_value": False,
                    "desc": "ignore mapping file and use default mapping.",
                    "required": True,
                }
            ]
        }
    )
    name: str = Field(..., description="option name.")
    type: Literal["bool", "str", "int", "float", "dict", "list"] = Field(
        ..., description="option type."
    )
    default_value: Optional[Any] = Field(None, description="default value.")
    desc: Optional[str] = Field(None, description="option description.")
    required: Optional[bool] = Field(False, description="is the option required ?")


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


class GulpPluginSigmaSupport(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](https://github.com/SigmaHQ/sigma-cli) for parameters (backend=-t, pipeline=-p, output=-f).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "backends": [
                        {
                            "name": "opensearch",
                            "description": "OpenSearch.",
                        }
                    ],
                    "pipelines": [
                        {
                            "name": "ecs_windows",
                            "description": "ECS Mapping for windows event logs ingested with Winlogbeat or Gulp.",
                        }
                    ],
                    "output_formats": [
                        {
                            "name": "dsl_lucene",
                            "description": "DSL with embedded Lucene queries.",
                        }
                    ],
                }
            ]
        }
    )
    backends: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more pysigma backend supported by the plugin: `opensearch` is the one to use to query Gulp.",
    )
    pipelines: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more pysigma pipelines supported by the plugin.",
    )
    output_formats: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more output formats supported by the plugin.",
    )


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"
