"""Gulp global definitions."""

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


class GulpPluginGenericExternalParameters(BaseModel):
    """
    generic parameters for an external service.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "uri": "http://localhost:9200",
                    "username": "admin",
                    "password": "admin",
                    "params": {"verify": False},
                }
            ]
        }
    )
    uri: str = Field(..., description="The URI to connect to the external service.")
    username: Optional[str] = Field(
        None, description="The username to connect to the external service."
    )
    password: Optional[str] = Field(
        None, description="The password to connect to the external service."
    )
    params: Optional[dict] = Field(None, description="Additional parameters.")


class GulpPluginParameters(BaseModel):
    """
    common parameters for a plugin, to be passed to ingest and query API.

    this may also include GulpPluginAdditionalParameter.name entries specific to the plugin
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "mapping_file": "mftecmd_csv.json",
                    "mappings": autogenerate_model_example_by_class(GulpMapping),
                    "mapping_id": "record",
                    "generic_external_parameters": autogenerate_model_example_by_class(
                        GulpPluginGenericExternalParameters
                    ),
                }
            ]
        },
    )
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
        description="used for ingestion only: the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )

    generic_external_parameters = Optional[GulpPluginGenericExternalParameters] = Field(
        None,
        description="used for external plugins only: generic parameters for an external service.",
    )

    def is_empty(self) -> bool:
        """
        check if all parameters are None

        Returns:
            bool: True if all parameters are None, False otherwise
        """
        return all(v is None for v in self.model_dump().values())


class GulpPluginAdditionalParameter(BaseModel):
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


class GulpPluginSigmaSupport(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](https://github.com/SigmaHQ/sigma-cli) for parameters (backend=-t, pipeline=-p, output=-f).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "backend": ["opensearch"],
                    "pipelines": ["ecs_windows", "ecs_windows_old"],
                    "output": ["dsl_lucene"],
                }
            ]
        }
    )
    backend: list[str] = Field(
        ...,
        description="one or more pysigma backend supported by the plugin: `opensearch` is the one to use to query Gulp.",
    )
    pipelines: list[str] = Field(
        ...,
        description="one or more pysigma pipelines supported by the plugin.",
    )
    output: list[str] = Field(
        ...,
        description="one or more output formats supported by the plugin.",
    )
