"""Gulp global definitions."""

from typing import Any, Literal, Optional, override

from pydantic import BaseModel, ConfigDict, Field

from gulp.api.mapping.models import GulpMapping
from muty.pydantic import autogenerate_model_example


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class IngestionFailed(Exception):
    pass


class GulpPluginParameters(BaseModel):
    """
    common parameters for a plugin, to be passed to ingest and query API.

    this may also include GulpPluginAdditionalParameter.name entries specific to the plugin
    """

    model_config = ConfigDict(extra="allow")

    mapping_file: Optional[str] = Field(
        None,
        example="mftecmd_csv.json",
        description="used for ingestion only: mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).",
    )

    mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="used for ingestion only: a dictionary of one or more { mapping_id: GulpMapping } to use directly (`mapping_file` is ignored if set).",
    )

    mapping_id: Optional[str] = Field(
        None,
        description="used for ingestion only: the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
        example="record",
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)


class GulpPluginAdditionalParameter(BaseModel):
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.

    `name` may also be a key in the `GulpPluginParameters` object, to list additional parameters specific for the plugin.
    """

    name: str = Field(..., description="option name.", example="ignore_mapping")
    type: Literal["bool", "str", "int", "float", "dict", "list"] = Field(
        ..., description="option type.", example="bool"
    )
    default_value: Optional[Any] = Field(
        None, description="default value.", example=False
    )
    desc: Optional[str] = Field(
        None, description="option description.", example="test description."
    )
    required: Optional[bool] = Field(
        False, description="is the option required ?", example=True
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)


class GulpPluginSigmaSupport(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](https://github.com/SigmaHQ/sigma-cli) for parameters (backend=-t, pipeline=-p, output=-f).
    """

    backend: list[str] = Field(
        ...,
        description="one or more pysigma backend supported by the plugin.",
        example="opensearch",
    )
    pipelines: list[str] = Field(
        ...,
        description="one or more pysigma pipelines supported by the plugin.",
        example="default",
    )
    output: list[str] = Field(
        ...,
        description="one or more output formats supported by the plugin. ",
        example="dsl_lucene",
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)
