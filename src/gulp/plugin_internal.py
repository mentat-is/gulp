import json
from typing import Any, Optional

from pydantic import BaseModel, Field, SkipValidation, model_validator


class GulpPluginParams(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query API
    """

    mapping_file: Optional[str] = Field(
        None,
        description='mapping file name (in gulp/mapping_files directory) to read "mappings" array from, if any.',
    )

    mapping_id: Optional[str] = Field(
        None,
        description="mapping identifier, i.e. to select this mapping via GulpMappingOptions.",
    )

    config_override: Optional[dict[str, Any]] = Field(
        {}, description="allow to override gulp configuration parameters."
    )
    ignore_mapping_ingest: Optional[bool] = Field(
        False,
        description="ignore mapping when ingesting (to be compatible with OpenSearch Security Analytics).",
    )
    ignore_mapping_sigma_query: Optional[bool] = Field(
        False,
        description="ignore mapping when querying using Sigma rules.",
    )
    timestamp_field: Optional[str] = Field(
        None,
        description="The timestamp field (for, i.e. to use in a generic plugin without any mapping)",
    )
    record_to_gulp_document_fun: SkipValidation[Any] = Field(
        [],
        description="INTERNAL USAGE ONLY, to get mapping from (for stacked plugins).",
    )
    pipeline: SkipValidation[Any] = Field(
        None,
        description="INTERNAL USAGE ONLY, the sigma ProcessingPipeline to get mapping from.",
    )
    extra: Optional[dict[str, Any]] = Field(
        {},
        description="any extra custom options, i.e. the ones listed in plugin.options().",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "mapping_file": "my_mapping.json",
                "mapping_id": "my_mapping_id",
                "config_override": {"parallel_processes_respawn_after_tasks": 500},
                "extra": {"my_custom_option": "my_custom_value"},
            }
        }
    }

    def to_dict(self) -> dict:
        d = {
            "mapping_file": self.mapping_file,
            "mapping_id": self.mapping_id,
            "config_override": self.config_override,
            "ignore_mapping_ingest": self.ignore_mapping_ingest,
            "ignore_mapping_sigma_query": self.ignore_mapping_sigma_query,
            "extra": self.extra,
            "timestamp_field": self.timestamp_field,
            "record_to_gulp_document_fun": self.record_to_gulp_document_fun,
            "pipeline": self.pipeline,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "GulpPluginParams":
        return GulpPluginParams(
            mapping_file=d.get("mapping_file", None),
            mapping_id=d.get("mapping_id", None),
            timestamp_field=d.get("timestamp_field", None),
            ignore_mapping_ingest=d.get("ignore_mapping_ingest", False),
            ignore_mapping_sigma_query=d.get("ignore_mapping_sigma_query", False),
            config_override=d.get("config_override", {}),
            extra=d.get("extra", {}),
            record_to_gulp_document_fun=d.get("record_to_gulp_document_fun", []),
            pipeline=d.get("pipeline", None),
        )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpPluginOption:
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.
    """

    def __init__(self, name: str, t: str, desc: str, default: any = None):
        """
        :param name: option name
        :param t: option type (use "bool", "str", "int", "float", "dict", "list" for the types.)
        :param desc: option description
        :param default: default value
        """
        self.name = name
        self.t = t
        self.default = default
        self.desc = desc

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.t,
            "default": self.default,
            "desc": self.desc,
        }
