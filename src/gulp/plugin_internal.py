import json
from typing import Any, Optional

from pydantic import BaseModel, Field, SkipValidation, model_validator


class GulpPluginParams(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query API
    """

    class Config:
        # allow extra fields in the model
        extra = "allow"

    mapping_file: Optional[str] = Field(
        None,
        description="mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from.",
    )

    mapping_id: Optional[str] = Field(
        None,
        description="the target GulpMapping in the `mapping_file`.",
    )

    config_override: Optional[dict[str, Any]] = Field(
        {}, description="allow to override gulp configuration parameters."
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: str | dict = None):
        if not data:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpPluginOption:
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.
    """

    def __init__(self, name: str, type: str, desc: str, default: any = None):
        """
        :param name: option name
        :param type: option type (use "bool", "str", "int", "float", "dict", "list" for the types.)
        :param desc: option description
        :param default: default value
        """
        self.name = name
        self.type = type
        self.default = default
        self.desc = desc

    def to_dict(self) -> dict:
        return self.__dict__
