import json
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, SkipValidation, model_validator
from gulp.api.mapping.models import GulpMapping

class GulpPluginGenericParams(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query API.

    this may also include GulpPluginSpecificParam.name entries specific to the plugin
    """

    class Config:
        # allow extra fields in the model
        extra = "allow"

    mapping_file: Optional[str] = Field(
        None,
        description="mapping file name in `gulp/mapping_files` directory to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).",
    )

    opt_mappings: Optional[dict[str, GulpMapping]] = Field(
        None,
        description="a dictionary of one or more { mapping_id: GulpMapping } to use directly (`mapping_file` is ignored if set).",
    )

    opt_mapping_id: Optional[str] = Field(
        None,
        description="the GulpMapping to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: str | dict = None):
        if not data:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpPluginSpecificParam(BaseModel):
    """
    this is used by the UI through the plugin.options() method to list the supported options, and their types, for a plugin.

    name is used as the key in the `GulpPluginGenericParams` object, to list additional parameters specific for the plugin.
    """
    name: str = Field(..., description="option name.")
    type: Literal['bool', 'str', 'int', 'float', 'dict', 'list'] = Field(..., description="option type.")
    default_value: Optional[Any] = Field(None, description="default value.")
    desc: Optional[str] = Field(None, description="option description.")
    required: Optional[bool] = Field(False, description="is the option required ?")

