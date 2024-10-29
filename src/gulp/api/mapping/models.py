import json
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator

"""
mapping files structure:
{
    mapping_id_1: {
        GulpMapping: {
            "fields": {
                field_1: {
                    GulpMappingField
                },
                field_2: {
                    GulpMappingField
                },
                ...        
            }
        }
    },
    mapping_id_2: {
        GulpMapping: {
            "fields": {
                field_1: {
                    GulpMappingField
                },
                field_2: {
                    GulpMappingField
                },
                ...        
            }
        }
    },
    ...
}
"""


class GulpMappingField(BaseModel):
    """
    defines how to map a single field, including field-specific options.
    """

    class Config:
        extra = "allow"

    ecs: str = Field(
        description="the ECS field name.",
    )
    opt_is_timestamp_chrome: Optional[bool] = Field(
        False,
        description="if set, the corresponding value is a webkit timestamp (from 1601) and will be converted to nanoseconds from unix epoch.",
    )


class GulpMapping(BaseModel):
    """
    defines a logsource -> gulp document mapping
    """

    class Config:
        extra = "allow"

    fields: dict[str, GulpMappingField] = Field(
        description="field mappings { raw_field: { GulpMappingField } } to translate a logsource to gulp document.",
    )
    description: Optional[str] = Field(
        None,
        description="The description of the mapping.",
    )
    opt_agent_type: Optional[str] = Field(
        None,
        description='if set, forces documents to have "agent.type" set to this value.',
    )

    opt_event_code: Optional[str] = Field(
        None,
        description='if set, forces documents to have "event.code" set to this value (and "gulp.event.code" to the corresponding numeric value).',
    )
