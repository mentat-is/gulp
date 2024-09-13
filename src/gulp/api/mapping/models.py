import json
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


class GulpMappingOptions(BaseModel):
    """
    GulpMappingOptions defines global options for a mapping (an array of FieldMappingEntry).
    """

    agent_type: Optional[str] = Field(
        None,
        description='if set, forces events fot this mapping to have "agent.type" set to this value.',
    )

    default_event_code: Optional[str] = Field(
        None,
        description='if set, this is the "event_code" to be set for this mapping: this may be overridden by "event_code" in FieldMappingEntry when "is_timestamp": true forces generation of a new event with such "@timestamp" and, possibly, "event.code".',
    )
    ignore_unmapped: Optional[bool] = Field(
        False,
        description='if True, ignore(=do not set in the output document) unmapped fields. Default is False, if a field does not have a corresponding mapping entry it will be mapped as "gulp.unmapped.fieldname").',
    )
    mapping_id: Optional[str] = Field(
        None,
        description="mapping identifier, i.e. to select this mapping via GulpPluginParams.",
    )
    ignore_blanks: Optional[bool] = Field(
        True,
        description="if True, remove blank (string) fields from the event (default: True).",
    )
    timestamp_yearfirst: Optional[bool] = Field(
        True,
        description="to convert timestamp string, indicating if the year comes first in the timestamp string (default: True).",
    )
    timestamp_dayfirst: Optional[bool] = Field(
        False,
        description="to convert timestamp string, indicating if the day comes first in the timestamp string (default: False).",
    )
    timestamp_utc: Optional[bool] = Field(
        True,
        description="to convert timestamp string, indicating if the converted timestamp should be UTC (default: True).",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "agent_type": "my_agent_type",
                "default_event_code": "my_event_code",
                "mapping_id": "my_mapping_id",
                "purge_blanks": True,
                "timestamp_yearfirst": True,
                "timestamp_dayfirst": False,
                "timestamp_utc": True,
            }
        }
    }

    def to_dict(self) -> dict:
        d = {
            "agent_type": self.agent_type,
            "mapping_id": self.mapping_id,
            "default_event_code": self.default_event_code,
            "purge_blanks": self.ignore_blanks,
            "ignore_unmapped": self.ignore_unmapped,
            "timestamp_yearfirst": self.timestamp_yearfirst,
            "timestamp_dayfirst": self.timestamp_dayfirst,
            "timestamp_utc": self.timestamp_utc
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "GulpMappingOptions":
        return GulpMappingOptions(
            agent_type=d.get("agent_type", None),
            default_event_code=d.get("default_event_code", None),
            mapping_id=d.get("mapping_id", None),
            ignore_blanks=d.get("purge_blanks", True),
            ignore_unmapped=d.get("ignore_unmapped", False),
            timestamp_yearfirst=d.get("timestamp_yearfirst", True),
            timestamp_dayfirst=d.get("timestamp_dayfirst", False),
            timestamp_utc=d.get("timestamp_utc", True),
        )


class FieldMappingEntry(BaseModel):
    """
    FieldMappingEntry defines how to map a single field, including field-specific options.
    """

    map_to: Optional[str | list[str] | list[list[str]]] = Field(
        None,
        description="""str for single mapping (field: mapped_field),<br>
         list[str] for multiple mapping (field: [mapped_field1, mapped_field2],<br>
         list[list[str]] for variable mapping.
         <br><br>
         a variable mapping is defined as a list of arrays, where each array is a condition to match.
         i.e. field: [["service", "security", "user.name"]] means
         "field" must be mapped as "user.name" when logsource field="service" and its name="security".
         <br><br>
         map_to may also not be set: in such case, the field is mapped as gulp.unmapped.fieldname.
        """,
    )
    is_timestamp: Optional[bool] = Field(
        False,
        description='if True, this field refer to a timestamp and will generate "@timestamp" and "@timestamp_nsec" fields.<br>'
        'NOTE: if more than one "is_timestamp" is set in the mapping, multiple events with such "@timestamp" and, possibly, "event.code" are generated.',
    )
    is_timestamp_chrome: Optional[bool] = Field(
        False,
        description='if set and if value is a number, or string representing a number, it is interpreted as a chrome/webkit timestamp (with epoch 01/01/1601) and converted to **nanoseconds from unix epoch**.<br>'
            'NOTE: ignored if "do_multiply" is set, if set together with "is_timestamp" will generate converted chrome "@timestamp" and "@timestamp_nsec".'
    )
    do_multiply: Optional[float] = Field(
        None,
        description='if set and if value is a number, or string representing a number, multiply it by this value (to divide, i.e. multiply by 0.5 to divide by 2).<br>'
            'NOTE: ignored if "is_timestamp_chrome" is set, result of the multiply/divide must be **nanoseconds**.'
    )
    event_code: Optional[str] = Field(
        None,
        description='if set, overrides "default_event_code" in the GulpMappingOptions (ignored if "is_timestamp" is not set).'
    )
    is_variable_mapping: Optional[bool] = Field(
        False,
        description='INTERNAL USAGE ONLY. if True, indicates the "map_to" field is a variable mapping.',
    )
    result: Optional[dict[str, Any]] = Field(
        None, description='INTERNAL USAGE ONLY. the destination dict representing the field (i.e. {"@timestamp": 123456}).'
    )

    def to_dict(self) -> dict:
        d = {
            "map_to": self.map_to,
            "is_timestamp": self.is_timestamp,
            "event_code": self.event_code,
            "do_multiply": self.do_multiply,
            "is_timestamp_chrome": self.is_timestamp_chrome,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "FieldMappingEntry":
        # print('FieldMappingEntry.from_dict: d=%s, type=%s' % (d, type(d)))
        fm = FieldMappingEntry(
            map_to=d.get("map_to", None),
            event_code=d.get("event_code", None),
            do_multiply=d.get("do_multiply", None),
            is_variable_mapping=d.get("is_variable_mapping", False),
            is_timestamp=d.get("is_timestamp", False),
            is_timestamp_chrome=d.get("is_timestamp_chrome", False)
        )
        # print('FieldMappingEntry.from_dict: fm=%s, type=%s' % (fm, type(fm)))
        return fm

    model_config = {
        "json_schema_extra": {
            "example": {
                "map_to": "my_field",
                "event_code": "my_event_code",
                "is_timestamp": False,
            }
        }
    }


class GulpMapping(BaseModel):
    """
    defines a source->elasticsearch document mapping for a Gulp plugin.
    """

    fields: dict[str, FieldMappingEntry] = Field(
        {},
        description='a dictionary where each key is a source field to be mapped to "map_to" defined in "FieldMappingEntry".',
    )
    options: Optional[GulpMappingOptions] = Field(
        None,
        description="specific options for this GulpMapping. if None, defaults are applied.",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "fields": {
                    "source_field1_single": {
                        "map_to": "my_field"
                    },
                    "source_field2_multiple": {
                        "map_to": ["my_field1", "myfield2"]
                    },
                    "source_field3_variable": {
                        "map_to": [
                            ["category", "process_creation", "process.pe.company"],
                            ["category", "image_load", "file.pe.company"],
                        ]
                    },
                },
                "options": {
                    "agent_type": "my_agent_type",
                    "event_code": "my_event_code",
                    "mapping_id": "my_mapping_id",
                    "ignore_unmapped": False,
                    "timestamp_yearfirst": True,
                    "timestamp_dayfirst": False,
                    "timestamp_tz": "UTC",
                    "extra": {"my_custom_option": "my_custom_value"},
                },
            }
        }
    }

    def to_dict(self) -> dict:
        """
        NOTE: options is guaranteed to be not None (default values are applied if None)
        """
        d = {
            "fields": {k: v.to_dict() for k, v in self.fields.items()},
            "options": (
                GulpMappingOptions.to_dict(self.options)
                if self.options is not None
                else GulpMappingOptions().to_dict()
            ),
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "GulpMapping":
        m = d.get("fields", {})
        if len(m) > 0:
            fields = {k: FieldMappingEntry.from_dict(v) for k, v in m.items()}
        else:
            fields = {}
        opts = d.get("options", None)
        if opts is not None:
            options = GulpMappingOptions.from_dict(opts)
        else:
            # default values
            options = GulpMappingOptions()

        return GulpMapping(
            fields=fields,
            options=options,
        )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None:
            return {}

        if isinstance(data, dict):
            return data

        return json.loads(data)
