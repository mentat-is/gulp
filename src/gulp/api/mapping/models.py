"""
Mapping models for Gulp, which facilitates transforming log sources into standardized documents.

This module defines the data structures used to map source data fields to ECS (Elastic Common Schema) fields.
The mapping system supports transformations, type conversion, and the creation of multiple output documents
from a single input event.

Key components:
- GulpMappingField: Configuration for individual field mappings with transformation options
- GulpMapping: Full mapping definition for translating a log source to Gulp documents
- GulpMappingFileMetadata: Metadata for mapping files, including plugin associations
- GulpMappingFile: Container for multiple mappings that can be loaded from JSON
"""

from typing import Literal, Optional

from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field


class GulpMappingField(BaseModel):
    """
    defines how to map a single field, including field-specific options.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "ecs": ["test.mapped"],
                    "extra_doc_with_event_code": "1234",
                    "is_timestamp": "chrome",
                }
            ]
        },
    )

    flatten_json: Optional[bool] = Field(
        False,
        description="""
if set, the corresponding value is a JSON object (or string) and will be flattened into the document (i.e. all nested keys are moved to the top level).""",
    )
    force_type: Optional[Literal["str", "int", "float"]] = Field(
        None,
        description="if set, the corresponding value is forced to this type before ingestion.",
    )
    multiplier: Optional[float] = Field(
        None,
        description="if set and > 1, the corresponding value is multiplied by this value.",
    )
    is_timestamp: Optional[Literal["chrome", "generic"]] = Field(
        None,
        description="""if set, the corresponding value is a timestamp in \"chrome\" (webkit) or generic (time string supported by python dateutil.parser, numeric-from-epoch) format and it will be translated to nanoseconds from the unix epoch.

note that value mapped as "@timestamp" with a mapping are automatically supported for "generic" timestamps, and do not need this field set: they do need it instead (set to "chrome") if the value mapped as "@timestamp" is from chrome.
""",
    )
    is_gulp_type: Optional[
        Literal["context_id", "context_name", "source_id", "source_name"]
    ] = Field(
        None,
        description="""
if set, the corresponding value is a either a GulpContext.id, GulpContext.name, GulpSource.id or GulpSource.name, and will be treated accordingly:
    - context_id: the value is the id of an existing GulpContext, and it will be set as-is as `gulp.context_id` in the resulting document.
    - context_name: the value is the name of a GulpContext, which is created (if not existent) or retrieved (if existent) and its `id` set as `gulp.context_id` in the resulting document.
    - source_id: the value is the id of an existing GulpSource, and it will be set as-is as `gulp.source_id` in the resulting document.
    - source_name: the value is the name of a GulpSource, which is created (if not existent) or retrieved (if existent) and its `id` set as `gulp.source_id` in the resulting document.
""",
    )
    extra_doc_with_event_code: Optional[str] = Field(
        None,
        description="""
if this is set, this field `represent a further timestamp` in addition to the main document `@timestamp`.

`ecs` is ignored and the creation of an extra document is triggered with the given `event.code` and `@timestamp` set to this field value.

in this setting, the mapping file should:

- map a **single** field directly as `@timestamp` (to indicate the *main* document)
- set `mapping.event_code` to the *main* event code
- add additional `extra_doc_with_event_code` fields to create further documents with their own event code.
- if this value needs chrome timestamp translation, "is_timestamp": "chrome" should be set also for the field.

check `mftecmd_csv.json` for an example of this setting.
""",
    )
    ecs: Optional[list[str] | str] = Field(
        None,
        description="one or more ECS field names to map the source field to in the resulting document.",
        min_length=1,
    )


class GulpMapping(BaseModel):
    """
    defines a logsource -> gulp document mapping
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "fields": {"field1": {"ecs": ["test.mapped"]}},
                    "description": "test description.",
                    "agent_type": "win_evtx",
                    "event_code": "1234",
                }
            ]
        },
    )

    fields: Optional[dict[str, GulpMappingField]] = Field(
        {},
        description="field mappings { raw_field: { GulpMappingField } } to translate a logsource to gulp document.",
    )
    description: Optional[str] = Field(
        None,
        description="if set, mapping's description.",
    )
    agent_type: Optional[str] = Field(
        None,
        description='if set, all documents generated by this mapping have "agent.type" set to this value. either, the plugin is responsible for setting this.',
    )

    event_code: Optional[str] = Field(
        None,
        description='if set, all documents generated by this mapping have "event.code" set to this value (and "gulp.event_code" to the corresponding numeric value). either, the plugin is responsible for setting this.',
    )
    exclude: Optional[list[str]] = Field(
        None,
        description="if set, these fields are ignored and not included in the generated document/s.",
    )
    include: Optional[list[str]] = Field(
        None,
        description="if set, only these fields are processed and included in the generated document/s.",
    )
    default_context: Optional[str] = Field(
        None,
        description="""if set, this is the default context name to use when we are unable to get the context from the document (a default GulpContext will be created if not exists).""",
    )
    default_source: Optional[str] = Field(
        None,
        description="""if set, this is the default source name to use when we are unable to get the source from the document (a default GulpSource will be created if not exists).""",
    )


class GulpMappingFileMetadata(BaseModel):
    """
    metadata for a mapping file.
    """

    model_config = ConfigDict(
        extra="allow", json_schema_extra={"examples": [{"plugin": ["win_evtx", "csv"]}]}
    )

    plugin: list[str] = Field(
        ...,
        description="one or more plugin names that this mapping file is associated with.",
    )


class GulpSigmaMapping(BaseModel):
    """
    maps sigma rule logsource attributes to document fields for targeted querying.

    used during sigma rule conversion to:

    1. filter applicable rules by `logsource.service`
    2. restrict queries to documents matching specific field values

    example:

    if the sigma rule is as follows:
    
    logsource:
        service: windefend

    and the GulpSigmaMapping is as follows:

    "windefend": {
      "service_field": "winlog.channel",
        "service_values": [
            "Microsoft-Windows-Windows Defender"
        ]
    }

    then the rule is applied only if the document has `winlog.channel`: "Microsoft-Windows-Windows Defender" and the query is restricted to documents matching this condition.    
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "service_field": "winlog.channel",
                    "service_values": ["Microsoft-Windows-Windows Defender"],
                }
            ]
        },
    )
    service_field: str = Field(
        ...,
        description="document field to match against when applying sigma rule",
        examples=["winlog.channel"],
    )
    service_values: list[str] = Field(
        ...,
        description="one or more values for document[service_field] to match against when applying sigma rule (OR match)",
        examples=[["Microsoft-Windows-Windows Defender"]],
    )


class GulpMappingFile(BaseModel):
    """
    a mapping file, containing one or more GulpMapping objects.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "mappings": autogenerate_model_example_by_class(GulpMapping),
                    "metadata": autogenerate_model_example_by_class(
                        GulpMappingFileMetadata
                    ),
                }
            ]
        },
    )

    mappings: dict[str, GulpMapping] = Field(
        ...,
        description="defined mappings for this mapping file, key is the `mapping_id`",
        min_length=1,
    )
    sigma_mappings: Optional[dict[str, GulpSigmaMapping]] = Field(
        None,
        description="""
internal use only with sigma queries: if set, rules to map `logsource` for sigma rules when using this mapping file.
         
each key corresponds to `logsource.service` in the sigma rule: basically, we want to use the sigma rule only if a (mapped) "logsource.service" is defined in the sigma rule (or no `logsource` is defined at all in the sigma rule).
        """,
    )
    metadata: Optional[GulpMappingFileMetadata] = Field(
        ...,
        description="metadata for the mapping file.",
    )
