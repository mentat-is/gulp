"""Gulp OpenSearch data structures.

This module defines the data structures for representing Gulp documents in OpenSearch.
Key classes include:

- GulpBasicDocument: Base model for Gulp documents with essential fields such as ID,
    timestamp, operation, context, and source information.

- GulpDocument: Extended document model with additional fields for event data,
    providing methods for initialization and timestamp validation.

- GulpDocumentFieldAliasHelper: Helper class for managing field aliases in document properties.

- GulpRawDocumentBaseFields: Defines mandatory fields for raw Gulp document records.

- GulpRawDocument: Represents a raw Gulp document with base fields and additional document data.

These structures form the foundation for document handling in the Gulp OpenSearch API.
"""

from typing import Optional, TypeVar, override

import muty.crypto
import muty.time
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.filters import QUERY_DEFAULT_FIELDS, GulpBaseDocumentFilter
import json

from gulp.structs import GulpPluginParameters

T = TypeVar("T", bound=GulpBaseDocumentFilter)


class GulpBasicDocument(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "_id": "1234567890abcdef1234567890abcdef",
                    "@timestamp": "2021-01-01T00:00:00Z",
                    "gulp.timestamp": 1609459200000000000,
                    "gulp.timestamp_invalid": False,
                    "gulp.operation_id": "test_operation",
                    "gulp.context_id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                    "gulp.source_id": "fa144510fd16cf5ffbaeec79d68b593f3ba7e7e0",
                }
            ]
        },
    )

    id: str = Field(
        description='"_id": the unique identifier of the document.',
        alias="_id",
    )
    timestamp: str = Field(
        description='"@timestamp": document timestamp, in iso8601 format.',
        alias="@timestamp",
    )
    gulp_timestamp: int = Field(
        description='"@timestamp": document timestamp in nanoseconds from unix epoch.',
        alias="gulp.timestamp",
    )
    invalid_timestamp: bool = Field(
        False,
        description='True if "@timestamp" is invalid and set to 1/1/1970 (the document should be checked, probably ...).',
        alias="gulp.timestamp_invalid",
    )
    operation_id: str = Field(
        description='"gulp.operation_id": the operation ID the document is associated with.',
        alias="gulp.operation_id",
    )
    context_id: Optional[str] = Field(
        None,
        description='"gulp.context_id": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context_id",
    )
    source_id: Optional[str] = Field(
        None,
        description='"gulp.source_id": the source the document is associated with.',
        alias="gulp.source_id",
    )


class GulpDocument(GulpBasicDocument):
    """
    represents a Gulp document.
    """

    model_config = ConfigDict(
        extra="allow",
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "_id": "1234567890abcdef1234567890abcdef",
                    "@timestamp": "2021-01-01T00:00:00Z",
                    "gulp.timestamp": 1609459200000000000,
                    "gulp.timestamp_invalid": False,
                    "gulp.operation_id": "test_operation",
                    "gulp.context_id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                    "gulp.source_id": "fa144510fd16cf5ffbaeec79d68b593f3ba7e7e0",
                    "agent.type": "win_evtx",
                    "event.original": "raw event content",
                    "event.sequence": 1,
                    "event.code": "1234",
                    "gulp.event_code": 1234,
                    "event.duration": 1,
                    "log.file.path": "C:\\Windows\\System32\\winevt\\Logs\\Security.evtx",
                }
            ]
        },
    )

    log_file_path: Optional[str] = Field(
        None,
        description='"log.file.path": the original log file name or path.',
        alias="log.file.path",
    )
    agent_type: str = Field(
        None,
        description='"agent.type": the ingestion source, i.e. gulp plugin.name().',
        alias="agent.type",
    )
    event_original: Optional[str] = Field(
        None,
        description='"event.original": the original event as text.',
        alias="event.original",
    )
    event_sequence: int = Field(
        0,
        description='"event.sequence": the sequence number of the document in the source.',
        alias="event.sequence",
    )
    event_code: Optional[str] = Field(
        "0",
        description='"event.code": the event code, "0" if missing.',
        alias="event.code",
    )
    gulp_event_code: Optional[int] = Field(
        0,
        description='"gulp.event_code": "event.code" as integer.',
        alias="gulp.event_code",
    )
    event_duration: Optional[int] = Field(
        1,
        description='"event.duration": the duration of the event in nanoseconds, defaults to 1.',
        alias="event.duration",
    )

    @staticmethod
    def ensure_timestamp(
        timestamp: str, plugin_params: GulpPluginParameters = None
    ) -> tuple[str, int, bool]:
        """
        ensure we have a proper iso8601 timestamp and return the timestamp in nanoseconds from unix epoch.

        Args:
            timestamp (str): The time string to parse, must be in one of the following formats:
                - (already in) iso8601 format
                - numeric or numeric string representing seconds/milliseconds/nanoseconds from unix epoch
                - any string format supported by python dateutil.parser

            plugin_params (GulpPluginParameters, optional): The plugin parameters, used to get i.e. the timestamp offset if defined. Defaults to None.
        Returns:
            tuple[str, int, bool]: The timestamp in iso8601 format, the timestamp in nanoseconds from unix epoch, and a boolean indicating if the timestamp is invalid.
        """

        epoch_start: str = "1970-01-01T00:00:00Z"
        # MutyLogger.get_instance().debug(f"ensure_timestamp: {timestamp}, plugin_params={plugin_params}")
        if not timestamp:
            # invalid timestamp
            return epoch_start, 0, True

        ns: int = 0
        try:
            if timestamp.isdigit():
                # timestamp is in seconds/milliseconds/nanoseconds from unix epoch
                ns = muty.time.number_to_nanos_from_unix_epoch(timestamp)
            else:
                ns = muty.time.string_to_nanos_from_unix_epoch(timestamp)

            # Timestamp is epoch or before, that's usually a sign of an invalid timestamp
            if ns <= 0:
                raise ValueError("timestamp is before unix epoch")

            if plugin_params and plugin_params.timestamp_offset_msec:
                # apply offset in milliseconds to the timestamp
                ns += (
                    plugin_params.timestamp_offset_msec
                    * muty.time.MILLISECONDS_TO_NANOSECONDS
                )
                timestamp = str(ns)

            # enforce iso8601 timestamp
            ts_string = muty.time.ensure_iso8601(timestamp)
            return ts_string, ns, False

        except Exception as e:
            # invalid timestamp
            MutyLogger.get_instance().error(f"invalid timestamp: {timestamp}, {e}")
            return epoch_start, 0, True

    @override
    def __init__(
        self,
        plugin_instance,
        operation_id: str | int,
        event_original: str,
        context_id: str = None,
        source_id: str = None,
        event_sequence: int = None,
        timestamp: str = None,
        event_code: str = "0",
        event_duration: int = 1,
        log_file_path: str = None,
        **kwargs,
    ) -> None:
        """
        initializea a GulpDocument instance.

        Args:
            plugin_instance: The calling PluginBase
            operation_id (str): The operation id on gulp collab database.
            event_original (str): The original event data.
            context_id (str): The context id on gulp collab database. if None, it will be attempted to be set from mapping.context_fallback.
            source_id (str): The source id on gulp collab database. if None, it will be attempted to be set from mapping.source_fallback.
            event_sequence (int, optional): The sequence number of the event.
            timestamp (str, optional): the document timestamp, expected to be in ISO8601 format.
                - if set, the plugin handled the timestamp conversion. Defaults to None ("@timestamp" expected in **kwargs and conversion handled by core).
                - if NOT set, "@timestamp" in **kwargs is expected to be a number (seconds/milliseconds/microseconds from the unix epoch), a numeric string (same as number), or a string in a format supported by python datetime.parse.
            event_code (str, optional): The event code. Defaults to "0".
            event_duration (int, optional): The duration of the event. Defaults to 1.
            log_file_path (str, optional): The source log file path. Defaults to None.
            **kwargs: the rest of the document as key/value pairs, to generate the `GulpDocument` with. This may also include the following internal flags:
                - __ignore_default_event_code__ (bool, optional): If True, do not use the default event code from the mapping (for extra documents). Defaults to False.
                - gulp_timestamp (int, optional): The timestamp in nanoseconds from unix epoch. If not set, it will be calculated from the timestamp argument or the "@timestamp" in kwargs.

            Returns:
            None
        """
        # ensure we have non-aliased keys in kwargs (we want i.e. "operation_id" instead of "gulp.operation_id"), to pass to the GulpDocument constructor
        kwargs = GulpDocumentFieldAliasHelper.set_kwargs_and_fix_aliases(kwargs)

        # internal flag, set by _finalize_process_record() in the mapping engine: this will ignore the default event code from the mapping
        # and use the one passed in the event_code argument
        # (this happens when extra documents are generated from a single document, read the corresponding code in plugin.py)
        ignore_default_event_code = kwargs.pop("__ignore_default_event_code__", False)

        # build initial data dict
        mapping: GulpMapping = plugin_instance.selected_mapping()

        # gulp.context_id and gulp.source_id may have been overridden by mapping and put into kwargs
        ctx_id = kwargs.pop("context_id", context_id)
        src_id = kwargs.pop("source_id", source_id)
        # if not ctx_id:
        #     raise ValueError("gulp.context_id is not set, skipping document!")
        # if not src_id:
        #     raise ValueError("gulp.source_id is not set, skipping document!")

        data = {
            "operation_id": operation_id,
            "context_id": ctx_id,
            "source_id": src_id,
            # force agent type from mapping or default to plugin name
            "agent_type": (
                mapping.agent_type
                if mapping and mapping.agent_type
                else plugin_instance.bare_filename
            ),
            "event_original": event_original,
            # force event code from mapping or default to event_code
            "event_code": (
                mapping.event_code
                if mapping and mapping.event_code and not ignore_default_event_code
                else event_code
            ),
            "event_duration": event_duration,
            "log_file_path": log_file_path,
            # add each kwargs as an attribute as-is (may contain event.code, @timestamp, and other fields previously set above, they will be overwritten)
            # @timestamp may have been mapped and already checked for validity in plugin._process_key()
            # if so, we will find it here...
        }
        if event_sequence != None:
            data["event_sequence"] = event_sequence

        data.update(kwargs)
        ts_nanos: int = 0
        invalid: bool = False

        if not timestamp:
            # if not explicitly passed by the plugin, this is expected to be in **kwargs as "@timestamp" (turned to "timestamp" by GulpDocumentFieldAliasHelper)
            timestamp: str = data.get("timestamp", 0)

        ts, ts_nanos, invalid = GulpDocument.ensure_timestamp(
            str(timestamp), plugin_params=plugin_instance._plugin_params
        )
        data["timestamp"] = ts
        data["gulp_timestamp"] = ts_nanos
        if invalid or ts_nanos == 0:
            # flag invalid timestamp
            data["invalid_timestamp"] = True

        # add gulp_event_code (event code as a number)
        data["gulp_event_code"] = (
            int(data["event_code"])
            if data["event_code"].isnumeric()
            else muty.crypto.hash_xxh64_int(data["event_code"])
        )

        # id is a hash of the document
        event_sequence = data.get("event_sequence", 0)
        data["id"] = muty.crypto.hash_xxh128(
            f"{data['event_original']}{data['event_code']}{data['operation_id']}{data['context_id']}{data['source_id']}{event_sequence}"
        )

        # initialize with complete data (and validate)
        super().__init__(**data)

    def __repr__(self) -> str:
        return f"GulpDocument(timestamp={self.timestamp}, gulp_timestamp={self.gulp_timestamp}, operation_id={self.operation_id}, context_id={self.context_id}, agent_type={self.agent_type}, event_sequence={self.event_sequence}, event_code={self.event_code}, event_duration={self.event_duration}, source_id={self.source_id}"

    @override
    def model_dump(
        self,
        lite: bool = False,
        exclude_none: bool = True,
        exclude_unset: bool = True,
        **kwargs,
    ) -> dict:
        """
        Convert the model instance to a dictionary.
        Args:
            lite (bool): If True, return a subset of the dictionary with "_id", "@timestamp",
                  "gulp.context_id", "gulp.operation_id", and "gulp.source_id" keys.
                         Defaults to False.
            **kwargs: Additional keyword arguments to pass to the parent class model_dump method.
        Returns:
            dict: A dictionary representation of the model instance
        """
        d = super().model_dump(
            exclude_none=exclude_none, exclude_unset=exclude_unset, **kwargs
        )
        if lite:
            # return just a minimal subset
            for k in list(d.keys()):
                if k not in QUERY_DEFAULT_FIELDS:
                    d.pop(k, None)
        return d


class GulpDocumentFieldAliasHelper:
    """
    internal helper class to fix alias keys in kwargs with their corresponding field names.
    """

    _alias_to_field_cache: dict[str, str] = {}

    @staticmethod
    def set_kwargs_and_fix_aliases(kwargs: dict) -> dict:
        """
        Replace alias keys in kwargs with their corresponding field names.

        i.e. "event.code" -> "event_code

        - if key is an alias, replace it with the corresponding field name,
        - if key is not an alias, keep it as is.

        NOTE: this is needed to i.e. ingest raw documents already in gulp ecs format.

        Args:
            kwargs (dict): The keyword arguments to fix.
        Returns:
            dict: The fixed keyword arguments.
        """
        if not GulpDocumentFieldAliasHelper._alias_to_field_cache:
            # initialize on first call
            GulpDocumentFieldAliasHelper._alias_to_field_cache = {
                field.alias: name
                for name, field in GulpDocument.model_fields.items()
                if field.alias
            }
        return {
            GulpDocumentFieldAliasHelper._alias_to_field_cache.get(k, k): v
            for k, v in kwargs.items()
        }


class GulpRawDocumentBaseFields(BaseModel):
    """
    the base(=mandatory) fields in a raw GulpDocument record
    """

    model_config = ConfigDict(
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "@timestamp": "2021-01-01T00:00:00Z",
                    "event.original": "raw event content",
                    "event.code": "1234",
                }
            ]
        },
    )
    timestamp: str = Field(
        ...,
        description="the document timestamp, in iso8601 format.",
        alias="@timestamp",
    )
    event_original: str = Field(
        ...,
        description="the original event as text.",
        alias="event.original",
    )


class GulpRawDocument(BaseModel):
    """
    represents a raw GulpDocument record, consisting of:

    - base_fields: these are the mandatory fields (timestamp, event code, original raw event).
    - doc: the rest of the document as key/value pairs, to generate the `GulpDocument` with.
    """

    model_config = ConfigDict(
        extra="allow",
        # solves the issue of not being able to populate fields using field name instead of alias
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "base_fields": autogenerate_model_example_by_class(
                        GulpRawDocumentBaseFields
                    ),
                    "doc": {
                        "agent.type": "win_evtx",
                        "event.original": "raw event content",
                        "event.sequence": 1,
                        "event.code": "1234",
                        "gulp.event_code": 1234,
                        "event.duration": 1,
                        "log.file.path": "C:\\Windows\\System32\\winevt\\Logs\\Security.evtx",
                    },
                }
            ]
        },
    )

    base_fields: GulpRawDocumentBaseFields = Field(
        ...,
        description="the basic fields.",
    )
    doc: dict = Field(
        ...,
        description="the document as key/value pairs, to generate the `GulpDocument` with.",
    )
