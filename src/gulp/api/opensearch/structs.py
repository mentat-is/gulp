import json
from typing import Optional, Union, TypeVar, override

import muty.crypto
import muty.dict
import muty.string
import muty.time
from pydantic import BaseModel, ConfigDict, Field, model_validator

from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.filters import QUERY_DEFAULT_FIELDS, GulpBaseDocumentFilter

EXAMPLE_QUERY_OPTIONS = {
    "example": {
        "disable_notes_on_match": False,
        "include_query_in_result": False,
        "sort": {"@timestamp": "asc"},
    }
}

EXAMPLE_QUERY_FILTER = {
    "example": {
        "event.code": ["5152"],
        "gulp.log.level": [5, 3],
        "start_msec": 1475730263242,
        "end_msec": 1475830263242,
        "gulp.operation.id": [1],
        "gulp.context": ["testcontext"],
    }
}

EXAMPLE_SIGMA_RULE_YML = {
    "example": {
        "pysigma_plugin": "windows",
        "name": "test",
        "tags": ["windows", "security"],
        "type": 1,
        "rule": """title: Test
id: 2dcca7b4-4b3a-4db6-9364-a019d54904bf
status: test
description: This is a test to match all events having gulp.context=*context
references:
  - ref1
  - ref2
tags:
  - attack.execution
  - attack.test
author: me
date: 2020-07-12
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    gulp.context|endswith: context
  condition: selection
fields:
  - gulp.context
falsepositives:
  - Everything
level: medium
""",
    }
}

EXAMPLE_INGESTION_FILTER = {
    "example": {
        "event.code": ["5152"],
        "log.level": [5, 3],
        "start_msec": 1609459200000,
        "end_msec": 1609545600000,
        "extra": {"winlog.event_data.SubjectUserName": "test"},
    }
}

T = TypeVar("T", bound="GulpBaseDocumentFilter")


class GulpBasicDocument(BaseModel):
    model_config = ConfigDict(extra="allow",
                              # solves the issue of not being able to populate fields using field name instead of alias
                              populate_by_name=True)

    """
    a stripped down basic version of a Gulp document, used to associate documents with a note/link
    """

    id: Optional[str] = Field(
        None, description='"_id": the unique identifier of the document.', alias="_id"
    )
    timestamp: Optional[str] = Field(
        None,
        description='"@timestamp": document timestamp, in iso8601 format.',
        alias="@timestamp",
    )
    gulp_timestamp: Optional[int] = Field(
        None,
        description='"@timestamp": document timestamp in nanoseconds from unix epoch',
        alias="gulp.timestamp",
    )
    invalid_timestamp: bool = Field(
        False,
        description="True if \"@timestamp\" is invalid and set to 1/1/1970 (the document should be checked, probably ...).",
        alias='gulp.invalid.timestamp',
    )
    operation: Optional[str] = Field(
        None,
        description='"gulp.operation": the operation ID the document is associated with.',
        alias="gulp.operation",
    )
    context: Optional[str] = Field(
        None,
        description='"gulp.context": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context",
    )
    log_file_path: Optional[str] = Field(
        None,
        description='"log.file.path": identifies the source of the document (i.e. the log file name or path). May be None for events ingested using the "raw" plugin, or generally for everything lacking a "file" (in this case, the source may be identified with "context").',
        alias="log.file.path",
    )


class GulpDocument(GulpBasicDocument):
    """
    represents a Gulp document.
    """
    id: str = Field(
        None, description='"_id": the unique identifier of the document.', alias="_id"
    )
    timestamp: str = Field(
        None,
        description='"@timestamp": document timestamp, in iso8601 format. This field allow queries as described [here](https://opensearch.org/docs/latest/query-dsl/term/range/#date-fields).',
        alias="@timestamp",
    )
    gulp_timestamp: int = Field(
        0,
        description='"gulp.timestamp": document timestamp in nanoseconds from unix epoch. This field allow queries using long numbers.',
        alias="gulp.timestamp"
    )
    invalid_timestamp: bool = Field(
        False,
        description="True if \"@timestamp\" is invalid and set to 1/1/1970 (the document should be checked, probably ...).",
        alias='gulp.invalid.timestamp',
    )
    operation: str = Field(
        None,
        description='"gulp.operation": the operation ID the document is associated with.',
        alias="gulp.operation",
    )
    context: str = Field(
        None,
        description='"gulp.context": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context",
    )
    log_file_path: Optional[str] = Field(
        None,
        description='"log.file.path": identifies the source of the document (i.e. the log file name or path). May be None for events ingested using the "raw" plugin, or generally for everything lacking a "file" (in this case, the source may be identified with "context").',
        alias="log.file.path",
    )
    agent_type: str = Field(
        None,
        description='"agent.type": the ingestion source, i.e. gulp plugin.name().',
        alias="agent.type",
    )
    event_original: str = Field(
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
        0, description='"gulp.event.code": "event.code" as integer.',
        alias="gulp.event.code",
    )
    event_duration: Optional[int] = Field(
        1,
        description='"event.duration": the duration of the event in nanoseconds, defaults to 1.',
        alias="event.duration",
    )

    @staticmethod
    def ensure_timestamp(timestamp: str, dayfirst: bool=None, yearfirst: bool=None, fuzzy: bool=None) -> tuple[str, int, bool]:
        """
        Ensure the timestamp is in iso8601 format.

        Args:
            timestamp (str): The timestamp.
            dayfirst (bool, optional): If set, parse the timestamp with dayfirst=True. Defaults to None (use dateutil.parser default).
            yearfirst (bool, optional): If set, parse the timestamp with yearfirst=True. Defaults to None (use dateutil.parser default).
            fuzzy (bool, optional): If set, parse the timestamp with fuzzy=True. Defaults to None (use dateutil.parser default).
        Returns:
            tuple[str, int, bool]: The timestamp in iso8601 format, the timestamp in nanoseconds from unix epoch, and a boolean indicating if the timestamp is invalid. 
        """
        epoch_start: str='1970-01-01T00:00:00Z'
        if not timestamp:
            return epoch_start, 0, True
        
        try:            
            ts = muty.time.ensure_iso8601(timestamp, dayfirst, yearfirst, fuzzy)
            if timestamp.isdigit():
                # timestamp is in seconds/milliseconds/nanoseconds from unix epoch
                ns = muty.time.number_to_nanos(timestamp)
            else:
                ns = muty.time.string_to_epoch_nsec(ts, dayfirst=dayfirst, yearfirst=yearfirst, fuzzy=fuzzy)
            return ts, ns, False
        except Exception as e:
            # invalid timestamp
            #GulpLogger.get_logger().error(f"invalid timestamp: {timestamp}, {e}")
            return epoch_start, 0, True
    
    @override
    def __init__(
        self,
        plugin_instance,
        operation: str|int,
        context: str,
        event_original: str,
        event_sequence: int,
        timestamp: str=None,
        event_code: str = "0",
        event_duration: int = 1,
        log_file_path: str = None,
        **kwargs,        
    ) -> None:
        """
        Initialize a GulpDocument instance.
        Args:
            plugin_instance: The calling PluginBase
            operation (str): The operation type.
            context (str): The context of the event.
            event_original (str): The original event data.
            event_sequence (int): The sequence number of the event.
            timestamp (str, optional): The timestamp of the event as a number or numeric string (seconds/milliseconds/nanoseconds from unix epoch)<br>
                or a string in a format supported by dateutil.parser.<br>
                if None, assumes **kwargs has been processed by the mapping engine and contains {"@timestamp", "gulp.timestamp" and possibly "gulp.timestamp.invalid" flag}
            event_code (str, optional): The event code. Defaults to "0".
            event_duration (int, optional): The duration of the event. Defaults to 1.
            source (str, optional): The source log file path. Defaults to None.
            **kwargs: Additional keyword arguments to be added as attributes.
        Returns:
            None
        """
        
        super().__init__()
 
        # replace alias keys in kwargs with their corresponding field names
        kwargs = GulpDocumentFieldAliasHelper.set_kwargs_and_fix_aliases(kwargs)
        mapping: GulpMapping = plugin_instance.selected_mapping()        
        
        self.operation = operation
        self.context = context
        if mapping and mapping.agent_type:
            # force agent type from mapping
            self.agent_type = mapping.agent_type
        else:
            # default to plugin name
            self.agent_type = plugin_instance.bare_filename
        self.event_original = event_original
        self.event_sequence = event_sequence
        if mapping and mapping.event_code:
            # force event code from mapping
            self.event_code = mapping.event_code
        else:
            self.event_code = event_code
        self.event_duration = event_duration
        self.log_file_path = log_file_path

        # add gulp_event_code (event code as a number)
        self.gulp_event_code = int(self.event_code) if self.event_code.isnumeric() else muty.crypto.hash_crc24(self.event_code)

        # add each kwargs as an attribute as-is
        # @timestamp may have been mapped and already checked for validity in plugin._process_key()
        # if so, we will find it in the kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)

        if not self.timestamp:
            # use argument, timestamp has been directly passed by the plugin
            self.timestamp = timestamp

        # finally check if it's valid
        self.timestamp, self.gulp_timestamp, invalid = GulpDocument.ensure_timestamp(timestamp,
            dayfirst=mapping.timestamp_dayfirst, yearfirst=mapping.timestamp_yearfirst, fuzzy=mapping.timestamp_fuzzy)
        if invalid:
            # invalid timestamp
            self.invalid_timestamp=True
        
        # id is a hash of the document
        self.id = muty.crypto.hash_blake2b(
            f"{self.event_original}{self.event_code}{self.event_sequence}")
        
        # finally check for consistency
        GulpDocument.model_validate(self)
        #GulpLogger.get_logger().debug(self.model_dump(by_alias=True, exclude='event_original'))
        
    #def __repr__(self) -> str:
    #    return f"GulpDocument(timestamp={self.timestamp}, gulp_timestamp={self.gulp_timestamp}, operation={self.operation}, context={self.context}, agent_type={self.agent_type}, event_sequence={self.event_sequence}, event_code={self.event_code}, event_duration={self.event_duration}, log_file_path={self.log_file_path}"
    
    @override
    def model_dump(self, lite: bool=False, exclude_none: bool=True, exclude_unset: bool=True, **kwargs) -> dict:
        """
        Convert the model instance to a dictionary.
        Args:
            lite (bool): If True, return a subset of the dictionary with "_id", "@timestamp",
                  "gulp.context", "gulp.operation", and "log.file.path" keys.
                         Defaults to False.
            **kwargs: Additional keyword arguments to pass to the parent class model_dump method.
        Returns:
            dict: A dictionary representation of the model instance
        """
        d = super().model_dump(exclude_none=exclude_none, exclude_unset=exclude_unset, **kwargs)
        if lite:
            # return just a minimal subset
            for k in list(d.keys()):
                if k not in QUERY_DEFAULT_FIELDS:
                    d.pop(k,None)
        return d

class GulpDocumentFieldAliasHelper():
    """
    internal helper class to fix alias keys in kwargs with their corresponding field names.
    """
    _alias_to_field_cache: dict[str, str] = {}

    @staticmethod
    def set_kwargs_and_fix_aliases(kwargs: dict) -> dict:
        """
        Replace alias keys in kwargs with their corresponding field names.

        Args:
            kwargs (dict): The keyword arguments to fix.
        Returns:
            dict: The fixed keyword arguments.
        """
        if not GulpDocumentFieldAliasHelper._alias_to_field_cache:
            # initialize
            GulpDocumentFieldAliasHelper._alias_to_field_cache = {field.alias: name for name, field in GulpDocument.model_fields.items() if field.alias}
        return {GulpDocumentFieldAliasHelper._alias_to_field_cache.get(k, k): v for k, v in kwargs.items()}
    


