import json
from enum import IntEnum
from typing import Optional, Union, TypeVar, override

import muty.crypto
import muty.dict
import muty.string
import muty.time
from pydantic import BaseModel, Field, model_validator

from gulp.api.mapping.models import GulpMapping
from gulp.defs import GulpSortOrder

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


class GulpBaseDocumentFilter(BaseModel):
    """
    base class for Gulp filters acting on documents.
    """

    time_range: Optional[tuple[int, int]] = Field(
        None,
        description="include documents matching `@timestamp` in a time range [start, end], inclusive, in nanoseconds from unix epoch.",
    )

    opt_query_string_parameters: Optional[dict] = Field(
        None,
        description="additional parameters to be applied to the resulting `query_string` query, according to [opensearch documentation](https://opensearch.org/docs/latest/query-dsl/full-text/query-string)",
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: str | dict = None) -> dict:
        if not data:
            return {}

        if isinstance(data, dict):
            return data

        return json.loads(data)

    def to_dict(self) -> dict:
        return self.model_dump()

    @staticmethod
    def from_dict(type: T, d: dict) -> T:
        return type(**d)


class GulpDocumentFilterResult(IntEnum):
    """wether if the event should be accepted or skipped during ingestion."""

    ACCEPT = 0
    SKIP = 1


class GulpIngestionFilter(GulpBaseDocumentFilter):
    """
    a GulpIngestionFilter defines a filter for the ingestion API.<br><br>

    each field is optional, if no filter is specified all events are ingested.
    """

    model_config = {"json_schema_extra": EXAMPLE_INGESTION_FILTER}

    opt_storage_ignore_filter: Optional[bool] = Field(
        False,
        description="on filtering during ingestion, websocket receives filtered results while OpenSearch stores all documents anyway (default=False=both OpenSearch and websocket receives the filtered results).",
    )

    @staticmethod
    def filter_doc_for_ingestion(
        doc: dict, flt: "GulpIngestionFilter" = None
    ) -> GulpDocumentFilterResult:
        """
        Check if a document is eligible for ingestion based on a filter.

        Args:
            doc (dict): The GulpDocument dictionary to check.
            flt (GulpIngestionFilter): The filter parameters, if any.

        Returns:
            GulpEventFilterResult: The result of the filter check.
        """
        # GulpLogger().error(flt)
        if not flt or flt.opt_storage_ignore_filter:
            # empty filter or ignore
            return GulpDocumentFilterResult.ACCEPT

        if flt.time_range:
            ts = doc["gulp.timestamp"]
            if ts <= flt.time_range[0] or ts >= flt.time_range[1]:
                return GulpDocumentFilterResult.SKIP

        return GulpDocumentFilterResult.ACCEPT


# mandatory fields to be included in the result for queries
QUERY_DEFAULT_FIELDS = [
    "_id",
    "@timestamp",
    "gulp.timestamp",
    "gulp.operation",
    "gulp.context",
    "log.file.path",
    "event.duration",
    "event.code",
    "gulp.event.code",
]

class GulpQueryFilter(GulpBaseDocumentFilter):
    """
    a GulpQueryFilter defines a filter for the query API.
    """

    model_config = {"json_schema_extra": EXAMPLE_QUERY_FILTER}

    agent_type: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `agent.type`/s.",
    )
    id: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `_id`/s.",
    )
    operation: Optional[list[str]] = Field(
        None,
        description="include documents  matching the given `gulp.operation`/s.",
    )
    context: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `gulp.context`/s.",
    )
    log_file_path: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `log.file.path`/s.",
    )
    event_code: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `event.code`/s.",
    )
    event_original: Optional[str] = Field(
        None,
        description="include documents matching the given `event.original`/s.",
    )
    extra: Optional[dict] = Field(
        None,
        description='include documents matching the given `extra` field/s (as OR), i.e. { "winlog.event_data.SubjectUserName": "test" }.',
    )
    opt_event_original_full_text_search: bool = Field(
        False,
        description="if True, perform a full [text](https://opensearch.org/docs/latest/field-types/supported-field-types/text/) search on `event.original` field.<br>"
        "default=False, uses [keyword](https://opensearch.org/docs/latest/field-types/supported-field-types/keyword/).",
    )

    def _query_string_build_or_clauses(self, field: str, values: list) -> str:
        qs = "("
        for v in values:
            """
            if isinstance(v, str):
                # only enclose if there is a space in the value
                vv = muty.string.enclose(v) if " " in v else v
            else:
                vv = v
            """
            qs += f"{field}: {v} OR "

        qs = qs[:-4]
        qs += ")"
        return qs

    def _query_string_build_eq_clause(self, field: str, v: int | str) -> str:
        """
        if isinstance(v, str):
            # only enclose if there is a space in the value
            vv = muty.string.enclose(v) if " " in v else v
        else:
            vv = v
        """
        qs = f"{field}: {v}"
        return qs

    def _query_string_build_gte_clause(self, field: str, v: int | str) -> str:
        qs = f"{field}: >={v}"
        return qs

    def _query_string_build_lte_clause(self, field: str, v: int | str) -> str:
        qs = f"{field}: <={v}"
        return qs

    def _query_string_build_exists_clause(self, field: str, exist: bool) -> str:
        if exist:
            qs = f"_exists_: {field}"
        else:
            qs = f"NOT _exists_: {field}"
        return qs

    """
    @staticmethod
    def gulpqueryflt_dsl_dict_empty(d: dict) -> bool:
        # check if the filter is empty ('*' in the query_string query).
        query = d.get("query", None)
        if query is not None:
            query_string = query.get("query_string", None)
            if query_string is not None:
                q = query_string.get("query", None)
                if q is not None:
                    if q in ["*", "", None]:
                        # empty filter
                        return True
                    return False
        return True
    """

    def to_opensearch_dsl(self, timestamp_field: str = "@timestamp", flt: "GulpQueryFilter"=None) -> dict:
        """
        convert to a query in OpenSearch DSL format using [query_string](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query

        Args:
            timestamp_field (str, optional): The timestamp field, default="@timestamp"
            flt (GulpQueryFilter, optional): used to pre-filter the query, default=None
        Returns:
            dict: a ready to be used query object for the search API, like:
            ```json
            {
                "query": {
                    "query_string": {
                        "query": "agent.type: \"winlogbeat\" AND gulp.operation: \"test\" AND gulp.context: \"testcontext\" AND log.file.path: \"test.log\" AND _id: \"testid\" AND event.original: \"test event\" AND event.code: \"5152\" AND @timestamp: >=1609459200000 AND @timestamp: <=1609545600000",
                        "analyze_wildcard": true,
                        "default_field": "_id"
                    }
                }
            }
            ```
        """
        def _build_clauses():
            clauses = []
            if self.agent_type:
                clauses.append(self._query_string_build_or_clauses("agent.type", self.agent_type))
            if self.operation:
                clauses.append(self._query_string_build_or_clauses("gulp.operation", self.operation))
            if self.context:
                clauses.append(self._query_string_build_or_clauses("gulp.context", self.context))
            if self.log_file_path:
                clauses.append(self._query_string_build_or_clauses("log.file.path", self.log_file_path))
            if self.id:
                clauses.append(self._query_string_build_or_clauses("_id", self.id))
            if self.event_original:
                field = "event.original.text" if self.opt_event_original_full_text_search else "event.original"
                clauses.append(self._query_string_build_eq_clause(field, self.event_original))
            if self.event_code:
                clauses.append(self._query_string_build_or_clauses("event.code", self.event_code))
            if self.time_range:
                if self.time_range[0]:
                    clauses.append(self._query_string_build_gte_clause(timestamp_field, self.time_range[0]))
                if self.time_range[1]:
                    clauses.append(self._query_string_build_lte_clause(timestamp_field, self.time_range[1]))
            if self.extra:
                for k, v in self.extra.items():
                    clauses.append(self._query_string_build_or_clauses(k, v))
            return clauses

        d = self.model_dump(exclude_none=True)
        qs = "*" if not d else " AND ".join(filter(None, _build_clauses())) or "*"

        # default_field: _id below is an attempt to fix "field expansion matches too many fields"
        # https://discuss.elastic.co/t/no-detection-of-fields-in-query-string-query-strings-results-in-field-expansion-matches-too-many-fields/216137/2
        # (caused by "default_field" which by default is "*" and the query string is incorrectly parsed when parenthesis are used as we do, maybe this could be fixed in a later opensearch version as it is in elasticsearch)
        query_dict = {
            "query": {
                "query_string": {
                    "query": qs,
                    "analyze_wildcard": True,
                    "default_field": "_id",
                }
            }
        }

        if qs != "*" and self.opt_query_string_parameters:
            query_dict["query"]["query_string"].update(self.opt_query_string_parameters)

        if flt:
            # merge with the provided filter using a bool query
            query_dict = {
                "query": {
                    "bool": {
                        "filter": [
                            flt.to_opensearch_dsl()['query'],
                            query_dict["query"],
                        ]
                    }
                }
            }
        
            
        # GulpLogger().debug('flt=%s, resulting query=%s' % (flt, json.dumps(query_dict, indent=2)))
        return query_dict

class GulpQueryAdditionalOptions(BaseModel):
    """
    additional options for a query.
    """
    opt_sort: Optional[dict[str, GulpSortOrder]] = Field(
        default={"@timestamp": "asc", "_id": "asc", "event.sequence": "asc"},
        max_length=1,
        description="how to sort results, default=sort by ascending `@timestamp`.",
    )
    opt_fields: Optional[list[str]] = Field(
        default=QUERY_DEFAULT_FIELDS,
        description="the set of fields to include in the returned documents.<br>"
        "default=`%s` (which are forcefully included anyway), use `None` to return all fields."
        % (QUERY_DEFAULT_FIELDS),
    )
    opt_limit: Optional[int] = Field(
        1000,
        gt=1,
        le=10000,
        description="for pagination: the maximum number of documents to return in a chunk, default=1000 (None=return up to 10000 documents).",
    )
    opt_search_after: Optional[list[Union[str, int]]] = Field(
        None,
        description="for pagination: this is the last value returned as \"search_after\" from the previous query, to be used as start offset.",
    )

    def parse(self) -> dict:
        """
        Parse the additional options to a dictionary for the OpenSearch query api.

        Returns:
            dict: The parsed dictionary.
        """
        n = {}
        
        # sorting
        n["sort"] = []
        for k, v in self.opt_sort.items():
            n["sort"].append({k: {"order": v}})
            # NOTE: this was "event.hash" before: i removed it since its values is the same as _id now, so put _id here.
            # if problems (i.e. issues with sorting on _id), we can add it back just by duplicating _id 
            if "_id" not in self.opt_sort:
                n["sort"].append({"_id": {"order": v}})
            if "event.sequence" not in self.opt_sort:
                n["sort"].append({"event.sequence": {"order": v}})

        # fields to be returned
        if self.opt_fields:
            # only return these fields (must always include the defaults)
            for f in QUERY_DEFAULT_FIELDS:
                if f not in self.opt_fields:
                    self.opt_fields.append(f)
            n["_source"] = self.opt_fields

        # pagination: doc limit
        if self.opt_limit is not None:
            # use provided
            n["size"] = self.opt_limit

        # pagination: start from
        if self.opt_search_after:
            # next chunk from this point
            n["search_after"] = self.opt_search_after
        else:
            n["search_after"] = None

        # GulpLogger().debug("query options: %s" % (json.dumps(n, indent=2)))
        return n  
      
class GulpAssociatedDocument(BaseModel):
    """
    a stripped down version of a Gulp document, used to associate documents with a note/link
    """

    id: Optional[str] = Field(
        None, description='"_id": the unique identifier of the document.', alias="_id"
    )
    timestamp: Optional[int] = Field(
        None,
        description='"@timestamp": document original timestamp in nanoseconds from unix epoch',
        alias="@timestamp",
    )


class GulpDocument(BaseModel):
    """
    represents a Gulp document.
    """
    class Config:
        extra = "allow"
        # solves the issue of not being able to populate fields with the same name as the model fields (aliasing)
        populate_by_name = True 

    id: str = Field(
        None, description='"_id": the unique identifier of the document.', alias="_id"
    )
    timestamp: str = Field(
        None,
        description='"@timestamp": document timestamp, in iso8601 format.',
        alias="@timestamp",
    )
    gulp_timestamp: int = Field(
        0,
        description='"gulp.timestamp": document timestamp in nanoseconds from unix epoch.',
        alias="gulp.timestamp"
    )
    invalid_timestamp: bool = Field(
        False,
        description="True if \"@timestamp\" is invalid and set to 1/1/1970 (the document should be checked, probably ...).",
        alias='gulp.invalid.timestamp',
    )
    operation: str = Field(
        ...,
        description='"gulp.operation": the operation ID the document is associated with.',
        alias="gulp.operation",
    )
    context: str = Field(
        ...,
        description='"gulp.context": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context",
    )
    agent_type: str = Field(
        None,
        description='"agent.type": the ingestion source, i.e. gulp plugin.name().',
        alias="agent.type",
    )
    event_original: str = Field(
        ...,
        description='"event.original": the original event as text.',
        alias="event.original",
    )
    event_sequence: int = Field(
        ...,
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
    log_file_path: Optional[str] = Field(
        None,
        description='"log.file.path": identifies the source of the document (i.e. the log file name or path). May be None for events ingested using the "raw" plugin, or generally for everything lacking a "file" (in this case, the source may be identified with "context").',
        alias="log.file.path",
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
            ts = muty.time.ensure_iso8601(timestamp, dayfirst, yearfirst, fuzzy), False
            if timestamp.isdigit():
                # timestamp is in seconds/milliseconds/nanoseconds from unix epoch
                ns = muty.time.number_to_nanos(timestamp)
            else:
                ns = muty.time.string_to_epoch_nsec(ts, dayfirst=dayfirst, yearfirst=yearfirst, fuzzy=fuzzy)
            return ts, ns, False
        except Exception as e:
            # invalid timestamp
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
        
        #GulpLogger().debug('--> GulpDocument.__init__: timestamp=%d, operation=%s, context=%s, event_original=%s, event_sequence=%s, event_code=%s, event_duration=%s, source=%s, kwargs=%s' % ( timestamp, operation, context, muty.string.make_shorter(event_original), event_sequence, event_code, event_duration, source, kwargs, ))
        super().__init__(timestamp=str(timestamp), operation=operation, context=context, event_original=event_original, event_sequence=event_sequence, event_code=event_code, event_duration=event_duration, log_file_path=log_file_path, **kwargs)
        mapping: GulpMapping = plugin_instance.selected_mapping()        
        
        self.operation = operation
        self.context = context
        if mapping and mapping.opt_agent_type:
            # force agent type from mapping
            self.agent_type = mapping.opt_agent_type
        else:
            # default to plugin name
            self.agent_type = plugin_instance.bare_filename
        self.event_original = event_original
        self.event_sequence = event_sequence
        if mapping and mapping.opt_event_code:
            # force event code from mapping
            self.event_code = mapping.opt_event_code
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
            # it was not in the kwargs, check if it's being passed in the arguments, either flag it as invalid
            self.timestamp, self.gulp_timestamp, invalid = GulpDocument.ensure_timestamp(timestamp,
                dayfirst=mapping.opt_timestamp_dayfirst, yearfirst=mapping.opt_timestamp_yearfirst, fuzzy=mapping.opt_timestamp_fuzzy)
            if invalid:
                # invalid timestamp
                self.invalid_timestamp=True
        
        # id is a hash of the document
        self.id = muty.crypto.hash_blake2b(
            f"{self.event_original}{self.event_code}{self.event_sequence}")
        
        # finally check for consistency
        GulpDocument.model_validate(self)
        #GulpLogger().debug(self.model_dump(by_alias=True, exclude='event_original'))
        
    def __repr__(self) -> str:
        return f"GulpDocument(timestamp={self.timestamp}, gulp_timestamp={self.gulp_timestamp}, operation={self.operation}, context={self.context}, agent_type={self.agent_type}, event_sequence={self.event_sequence}, event_code={self.event_code}, event_duration={self.event_duration}, log_file_path={self.log_file_path}"
    
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

class GulpQueryType(IntEnum):
    """Gulp rule types"""

    SIGMA_YAML = 1  # sigma rule YML
    # raw query dict
    RAW = 2
    GULP_FILTER = 3  # GULP filter
    INDEX = 4  # an index of a stored query
