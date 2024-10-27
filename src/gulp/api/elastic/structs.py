import json
from enum import IntEnum, StrEnum
from typing import Any, Dict, Literal, Optional, Set, Union, TypeVar, override

import muty.crypto
import muty.dict
import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.mapping.models import FieldMappingEntry
from gulp.defs import GulpLogLevel, GulpSortOrder
from gulp.plugin_internal import GulpPluginParams

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

    T = TypeVar("T", bound="GulpBaseDocumentFilter")

    @staticmethod
    def from_dict(type: T, d: dict) -> T:
        return type(**d)


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


# mandatory fields to be included in the result for queries
QUERY_DEFAULT_FIELDS = [
    "_id",
    "@timestamp",
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
    opt_limit: int = Field(
        1000,
        gt=1,
        le=10000,
        description="maximum number of results to return per chunk, default=1000",
    )
    opt_sort: dict[str, GulpSortOrder] = Field(
        default={"@timestamp": "asc"},
        max_length=1,
        description="how to sort results, default=sort by ascending `@timestamp`.",
    )
    opt_fields: list[str] = Field(
        default=QUERY_DEFAULT_FIELDS,
        description="the set of fields to include in the returned documents.<br>"
        "default=`%s` (which are forcefully included anyway), use `None` to return all fields."
        % (QUERY_DEFAULT_FIELDS),
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

    def to_opensearch_dsl(self, timestamp_field: str = "@timestamp") -> dict:
        """
        convert to a query in OpenSearch DSL format using [query_string](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query

        Args:
            timestamp_field (str, optional): The timestamp field, default="@timestamp"
        Returns:
            dict: The Elasticsearch query dictionary.

        """
        d = self.model_dump(exclude_none=True)
        if not d:
            # empty filter
            qs = "*"
        else:
            # build the query string
            clauses = []
            if self.agent_type:
                clauses.append(
                    self._query_string_build_or_clauses("agent.type", self.agent_type)
                )
            if self.operation:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.operation", self.operation
                    )
                )
            if self.context:
                clauses.append(
                    self._query_string_build_or_clauses("gulp.context", self.context)
                )
            if self.log_file_path:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "log.file.path", self.log_file_path
                    )
                )
            if self.id:
                clauses.append(self._query_string_build_or_clauses("_id", self.id))
            if self.event_original:
                field = (
                    "event.original.text"
                    if self.opt_event_original_full_text_search
                    else "event.original"
                )
                clauses.append(
                    self._query_string_build_eq_clause(field, self.event_original)
                )
            if self.event_code:
                clauses.append(
                    self._query_string_build_or_clauses("event.code", self.event_code)
                )
            if self.time_range:
                if self.time_range[0]:
                    clauses.append(
                        self._query_string_build_gte_clause(
                            timestamp_field, self.time_range[0]
                        )
                    )
                if self.time_range[1]:
                    clauses.append(
                        self._query_string_build_lte_clause(
                            timestamp_field, self.time_range[1]
                        )
                    )
            if self.extra:
                for k, v in self.extra.items():
                    clauses.append(self._query_string_build_or_clauses(k, v))

            qs = " AND ".join(filter(None, clauses)) or "*"

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

        # print('flt=%s, resulting query=%s' % (flt, json.dumps(query_dict, indent=2)))
        return query_dict


class GulpAssociatedDocument(BaseModel):
    """
    base class for Gulp documents.
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

    id: str = Field(
        description='"_id": the unique identifier of the document.', alias="_id"
    )
    hash: str = Field(
        description='"event.hash": the hash of the event.', alias="event.hash"
    )
    gulp_event_code: int = Field(
        description='"gulp.event_code": the event code as an integer.',
        alias="gulp.event_code",
    )
    timestamp: int = Field(
        ...,
        description='"@timestamp": document original timestamp in nanoseconds from unix epoch',
        alias="@timestamp",
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
        ...,
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

    @override
    def __init__(
        self,
        timestamp: int,
        operation: str,
        context: str,
        agent_type: str,
        event_original: str,
        event_sequence: int,
        event_code: str,
        event_duration: int = 1,
        source: str = None,
        **kwargs,
    ) -> None:
        super().__init__()
        self.timestamp = timestamp
        if not self.timestamp:
            # flag as invalid
            self.invalid_timestamp = True
            self.timestamp = 0

        self.operation = operation
        self.context = context
        self.agent_type = agent_type
        self.event_original = event_original
        self.event_sequence = event_sequence
        self.event_code = event_code
        self.event_duration = event_duration
        self.log_file_path = source
        self.hash = muty.crypto.hash_blake2b(
            f"{self.event_original}{event_code}{self.event_sequence}"
        )
        self.id = self.hash

        # add gulp_event_code (event code as a number)
        self.gulp_event_code = (
            int(self.event_code)
            if self.event_code.isnumeric()
            else muty.crypto.hash_crc24(self.event_code)
        )

        # add each kwargs as an attribute as-is
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self) -> str:
        return f"GulpDocument(timestamp={self.timestamp}, operation={self.operation}, context={self.context}, agent_type={self.agent_type}, event_sequence={self.event_sequence}, event_code={self.event_code}, event_duration={self.event_duration}, log_file_path={self.log_file_path}"

    def to_dict(self, lite: bool = False) -> dict:
        d = self.model_dump(exclude_none=True, exclude_unset=True)
        if lite:
            # return just this subset
            for k in list(d.keys()):
                if k not in [
                    "_id",
                    "@timestamp",
                    "gulp.context",
                    "gulp.operation",
                    "log.file.path",
                ]:
                    del d[k]
        return d


class GulpQueryType(IntEnum):
    """Gulp rule types"""

    SIGMA_YAML = 1  # sigma rule YML
    # raw query dict
    RAW = 2
    GULP_FILTER = 3  # GULP filter
    INDEX = 4  # an index of a stored query
