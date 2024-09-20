import json
from enum import IntEnum, StrEnum
from typing import Any, Optional, Union

import muty.crypto
import muty.dict
import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.mapping.models import FieldMappingEntry
from gulp.defs import GulpLogLevel, SortOrder
from gulp.plugin_internal import GulpPluginParams

EXAMPLE_QUERY_OPTIONS = {
    "example": {
        "disable_notes_on_match": False,
        "fields_filter": "_id,@timestamp",
        "include_query_in_result": False,
        "all_tags_must_match": False,
        "sort": {"@timestamp": "asc"},
    }
}

EXAMPLE_QUERY_FILTER = {
    "example": {
        "event_code": ["5152"],
        "gulp.log.level": [5, 3],
        "start_msec": 1475730263242,
        "end_msec": 1475830263242,
        "operation_id": [1],
        "context": ["testcontext"],
    }
}

EXAMPLE_SIGMA_RULE_YML = {
    "example": {
        "pysigma_plugin": "gs_windows",
        "name": "test",
        "tags": ["windows", "security"],
        "type": 1,
        "rule": """title: Test
id: 2dcca7b4-4b3a-4db6-9364-a019d54904bf
status: test
description: This is a test
references:
  - ref1
  - ref2
tags:
  - attack.execution
  - attack.t1059
author: Thomas Patzke
date: 2020/07/12
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    EventID: 4732
    SourceHostname|endswith: context
  condition: selection
fields:
  - EventId
  - SourceHostname
falsepositives:
  - Everything
level: medium
""",
    }
}

EXAMPLE_INGESTION_FILTER = {
    "example": {
        "event_code": ["5152"],
        "level": [5, 3],
        "start_msec": 1609459200000,
        "end_msec": 1609545600000,
        "extra": {"winlog.event_data.SubjectUserName": "test"},
    }
}


class GulpBaseFilter(BaseModel):
    """
    a GulpBaseFilter defines a filter for the query/ingestion API.<br><br>

    each field is optional.<br><br>

    if no filter is specified, all events are considered.
    """

    category: Optional[list[str]] = Field(
        None,
        description="filter to include events of certain (plugin dependent) categories.",
    )
    gulp_log_level: Optional[list[GulpLogLevel]] = Field(
        None,
        description="filter to include events of certain GULP loglevels (the original level mapped to GULP numeric GulpLogLevel, which may be different).",
    )
    event_code: Optional[list[str]] = Field(
        None,
        description="filter to include events of certain (plugin dependent) event codes.",
    )
    start_msec: Optional[int] = Field(
        None,
        description="filter to include events happened AFTER a certain timestamp (inclusive, milliseconds from unix epoch).",
    )
    end_msec: Optional[int] = Field(
        None,
        description="filter to include events happened BEFORE a certain timestamp (inclusive, milliseconds from unix epoch).",
    )
    extra: Optional[dict] = Field(
        None,
        description='filter as {"key": value, ...} to include events matching further field/s not included in the predefined set.<br>'
        'NOTE: not supported for filtering events sent through websocket when "store_all_documents" is set to True.',
    )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpIngestionFilter(GulpBaseFilter):
    """
    a GulpIngestionFilter defines a filter for the ingestion API.<br><br>

    each field is optional, if no filter is specified all events are ingested.
    """

    # TODO: openapi_examples seems not working with multipart/form-data requests, so we put the example here instead of in the Annotation in rest_api.py
    model_config = {"json_schema_extra": EXAMPLE_INGESTION_FILTER}

    store_all_documents: Optional[bool] = Field(
        False,
        description="filtering is done on websocket data only, but all of the documents are stored on database.",
    )

    def to_dict(self) -> dict:
        d = {
            "store_all_documents": self.store_all_documents,
            "category": self.category,
            "event_code": self.event_code,
            "level": self.gulp_log_level,
            "start_msec": self.start_msec,
            "end_msec": self.end_msec,
            "extra": self.extra,
        }
        return d

    @staticmethod
    def from_dict(d: dict) -> "GulpIngestionFilter":
        """
        Create a GulpIngestionFilter object from a dictionary.

        Args:
            d (dict): The dictionary containing the filter attributes.

        Returns:
            GulpIngestionFilter: The created GulpIngestionFilter object.
        """
        return GulpIngestionFilter(**d)


class GulpQueryFilter(GulpBaseFilter):
    """
    a GulpQueryFilter defines a filter for the query API, and is to be used together with `GulpQueryOptions`.
    <br><br>
    each field in a GulpQueryFilter is optional:<br>
    if none is specified, ALL events (depending on GulpQueryOptions "limit", and up to 10k events in a shot as per OpenSearch limitation) are returned.<br>
    wildcards are supported, combining multiple filters (i.e. plugin + operation + context + ...) matches as AND, internally each *list[]* filter is matched as OR.
    <br><br>
    internally, wherever a GulpQueryFilter is used, it is converted to a [`query_string`](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query.<br>
    when it is used together with Sigma queries (i.e. as in `query_sigma_zip`), the resulting query is combined to the converted sigma query using a [compund bool/must](https://opensearch.org/docs/latest/query-dsl/compound/bool/) query.
    <br><br>
    to customize the resulting query, `query_string_parameters` can be used to pass additional parameters to the `query_string` query.
    """

    plugin: Optional[list[str]] = Field(
        None,
        description="filter to include events ingested by one or more plugin (`agent.type` in the document).",
    )
    client_id: Optional[list[int]] = Field(
        None,
        description="filter to include events ingested by one or more client id (`agent.id` in the document).",
    )
    event_id: Optional[list[str]] = Field(
        None,
        description="filter to include events having the given id/s in the source log (`event.id` in the document).",
    )
    elastic_id: Optional[list[str]] = Field(
        None,
        description="filter to include having the given document `_id`/s on the gulp storage.",
    )
    src_file: Optional[list[str]] = Field(
        None,
        description="filter to include events belonging to one or more source logs (`gulp.source.file` in the document).",
    )
    operation_id: Optional[list[int]] = Field(
        None,
        description="filter to include events belonging to one or more operation_ids.",
    )
    context: Optional[list[str]] = Field(
        None,
        description="filter to include events belonging to one or more contexts (`gulp.context` in the document).",
    )
    ev_hash: Optional[list[str]] = Field(
        None,
        description="filter to include events matching the given `blake2b` hash (the hash assigned when the document is stored into gulp).",
    )
    raw: Optional[str] = Field(
        None,
        description="filter to include events matching the the given raw text (`event.original` in the document).",
    )
    query_string_parameters: Optional[dict] = Field(
        None,
        description="additional parameters to be applied to the resulting `query_string` query, according to [opensearch documentation](https://opensearch.org/docs/latest/query-dsl/full-text/query-string)",
    )

    # TODO: openapi_examples seems not working with multipart/form-data requests, so we put the example here instead of in the Annotation in rest_api.py
    model_config = {"json_schema_extra": EXAMPLE_QUERY_FILTER}

    def to_dict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        d = {
            "category": self.category,
            "event_code": self.event_code,
            "level": self.gulp_log_level,
            "start_msec": self.start_msec,
            "end_msec": self.end_msec,
            "extra": self.extra,
            "plugin": self.plugin,
            "client_id": self.client_id,
            "event_id": self.event_id,
            "elastic_id": self.elastic_id,
            "src_file": self.src_file,
            "operation_id": self.operation_id,
            "context": self.context,
            "ev_hash": self.ev_hash,
            "raw": self.raw,
            "query_string_parameters": self.query_string_parameters,
        }
        return d

    @staticmethod
    def from_dict(flt: dict) -> "GulpQueryFilter":
        """
        Create a GulpQueryFilter object from a dictionary.

        Args:
            flt (dict): The dictionary containing the filter attributes.

        Returns:
            GulpQueryFilter: The created GulpQueryFilter object.
        """
        return GulpQueryFilter(**flt)


class GulpDocument:
    __slots__ = (
        "idx",
        "operation_id",
        "context",
        "plugin",
        "client_id",
        "original_id",
        "src_file",
        "event_code",
        "gulp_event_code",
        "cat",
        "duration_nsec",
        "original_event",
        "gulp_log_level",
        "original_log_level",
        "timestamp",
        "timestamp_nsec",
        "extra",
        "hash",
    )
    """
    represents a Gulp document.
    """

    def __init__(
        self,
        fme: list[FieldMappingEntry],
        idx: int,
        operation_id: int,
        context: str,
        plugin: str,
        client_id: int,
        raw_event: str,
        original_id: str,
        src_file: str,
        event_code: str,
        f: FieldMappingEntry = None,
        timestamp: int = None,
        timestamp_nsec: int = None,
        cat: list[str] = None,
        duration_nsec: int = 0,
        gulp_log_level: GulpLogLevel = None,
        original_log_level: str = None,
        **kwargs,
    ) -> None:
        """
        Initializes a new GulpDocument object.

        NOTE: each field not present in the ECS standard (https://www.elastic.co/guide/en/ecs/current/index.html) mapping should be named as "gulp.<field_name>" (i.e. "gulp.event.code").
              "gulp.event.code" is always guaranteed to be numeric ("event.code" is converted to int or hashed if not convertible to int).
              "gulp.log.level", if set, is the numeric representation of the log level (mapped from the original log level to GulpLogLevel).

        Args:
            fme: list[FieldMappingEntry]: a list of FieldMappingEntry objects.
            idx(int): index of the document in src_file (i.e. the order in which the document was ingested).
            operation_id (int): The operation ID in the collab database
            context (str): The context of the document.
            plugin (str): The plugin of the document.
            client_id (int): ID of the client that performed the ingestion
            raw_event (str: The raw event as text.
            original_id (str): The original event ID.
            src_file (str): The log file this event comes from (or, generically, the source in case it is not a file).
            event_code (str, optional): will be mapped to "event.code".
            f: (FieldMappingEntry, optional): a FieldMappingEntry object (the one with specific document extras).
            timestamp (int, optional): The timestamp of the event (in milliseconds from unix epoch). Defaults to None (provided in fme).
            timestamp_nsec (int, optional): The timestamp of the event (in nanoseconds from unix epoch). Defaults to None (provided in fme).
            cat (list[str], optional): Event category. Defaults to None.
            duration_nsec (int, optional): The duration of the event (in nanoseconds). Defaults to 0.
            gulp_log_level (GulpLogLevel, optional): The log level (one of the GulpLogLevel values). Defaults to None.
            original_log_level (str, optional): The original log level. Defaults to None.
            kwargs (optional): additional keyword arguments to be added to the "extra" dictionary.

        """
        self.idx = idx
        self.operation_id = operation_id
        self.context = context
        self.plugin = plugin
        self.client_id = str(client_id)
        self.original_id = str(original_id)
        self.src_file = src_file
        self.event_code = event_code or "0"
        self.cat = cat
        self.duration_nsec = duration_nsec
        self.original_event = raw_event
        self.gulp_log_level = gulp_log_level.value if gulp_log_level else None
        self.original_log_level = original_log_level or None
        self.timestamp = timestamp
        self.timestamp_nsec = timestamp_nsec
        self.hash = muty.crypto.hash_blake2b(f"{raw_event}{event_code}{self.idx}")

        # build extras (may override values set in self when turning to dict)
        # self.extra = {**kwargs}
        self.extra = {}
        self.extra.update(
            {k: v for ff in fme if ff.result for k, v in ff.result.items()}
        )
        if f and f.is_timestamp and f.event_code:
            # it's a timestamp and there is an event code set, override
            self.event_code = f.event_code

        # event code must also be set as a number
        gulp_event_code = (
            int(self.event_code)
            if self.event_code.isnumeric()
            else muty.crypto.hash_crc24(self.event_code)
        )
        self.gulp_event_code = gulp_event_code

        # handle invalid timestamp
        if self.timestamp is None and "@timestamp" not in self.extra:
            GulpDocument.add_invalid_timestamp(self.extra)

        # logger().error(f"**** DOC INIT: FME={fme}\n, DOC={self}")

    def __repr__(self) -> str:
        return f"GulpDocument(idx={self.idx}, \
                operation_id={self.operation_id}, \
                context={self.context}, \
                plugin={self.plugin}, \
                client_id={self.client_id}, \
                original_id={self.original_id}, \
                gulp_log_level={self.gulp_log_level}, \
                cat={self.cat}, \
                src_file={self.src_file}, \
                event_code={self.event_code}, \
                timestamp={self.timestamp}, \
                timestamp_nsec={self.timestamp_nsec}, \
                duration_nsec={self.duration_nsec}, \
                extra={self.extra}"

    def to_dict(self) -> dict:
        """
        returns a dict representation of the document ready to be ingested into elastic.

        NOTE: when turning a GulpDocument to dictionary (before insertion on ElasticSearch), all the fields except "extra" are mapped as close as possible to the corresponding ECS fields (https://www.elastic.co/guide/en/ecs/current/index.html).
        also note that fields in the "extra" dictionary are added as is.
        """
        # create the dictionary using dictionary comprehension
        d = {
            "operation_id": self.operation_id,
            "gulp.context": self.context,
            "agent.type": self.plugin,
            "agent.id": self.client_id,
            "event.id": self.original_id,
            "event.sequence": self.idx,
            "gulp.source.file": self.src_file,
            "event.code": self.event_code,
            "gulp.event.code": self.gulp_event_code,
            "event.duration": (
                self.duration_nsec if self.duration_nsec not in (None, 0) else 1
            ),
            "event.hash": self.hash,
            "event.original": self.original_event,
            "event.category": self.cat,
            "log.level": self.original_log_level,
            "gulp.log.level": self.gulp_log_level,
            "@timestamp": self.timestamp,
            "@timestamp_nsec": self.timestamp_nsec,
        }

        # remove None values
        d = {k: v for k, v in d.items() if v is not None}

        # add extra fields, if any: they will be added as is and will override the ones already set
        d.update({k: v for k, v in self.extra.items() if v is not None})
        return d

    @staticmethod
    def add_invalid_timestamp(d: dict) -> dict:
        """
        Adds an invalid timestamp to the given dictionary.
        NOTE: this should be investigated if unexpected (i.e. when data have a timestamp)
        """
        d["invalid_timestamp"] = True
        d["@timestamp"] = 0
        d["@timestamp_nsec"] = 0
        return d


class GulpQueryType(IntEnum):
    """Gulp rule types"""

    SIGMA_YAML = 1  # sigma rule YML
    # raw query dict
    RAW = 2
    GULP_FILTER = 3  # GULP filter
    INDEX = 4  # an index of a stored query


class GulpQueryParameter(BaseModel):
    """
    a sigma rule YML, elasticsearch DSL/Raw query or gulp filter.
    """

    type: GulpQueryType = Field(
        GulpQueryType.SIGMA_YAML,
        description="the type of source rule to converted (if needed) into an Elasticsearch DSL query.",
    )
    rule: Union[str, dict, int, Any] = Field(
        None,
        description="a rule according to type: str|SigmaRule for SIGMA_YAML (will be converted to RAW), dict for RAW (no conversion), dict(GulpQueryFilter) for GULP_FILTER, int for INDEX (an index of a stored query, no conversion).",
    )
    name: str = Field(
        None,
        description="the name of the query, mandatory for RAW, GULP_FILTER.",
    )
    tags: list[str] = Field(
        None,
        description="optional tags to set in the converted query, for RAW, GULP_FILTER, SIGMA_YAML.",
    )
    pysigma_plugin: Optional[str] = Field(
        None,
        description='the pysigma pipeline plugin to be used to transform a SIGMA_YAML rule into an elasticsearch query: if None, rule "logsource.product" is used as plugin name. if plugin loading fails, an empty pipeline is used.',
    )
    plugin_params: Optional[GulpPluginParams] = Field(
        None,
        description="optional parameters to be passed to the pysigma plugin pipeline() function (valid for SIGMA_YAML only).",
    )
    glyph_id: int = Field(None, description="the id of the associated glyph (if any).")

    model_config = {"json_schema_extra": EXAMPLE_SIGMA_RULE_YML}

    @staticmethod
    def from_dict(d: dict) -> "GulpQueryParameter":
        """
        Create a GulpQueryParameter object from a dictionary.

        Args:
            d (dict): The dictionary containing the parameter values.

        Returns:
            GulpQueryParameter: The created GulpQueryParameter object.
        """
        pspp = d.get("plugin_params", None)
        if pspp is not None:
            d["plugin_params"] = GulpPluginParams.from_dict(pspp)
        return GulpQueryParameter(**d)

    def to_dict(self) -> dict:
        """
        returns a dictionary representation of the rule.
        """
        d = {
            "type": self.type.value,
            "name": self.name,
            "tags": self.tags,
            "rule": self.rule,
            "pysigma_plugin": self.pysigma_plugin,
            "plugin_params": (
                self.plugin_params.to_dict() if self.plugin_params is not None else None
            ),
        }
        return muty.dict.clear_dict(d)

    def __repr__(self) -> str:
        return f"GulpQueryParameter(type={self.type}, rule={self.rule}, name={self.name}, tags={self.tags}, pysigma_plugin={self.pysigma_plugin}, plugin_params={self.plugin_params}, glyph_id={self.glyph_id})"


class GulpFieldsFilterType(StrEnum):
    """
    for filtering returned fields
    """

    ALL = "*"
    DEFAULT = "_id,@timestamp,operation_id,gulp.context,gulp.source.file,event.duration,gulp.event.code,event.code"


class GulpQueryOptions(BaseModel):
    """
    options for the query API (ordering, limit, skip).<br><br>

    if not specified, default is applied (sort by ascending @timestamp, limit=1000, full result returned).<br><br>

    "search_after" is to be used for pagination together with "limit" (read https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html)<br><br>

    basically, you have to set QueryOptions.search_after with the "search_after" returned in the PREVIOUS query to get another chunk.
    """

    limit: int = Field(
        1000,
        description="maximum number of results to return per chunk, streamed on the websocket (this cannot be more than 10000), default=1000",
    )
    search_after: list = Field(
        None,
        description='this must be set as the "search_after" returned from the PREVIOUS search, to get another chunk (used for pagination together with "limit", read https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html).',
    )
    sort: dict[str, SortOrder] = Field(
        {"@timestamp": "asc"},
        description="defines specific sort order for specific fields (default=sort by ascending @timestamp).",
    )
    fields_filter: Union[GulpFieldsFilterType, str] = Field(
        GulpFieldsFilterType.DEFAULT,
        description='a CSV list of fields to include in the result. special values are: "*"=all fields, "DEFAULT"=_id,@timestamp,gulp.context,gulp.source.file,event.duration,event.code,gulp.event.code',
    )
    disable_notes_on_match: bool = Field(
        False, description="disable automatic notes on query match (sigma-rules only)."
    )
    notes_on_match_color: str = Field(
        "green", description="color of the notes created on query match."
    )
    notes_on_match_glyph_id: int = Field(
        None, description="glyph ID for the notes created on query match."
    )
    include_query_in_result: bool = Field(
        False,
        description="include query in the result (for sigma-based query, also include the sigma rule text).",
    )
    all_tags_must_match: bool = Field(
        False,
        description="If True, when querying for tags a stored rules is selected ONLY if it have ALL of the tags. Either, just one tag match is enough for the rule to be selected. Defaults to False (just one tag is enough).",
    )

    # TODO: openapi_examples seems not working with multipart/form-data requests, so we put the example here instead of in the Annotation in rest_api.py
    model_config = {"json_schema_extra": EXAMPLE_QUERY_OPTIONS}

    @staticmethod
    def from_dict(d: dict) -> "GulpQueryOptions":
        """
        Create a GulpQueryOptions object from a dictionary.

        Args:
            d (dict): The dictionary containing the options.

        Returns:
            GulpQueryOptions: The created GulpQueryOptions object.
        """
        return GulpQueryOptions(**d)

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


def _query_string_add_or_clause(qs: str, field: str, values: list) -> str:
    qs = _query_string_init(qs)
    qs += "("
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


def _query_string_add_eq_clause(qs: str, field: str, v: int | str) -> str:
    qs = _query_string_init(qs)
    """
    if isinstance(v, str):
        # only enclose if there is a space in the value
        vv = muty.string.enclose(v) if " " in v else v
    else:
        vv = v
    """
    qs += f"{field}: {v}"
    return qs


def _query_string_add_gte_clause(qs: str, field: str, v: int | str) -> str:
    qs = _query_string_init(qs)
    qs += f"{field}: >={v}"
    return qs


def _query_string_add_lte_clause(qs: str, field: str, v: int | str) -> str:
    qs = _query_string_init(qs)
    qs += f"{field}: <={v}"
    return qs


def _query_string_add_exists_clause(qs: str, field: str, exist: bool) -> str:
    qs = _query_string_init(qs)
    if exist:
        qs += f"_exists_: {field}"
    else:
        qs += f"NOT _exists_: {field}"
    return qs


def _query_string_init(qs: str) -> str:
    if len(qs) > 0:
        # add an and clause
        qs += " AND "
    else:
        qs = ""
    return qs


def gulpqueryflt_dsl_dict_empty(d: dict) -> bool:
    """
    check if the filter is empty ('*' in the query_string query).
    """
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


def gulpqueryflt_to_dsl(flt: GulpQueryFilter = None) -> dict:
    """
    Converts a GulpQueryFilter object into an Elasticsearch DSL query.

    Args:
        flt (GulpQueryFilter, optional): The GulpQueryFilter object containing the filter parameters.

    Returns:
        dict: The Elasticsearch query dictionary.

    """

    if flt is None:
        # all
        qs = "*"
    else:
        # build the query string
        qs: str = ""
        if flt.plugin is not None and len(flt.plugin) > 0:
            qs = _query_string_add_or_clause(qs, "agent.type", flt.plugin)
        if flt.client_id is not None and len(flt.client_id) > 0:
            qs = _query_string_add_or_clause(qs, "agent.id", flt.client_id)
        if flt.operation_id is not None and len(flt.operation_id) > 0:
            qs = _query_string_add_or_clause(qs, "operation_id", flt.operation_id)
        if flt.context is not None and len(flt.context) > 0:
            qs = _query_string_add_or_clause(qs, "gulp.context", flt.context)
        if flt.event_id is not None and len(flt.event_id) > 0:
            qs = _query_string_add_or_clause(qs, "event.id", flt.event_id)
        if flt.elastic_id is not None and len(flt.elastic_id) > 0:
            qs = _query_string_add_or_clause(qs, "_id", flt.elastic_id)
        if flt.category is not None and len(flt.category) > 0:
            qs = _query_string_add_or_clause(qs, "event.category", flt.category)
        if flt.event_code is not None and len(flt.event_code) > 0:
            qs = _query_string_add_or_clause(qs, "event.code", flt.event_code)
        if flt.gulp_log_level is not None and len(flt.gulp_log_level) > 0:
            levels = [int(l) for l in flt.gulp_log_level]
            qs = _query_string_add_or_clause(qs, "log.level", levels)
        if flt.start_msec is not None and flt.start_msec > 0:
            qs = _query_string_add_gte_clause(qs, "@timestamp", flt.start_msec)
        if flt.end_msec is not None and flt.end_msec > 0:
            qs = _query_string_add_lte_clause(qs, "@timestamp", flt.end_msec)
        if flt.ev_hash is not None and len(flt.ev_hash) > 0:
            qs = _query_string_add_or_clause(qs, "event.hash", flt.ev_hash)
        if flt.raw is not None and len(flt.raw) > 0:
            qs = _query_string_add_eq_clause(qs, "event.original", flt.raw)
        if flt.src_file is not None and len(flt.src_file) > 0:
            qs = _query_string_add_or_clause(qs, "gulp.source.file", flt.src_file)
        if flt.extra is not None and len(flt.extra) > 0:
            for k, v in flt.extra.items():
                qs = _query_string_add_or_clause(qs, k, v)

    if len(qs) == 0:
        # all
        qs = "*"

    # default_field: _id below is an attempt to fix "field expansion matches too many fields"
    # https://discuss.elastic.co/t/no-detection-of-fields-in-query-string-query-strings-results-in-field-expansion-matches-too-many-fields/216137/2
    # (caused by "default_field" which by default is "*" and the query string is incorrectly parsed when parenthesis are used as we do, maybe this could be fixed in a later opensearch version as it is in elasticsearch)
    n = {"query": {"query_string": {"query": qs, "analyze_wildcard": True}}}
    qq = n["query"]["query_string"]
    if flt is None:
        # default
        qq["default_field"] = "_id"
    else:
        if flt.query_string_parameters is None:
            # default
            qq["default_field"] = "_id"
        else:
            qq.update(flt.query_string_parameters)
            if "default_field" not in qq:
                # apply default
                qq["default_field"] = "_id"

    # print('flt=%s, resulting query=%s' % (flt, json.dumps(n, indent=2)))
    return n


class GulpAPIContext(BaseModel):
    """
    Gulp API context.
    """

    operation_id: int = Field(
        None,
        description="The operation ID in the collab database to which the request belongs to.",
    )
    client_id: int = Field(
        None,
        description="ID of the client in the collab database who is issuing the request.",
    )
    user_id: int = Field(
        None,
        description="The user ID in the collab database of the user issuing the request.",
    )
    username: str = Field(
        None,
        description="The username of the user issuing the request.",
    )
    ws_id: str = Field(
        None,
        description="The websocket ID to stream data to",
    )
    req_id: str = Field(
        None,
        description="The request ID to identify the request",
    )

    @staticmethod
    def from_dict(d: dict) -> "GulpAPIContext":
        """
        Create a GulpAPIContext object from a dictionary.

        Args:
            d (dict): The dictionary containing the context attributes.

        Returns:
            GulpAPIContext: The created GulpAPIContext object.
        """
        return GulpAPIContext(**d)

    def to_dict(self) -> dict:
        """
        returns a dictionary representation of the context.
        """
        d = {
            "operation_id": self.operation_id,
            "client_id": self.client_id,
            "user_id": self.user_id,
            "username": self.username,
            "ws_id": self.ws_id,
            "req_id": self.req_id,
        }
        return d

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return GulpAPIContext().to_dict()

        if isinstance(data, dict):
            return data
        return json.loads(data)


