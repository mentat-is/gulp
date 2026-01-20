"""Gulp OpenSearch data structures.

This module defines the data structures for representing Gulp documents in OpenSearch.
Key classes include:

- GulpBasicDocument: Base model for Gulp documents with essential fields such as ID,
    timestamp, operation, context, and source information.

- GulpDocument: Extended document model with additional fields for event data,
    providing methods for initialization and timestamp validation.

- GulpDocumentFieldAliasHelper: Helper class for managing field aliases in document properties.

These structures form the foundation for document handling in the Gulp OpenSearch API.
"""

from typing import Annotated, Any, Optional, TypeVar, override

import muty.crypto
import muty.string
import muty.time
from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field

from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.filters import QUERY_DEFAULT_FIELDS, GulpBaseDocumentFilter
from gulp.structs import GulpPluginParameters, GulpSortOrder

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

    id: Annotated[
        str,
        Field(
            description='"_id": the unique identifier of the document.',
            alias="_id",
        ),
    ]
    timestamp: Annotated[
        str,
        Field(
            description='"@timestamp": document timestamp, in iso8601 format.',
            alias="@timestamp",
        ),
    ]
    gulp_timestamp: Annotated[
        int,
        Field(
            description='"@timestamp": document timestamp in nanoseconds from unix epoch.',
            alias="gulp.timestamp",
        ),
    ]
    invalid_timestamp: Annotated[
        bool,
        Field(
            description='True if "@timestamp" is invalid and set to 1/1/1970 (the document should be checked, probably ...).',
            alias="gulp.timestamp_invalid",
        ),
    ] = False
    operation_id: Annotated[
        str,
        Field(
            description='"gulp.operation_id": the operation ID the document is associated with.',
            alias="gulp.operation_id",
        ),
    ]
    context_id: Annotated[
        str,
        Field(
            description='"gulp.context_id": the context (i.e. an host name) the document is associated with.',
            alias="gulp.context_id",
        ),
    ]
    source_id: Annotated[
        str,
        Field(
            description='"gulp.source_id": the source the document is associated with.',
            alias="gulp.source_id",
        ),
    ]


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

    log_file_path: Annotated[
        Optional[str],
        Field(
            description='"log.file.path": the original log file name or path.',
            alias="log.file.path",
        ),
    ] = None
    agent_type: Annotated[
        str,
        Field(
            description='"agent.type": the ingestion source, i.e. gulp plugin.name().',
            alias="agent.type",
        ),
    ] = None
    event_original: Annotated[
        str,
        Field(
            description='"event.original": the original event as text.',
            alias="event.original",
        ),
    ] = None
    event_sequence: Annotated[
        int,
        Field(
            description='"event.sequence": the sequence number of the document in the source.',
            alias="event.sequence",
        ),
    ] = 0
    event_code: Annotated[
        str,
        Field(
            description='"event.code": the event code, "0" if missing.',
            alias="event.code",
        ),
    ] = "0"
    gulp_event_code: Annotated[
        int,
        Field(
            description='"gulp.event_code": "event.code" as integer.',
            alias="gulp.event_code",
        ),
    ] = 0
    event_duration: Annotated[
        int,
        Field(
            description='"event.duration": the duration of the event in nanoseconds, defaults to 1.',
            alias="event.duration",
        ),
    ] = 1

    
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
            ns = muty.time.string_to_nanos_from_unix_epoch(timestamp)

            # timestamp is epoch or before, that's usually a sign of an invalid timestamp
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

        # severe log file contains duration values in float format (fractional seconds),
        # this constructor converts them to nanoseconds for consistency in storage and processing.
        if "event_duration" in kwargs and isinstance(kwargs["event_duration"], float):
            value_to_change = kwargs["event_duration"]
            s, us = divmod(float(value_to_change), 1.0)
            kwargs["event_duration"] = int(s) * 1_000_000_000 + round(
                us * 1_000_000_000
            )
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
                else plugin_instance.name
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

        # add gulp_event_code (event code as a number), try to find it in cache first
        from gulp.plugin import DocValueCache        
        evc = data["event_code"]
        gulp_evc: int = plugin_instance.doc_value_cache.get_value(evc)
        if gulp_evc:
            # cache hit
            data["gulp_event_code"] = gulp_evc
        else:
            # cache miss
            gulp_evc = (
                int(data["event_code"])
                if data["event_code"].isnumeric()
                else muty.crypto.hash_xxh64_int(data["event_code"])
            )
            plugin_instance.doc_value_cache.set_value(evc, gulp_evc)
            data["gulp_event_code"] = gulp_evc

        # id is a hash of the document
        event_sequence = data.get("event_sequence", 0)
        data["id"] = muty.crypto.hash_xxh128(
            f"{data['event_original']}{data['event_code']}{data['operation_id']}{data['context_id']}{data['source_id']}{event_sequence}{ts_nanos}"
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


class GulpQueryParameters(BaseModel):
    """
    additional options for a query.

    NOTE: when using with external queries, not all options are guaranteed to be implemented (it is the plugin responsibility to handle them)
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "sort": {
                        "@timestamp": GulpSortOrder.ASC,
                        "_doc": GulpSortOrder.ASC,
                        "event.sequence": GulpSortOrder.ASC,
                    },
                    "fields": ["@timestamp", "event.id"],
                    "limit": 1000,
                    "total_limit": 0,
                    "group": "test",
                    "preview_mode": False,
                    "search_after": None,
                    "highlight_results": True,
                }
            ]
        },
    )
    name: Annotated[
        str,
        Field(
            description="the name of the query, used as tag for notes if `create_notes` is set. this is autogenerated if not set, for sigma queries it is set to rule.name (or rule.title or rule.id), depending on what is available.",
        ),
    ] = None
    group: Annotated[
        str,
        Field(
            description="the query group, if any: if set, `WSDATA_QUERY_GROUP_MATCH` with payload=`GulpQueryGroupMatchPacket` is sent on the websocket if all queries in the same request matches.",
        ),
    ] = None
    group_glyph_id: Annotated[
        Optional[str],
        Field(
            description="the query group glyph, if any: `GulpQueryGroupMatchPacket.glyph_id` in `WSDATA_QUERY_GROUP_MATCH` and notes generated by **matched** queries in this group (if `create_notes` is set) will use this glyph id",
        ),
    ] = None
    group_color: Annotated[
        Optional[str],
        Field(
            description="the query group color, if any: `GulpQueryGroupMatchPacket.color `WSDATA_QUERY_GROUP_MATCH` and notes generated by **matched** queries in this group (if `create_notes` is set) will use this color",
        ),
    ] = None
    sort: Annotated[
        dict[str, GulpSortOrder],
        Field(
            description="""
how to sort results, default=sort by ascending `@timestamp`.

- for `external` queries, its the plugin responsibility to handle this.""",
        ),
    ] = {}
    fields: Annotated[
        list[str] | str,
        Field(
            description="""
the set of fields to include in the returned documents.

- for `external` queries, the plugin should ignore this and always return all fields
- default=`%s`, use `*` to return all fields.
"""
            % (QUERY_DEFAULT_FIELDS),
        ),
    ] = None
    ensure_default_fields: Annotated[
        bool,
        Field(
            description="""
if set and `fields` is set, ensure the default fields (%s) are included in the returned documents (default=True).

- for `external` queries, its the plugin responsibility to handle this."""
            % (QUERY_DEFAULT_FIELDS),
        ),
    ] = True
    limit: Annotated[
        int,
        Field(
            ge=1,
            le=10000,
            description="""
for pagination, the maximum number of documents to return **per chunk**, default=1000 (None=return up to 10000 documents per chunk).

- for `external` queries, its the plugin responsibility to handle this.""",
        ),
    ] = 1000
    total_limit: Annotated[
        int,
        Field(
            description="""
The maximum number of documents to return in total, default=0 (no limit).

NOTE: as documents are returned in chunk of `limit` size, total is intended as a multiple of it. default=0 (no limit).
""",
        ),
    ] = 0
    search_after: Annotated[
        list[dict],
        Field(
            description="""
for pagination, this should be set to the `search_after` returned by the previous call.

- check [OpenSearch documentation](https://opensearch.org/docs/latest/search-plugins/searching-data/paginate/#the-search_after-parameter).
- for `external` queries, this may not be supported (loop handling is responsibility of the plugin).
""",
        ),
    ] = None
    preview_mode: Annotated[
        bool,
        Field(
            description="""
if set, the query is **synchronous** and returns the preview chunk of documents, without streaming data on the websocket nor counting data in the stats.
""",
        ),
    ] = False
    create_notes: Annotated[
        bool,
        Field(
            description="""
if set, create notes on match with title=query name, tags=["auto"] (plus sigma rule tags if the query comes from a sigma rule)""",
        ),
    ] = False
    notes_color: Annotated[
        Optional[str],
        Field(
            description="""
the color to use for notes created from this query (if `create_notes` is set).
""",
        ),
    ] = None
    notes_glyph_id: Annotated[
        Optional[str],
        Field(
            description="""
the glyph ID to use for notes created from this query (if `create_notes` is set).
""",
        ),
    ] = None
    highlight_results: Annotated[
        bool,
        Field(
            description="""
if set, highlights are included in the results (default=False).
- this is valid only for local queries to Gulp (including sigma queries), it is ignored for `external` queries.
- may need adjustment to OpenSearch configuration if causing heap exhaustion errors.
""",
        ),
    ] = False
    add_to_history: Annotated[
        bool,
        Field(
            description="if set, add this/these queries to the query history for the calling user. Default is False."
        ),
    ] = False

    force_ignore_missing_ws: Annotated[
        bool,
        Field(
            description="if set, disconnecting client websocket does not stop query/ies processing."
        ),
    ] = False
    def __init__(self, **data: Any) -> None:
        if "name" not in data or not data["name"]:
            # autogenerate name
            data["name"] = "query_%s" % (muty.string.generate_unique())
        super().__init__(**data)

    def parse(self) -> dict:
        """
        Parse the additional options to a dictionary for the OpenSearch/Elasticsearch search api.

        Returns:
            dict: The parsed dictionary.
        """
        n = {}

        # sorting
        n["sort"] = []
        if not self.sort:
            # default sort
            sort = {
                "@timestamp": GulpSortOrder.ASC.value,
                # read the NOTE below...
                "_doc": GulpSortOrder.ASC.value,
                "event.sequence": GulpSortOrder.ASC.value,
            }

        else:
            # use provided
            sort = self.sort

        for k, v in sort.items():
            n["sort"].append({k: {"order": v}})
            # NOTE: test with VERY VERY large datasets (5M+), and consider to remove "_doc" here this since it may not be needed after all.... event.sequence should be enough.
            if "_doc" not in sort:
                # make sure document order is always sorted, use _doc instead of _id for less overhead (CircuitBreakingException error from opensearch)
                n["sort"].append({"_doc": {"order": v}})
            if "event.sequence" not in sort:
                # make sure event.sequence is always sorted
                n["sort"].append({"event.sequence": {"order": v}})

        # fields to be returned
        if not self.fields:
            # default, if not set
            fields = QUERY_DEFAULT_FIELDS
        else:
            # use the given set
            fields = self.fields

        # n["_source"] = None
        if fields != "*":
            # if "*", return all (so we do not set "_source"). either, only return these fields
            if self.ensure_default_fields:
                # ensure default fields are included
                for f in QUERY_DEFAULT_FIELDS:
                    if f not in fields:
                        fields.append(f)
            n["_source"] = fields

        # pagination: doc limit
        if self.limit:
            # use provided
            n["size"] = self.limit

        # pagination: start from
        if self.search_after:
            # next chunk from this point
            n["search_after"] = self.search_after

        # wether to highlight results for the query (warning: may take a lot of memory)
        if self.highlight_results:
            n["highlight"] = {"fields": {"*": {}}}
        # MutyLogger.get_instance().debug("query options: %s" % (orjson.dumps(n, option=orjson.OPT_INDENT_2).decode()))
        return n


class GulpQueryHelpers:
    """
    helpers to perform queries
    """

    @staticmethod
    def merge_queries(q1: dict, q2: dict) -> dict:
        """
        merge two queries into one.

        Args:
            q1 (dict): the first query
            q2 (dict): the second query

        Returns:
            dict: the merged query
        """
        # handle empty queries
        if not q1:
            return q2
        if not q2:
            return q1

        # merge both queries into a bool filter
        d: dict = dict(query={"bool": {"filter": [q1["query"], q2["query"]]}})
        return d


class GulpQuery(BaseModel):
    """
    A query (may be sigma yml or opensearch lucene raw)
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "test",
                    "id": "test",
                    "q": {"query": {"match_all": {}}},
                    "tags": ["test"],
                    "q_group": "test_group",
                }
            ]
        }
    )
    q: Annotated[Any, Field(..., description="the query in the target DSL format.")]

    q_name: Annotated[
        str, Field(description="the name/title of the query, autogenerated if not set.")
    ] = None
    sigma_yml: Annotated[
        Optional[str],
        Field(description="if this is a sigma rule, the query in YAML format."),
    ] = None
    sigma_id: Annotated[
        Optional[str],
        Field(None, description="the id of the sigma rule, if this is a sigma query."),
    ] = None
    tags: Annotated[list[str], Field(description="query tags.")] = []
    q_group: Annotated[
        Optional[str], Field(description="the group this query belongs to, if any.")
    ] = None

    def __init__(
        self,
        q: Any,
        q_name: str = None,
        sigma_yml: str = None,
        sigma_id: str = None,
        tags: list[str] = None,
        q_group: str = None,
    ) -> None:
        if not q_name:
            # autogenerate name
            self.q_name = "query_%s" % (muty.string.generate_unique())
        super().__init__(
            q=q,
            q_name=q_name,
            sigma_yml=sigma_yml,
            sigma_id=sigma_id,
            tags=tags or [],
            q_group=q_group,
        )
