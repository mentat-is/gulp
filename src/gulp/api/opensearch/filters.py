from enum import IntEnum, StrEnum
from pydantic import BaseModel, Field


from typing import Optional, override


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

class GulpBaseDocumentFilter(BaseModel):
    """
    base class for Gulp filters acting on documents.
    """
    class Config:
        extra = "allow"
    time_range: Optional[tuple[int|str, int|str, bool]] = Field(
        None,
        description="include documents matching `gulp.timestamp` in a time range [start, end], inclusive, in nanoseconds from unix epoch.<br>"
            "if the third element is True, [start, end] are strings and matching is against `@timestamp` string, according to [DSL docs about date ranges](https://opensearch.org/docs/latest/query-dsl/term/range/#date-fields).<br><br>"
            "**for ingestion filtering, `gulp.timestamp` is always used and the third element is ignored**.",
    )

    query_string_parameters: Optional[dict] = Field(
        None,
        description="additional parameters to be applied to the resulting `query_string` query, according to [opensearch documentation](https://opensearch.org/docs/latest/query-dsl/full-text/query-string)",
    )

    @override
    def __str__(self) -> str:
        return self.model_dump_json(exclude_none=True)


class GulpDocumentFilterResult(IntEnum):
    """wether if the event should be accepted or skipped during ingestion."""

    ACCEPT = 0
    SKIP = 1


class GulpIngestionFilter(GulpBaseDocumentFilter):
    """
    a GulpIngestionFilter defines a filter for the ingestion API.<br><br>

    each field is optional, if no filter is specified all events are ingested.
    """

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "time_range": [1609459200000, 1609545600000, False],
                    "storage_ignore_filter": False,
                }
            ]
        }
    }

    storage_ignore_filter: Optional[bool] = Field(
        False,
        description="on filtering during ingestion, websocket receives filtered results while OpenSearch stores all documents anyway (default=False=both OpenSearch and websocket receives the filtered results).",
    )

    @override
    def __str__(self) -> str:
        return super().__str__()

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
        # GulpLogger.get_instance().error(flt)
        if not flt or flt.storage_ignore_filter:
            # empty filter or ignore
            return GulpDocumentFilterResult.ACCEPT

        if flt.time_range:
            ts = doc["gulp.timestamp"]
            if ts <= flt.time_range[0] or ts >= flt.time_range[1]:
                return GulpDocumentFilterResult.SKIP

        return GulpDocumentFilterResult.ACCEPT


class GulpQueryFilter(GulpBaseDocumentFilter):
    """
    a GulpQueryFilter defines a filter for the query API.

    further key,value pairs are allowed and will be used as additional filters.
    """

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "time_range": [1609459200000, 1609545600000, False],
                    "agent_type": ["winlogbeat"],
                    "operation": ["test"],
                    "context": ["testcontext"],
                    "log_file_path": ["test.log"],
                    "event_original": ["test event", True],
                    "event_code": ["5152"],
                }
            ]
        }
    }

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
    event_original: Optional[tuple[str,bool]] = Field(
        (None, False),
        description="include documents matching the given `event.original`/s.<br><br>"
        "if the second element is True, perform a full [text](https://opensearch.org/docs/latest/field-types/supported-field-types/text/) search on `event.original` field.<br>"
        "if the second element is False, uses [the default keyword search](https://opensearch.org/docs/latest/field-types/supported-field-types/keyword/).",
    )

    @override
    def __str__(self) -> str:
        return super().__str__()

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

    def to_opensearch_dsl(self, flt: "GulpQueryFilter"=None) -> dict:
        """
        convert to a query in OpenSearch DSL format using [query_string](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query

        Args:
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
                # check for full text search or keyword search
                event_original = self.event_original[0]
                fts = event_original[1] or False
                field = "event.original.text" if fts else "event.original"
                clauses.append(self._query_string_build_eq_clause(field, event_original))
            if self.event_code:
                clauses.append(self._query_string_build_or_clauses("event.code", self.event_code))
            if self.time_range:
                # use string or numeric field depending on the third element (default to numeric)
                timestamp_field = "@timestamp" if self.time_range[2] else "gulp.timestamp"
                if self.time_range[0]:
                    clauses.append(self._query_string_build_gte_clause(timestamp_field, self.time_range[0]))
                if self.time_range[1]:
                    clauses.append(self._query_string_build_lte_clause(timestamp_field, self.time_range[1]))
            if self.model_extra:
                # extra fields
                for k, v in self.model_extra.items():
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

        if qs != "*" and self.query_string_parameters:
            query_dict["query"]["query_string"].update(self.query_string_parameters)

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


        # GulpLogger.get_instance().debug('flt=%s, resulting query=%s' % (flt, json.dumps(query_dict, indent=2)))
        return query_dict

    def merge_to_opensearch_dsl(self, dsl: dict) -> dict:
        """
        merge the filter with an existing OpenSearch DSL query.

        Args:
            dsl (dict): the existing OpenSearch DSL query.
        Returns:
            dict: the merged query.
        """
        return {
            "query": {
                "bool": {
                    "filter": [
                        self.to_opensearch_dsl()["query"],
                        dsl["query"],
                    ]
                }
            }
        }

    def is_empty(self) -> bool:
        """
        Check if the filter is empty.

        Returns:
            bool: True if the filter is empty, False otherwise.
        """
        return not any([self.agent_type,
                        self.id,
                        self.operation,
                        self.context,
                        self.log_file_path,
                        self.event_code,
                        self.event_original,
                        self.time_range,
                        self.model_extra
                        ])


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"


class GulpQueryAdditionalOptions(BaseModel):
    """
    additional options for a query.
    """
    sort: Optional[dict[str, GulpSortOrder]] = Field(
        default={"@timestamp": "asc", "_id": "asc", "event.sequence": "asc"},
        max_length=1,
        description="how to sort results, default=sort by ascending `@timestamp`.",
    )
    fields: Optional[list[str]] = Field(
        default=QUERY_DEFAULT_FIELDS,
        description="the set of fields to include in the returned documents.<br>"
        "default=`%s` (which are forcefully included anyway), use `None` to return all fields."
        % (QUERY_DEFAULT_FIELDS),
    )
    limit: Optional[int] = Field(
        1000,
        gt=1,
        le=10000,
        description="for pagination, the maximum number of documents to return in a chunk, default=1000 (None=return up to 10000 documents).",
    )
    search_after: Optional[list[int|str]] = Field(
        None,
        description="to use pagination driven by the client: this is the last value returned as `search_after` from the previous query, to be used as start offset. Ignored if `loop` is set.",
    )
    loop: Optional[bool] = Field(
        True,
        description="if set, keep querying until all documents are returned (default=True, ignores `search_after`).",
    )
    sigma_create_note: Optional[bool] = Field(
        False,
        description="if set in a sigma query, annotate each document returned on the collab database (default=False).",
    )
    sigma_note_color: Optional[str] = Field(
        None,
        description="the color of the note created for each document returned by a sigma query (default=use default for notes).",
    )
    sigma_note_glyph: Optional[str] = Field(
        None,
        description="the glyph of the note created for each document returned by a sigma query (default=use default for notes).",
    )
    def parse(self) -> dict:
        """
        Parse the additional options to a dictionary for the OpenSearch/Elasticsearch search api.

        Returns:
            dict: The parsed dictionary.
        """
        n = {}

        # sorting
        n["sort"] = []
        for k, v in self.sort.items():
            n["sort"].append({k: {"order": v}})
            # NOTE: this was "event.hash" before: i removed it since its values is the same as _id now, so put _id here.
            # if problems (i.e. issues with sorting on _id), we can add it back just by duplicating _id 
            if "_id" not in self.sort:
                n["sort"].append({"_id": {"order": v}})
            if "event.sequence" not in self.sort:
                n["sort"].append({"event.sequence": {"order": v}})

        # fields to be returned
        if self.fields:
            # only return these fields (must always include the defaults)
            for f in QUERY_DEFAULT_FIELDS:
                if f not in self.fields:
                    self.fields.append(f)
            n["_source"] = self.fields

        # pagination: doc limit
        if self.limit is not None:
            # use provided
            n["size"] = self.limit

        # pagination: start from
        if self.search_after:
            # next chunk from this point
            n["search_after"] = self.search_after
        else:
            n["search_after"] = None

        # GulpLogger.get_instance().debug("query options: %s" % (json.dumps(n, indent=2)))
        return n


