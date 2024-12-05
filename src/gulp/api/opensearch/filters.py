from enum import IntEnum, StrEnum
from typing import Optional, override

from pydantic import BaseModel, ConfigDict, Field
from muty.pydantic import autogenerate_model_example

from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_OPERATION_ID,
    TEST_SOURCE_ID,
)

# mandatory fields to be included in the result for queries
QUERY_DEFAULT_FIELDS = [
    "_id",
    "@timestamp",
    "event.duration",
    "event.code",
    "gulp.timestamp",
    "gulp.timestamp_invalid",
    "gulp.operation_id",
    "gulp.context_id",
    "gulp.source_id",
    "gulp.event_code",
]


class GulpBaseDocumentFilter(BaseModel):
    """
    base class for Gulp filters acting on documents.
    """

    model_config = ConfigDict(extra="allow")

    time_range: Optional[tuple[int | str, int | str, bool]] = Field(
        default=None,
        example=[1551385571023173120, 1551446406878338048, False],
        description="""
include documents matching `gulp.timestamp` in a time range [start, end], inclusive, in nanoseconds from unix epoch.
if the third element is True, [start, end] are strings and matching is against `@timestamp` string, according to [DSL docs about date ranges](https://opensearch.org/docs/latest/query-dsl/term/range/#date-fields).

**for ingestion filtering, `gulp.timestamp` is always used (match against nanoseconds) and the third element is ignored**.
        """,
    )

    query_string_parameters: Optional[dict] = Field(
        default=None,
        example={"analyze_wildcard": True, "default_field": "_id"},
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

    storage_ignore_filter: Optional[bool] = Field(
        False,
        description="on filtering during ingestion, websocket receives filtered results while OpenSearch stores all documents anyway (default=False=both OpenSearch and websocket receives the filtered results).",
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)

    @override
    def __str__(self) -> str:
        return super().__str__()

    @staticmethod
    def filter_doc_for_ingestion(
        doc: dict, flt: "GulpIngestionFilter" = None
    ) -> GulpDocumentFilterResult:
        """
        Check if a document is eligible for ingestion based on a time-range filter.

        Args:
            doc (dict): The GulpDocument dictionary to check.
            flt (GulpIngestionFilter): The filter parameters, if any.

        Returns:
            GulpEventFilterResult: The result of the filter check.
        """
        # MutyLogger.get_instance().error(flt)
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

    agent_type: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `agent.type`/s.",
        example=["win_evtx"],
    )
    id: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `_id`/s.",
        example=["18b6332595d82048e31963e6960031a1"],
    )
    operation_id: Optional[list[str]] = Field(
        None,
        description="include documents  matching the given `gulp.operation_id`/s.",
        example=[TEST_OPERATION_ID],
    )
    context_id: Optional[list[str]] = Field(
        None,
        description="""
include documents matching the given `gulp.context_id`/s.

- this must be set to the *real context_id* as on the collab database, calculated as *SHA1(operation_id+context_id)*.
""",
        example=[TEST_CONTEXT_ID],
    )
    source_id: Optional[list[str]] = Field(
        None,
        description="""
include documents matching the given `gulp.source_id`/s.
- this must be set to the *real source_id* as on the collab database, calculated as *SHA1(operation_id+context_id+source_id)*.
""",
        example=[TEST_SOURCE_ID],
    )
    event_code: Optional[list[str]] = Field(
        None,
        description="include documents matching the given `event.code`/s.",
        example=["5152"],
    )
    event_original: Optional[tuple[str, bool]] = Field(
        None,
        description="""include documents matching the given `event.original`.

        if the second element is `True`, perform a full [text](https://opensearch.org/docs/latest/field-types/supported-field-types/text/) search on `event.original` field.
        if the second element is `False`, uses [the default keyword search](https://opensearch.org/docs/latest/field-types/supported-field-types/keyword/).
        """,
        example=["*searchme*", False],
    )

    @override
    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        return autogenerate_model_example(cls, *args, **kwargs)

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

    def to_opensearch_dsl(self, flt: "GulpQueryFilter" = None) -> dict:
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
                        "query": "agent.type: \"winlogbeat\" AND gulp.operation_id: \"test\" AND gulp.context_id: \"testcontext\" AND gulp.source_id: \"test.log\" AND _id: \"testid\" AND event.original: \"test event\" AND event.code: \"5152\" AND @timestamp: >=1609459200000 AND @timestamp: <=1609545600000",
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
                clauses.append(
                    self._query_string_build_or_clauses("agent.type", self.agent_type)
                )
            if self.operation_id:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.operation_id", self.operation_id
                    )
                )
            if self.context_id:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.context_id", self.context_id
                    )
                )
            if self.source_id:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.source_id", self.source_id
                    )
                )
            if self.id:
                clauses.append(self._query_string_build_or_clauses("_id", self.id))
            if self.event_original:
                # check for full text search or keyword search
                event_original = self.event_original[0]
                fts = event_original[1] or False
                field = "event.original.text" if fts else "event.original"
                clauses.append(
                    self._query_string_build_eq_clause(field, event_original)
                )
            if self.event_code:
                clauses.append(
                    self._query_string_build_or_clauses("event.code", self.event_code)
                )
            if self.time_range:
                # use string or numeric field depending on the third element (default to numeric)
                timestamp_field = (
                    "@timestamp" if self.time_range[2] else "gulp.timestamp"
                )
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
                            flt.to_opensearch_dsl()["query"],
                            query_dict["query"],
                        ]
                    }
                }
            }

        # MutyLogger.get_instance().debug('flt=%s, resulting query=%s' % (flt, json.dumps(query_dict, indent=2)))
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
        return not any(
            [
                self.agent_type,
                self.id,
                self.operation_id,
                self.context_id,
                self.source_id,
                self.event_code,
                self.event_original,
                self.time_range,
                self.model_extra,
            ]
        )


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"
