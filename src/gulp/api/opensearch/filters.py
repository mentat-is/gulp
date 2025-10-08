"""
This module provides filtering functionality for OpenSearch queries in the Gulp API.

It defines classes for filtering documents during ingestion and queries, with support
for various criteria such as time ranges, document IDs, operation IDs, etc.

The main classes include:
- GulpBaseDocumentFilter: Base class for all document filters
- GulpDocumentFilterResult: Enum for filter results (ACCEPT/SKIP)
- GulpIngestionFilter: Filter for document ingestion
- GulpQueryFilter: Filter for document queries with OpenSearch DSL conversions

The module also defines constants like QUERY_DEFAULT_FIELDS which specifies mandatory
fields to include in query results.

"""

from enum import IntEnum
from typing import Optional, override, Annotated
import orjson
from pydantic import BaseModel, ConfigDict, Field

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

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "time_range": [
                        1551385571023173120,
                        1551446406878338048,
                    ],
                    "query_string_parameters": {
                        "analyze_wildcard": True,
                        "default_field": "_id",
                    },
                }
            ]
        },
    )

    time_range: Annotated[
        tuple[int, int],
        Field(
            default=None,
            description="""
a tuple representing a `gulp.timestamp` range `[ start, end ]`.

- `start` and `end` are nanoseconds from the unix epoch.
""",
        ),
    ] = None

    query_string_parameters: Annotated[
        Optional[dict],
        Field(
            description="""
additional parameters to be applied to the resulting `query_string` query, according to [opensearch documentation](https://opensearch.org/docs/latest/query-dsl/full-text/query-string)

""",
        ),
    ] = None

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

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "time_range": [
                        1551385571023173120,
                        1551446406878338048,
                    ],
                    "storage_ignore_filter": False,
                }
            ]
        }
    )
    storage_ignore_filter: Annotated[
        bool,
        Field(
            description="""
if set, websocket receives filtered results while OpenSearch stores unfiltered (=all) documents.
default is False (both OpenSearch and websocket receives the filtered results).
""",
        ),
    ] = False

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
        if not flt.time_range:
            # no time range, accept all
            return GulpDocumentFilterResult.ACCEPT

        # filter based on time range
        # check if ts is within the range. either start or end can be None
        # if both are None, the filter is empty and all events are accepted
        ts = doc["gulp.timestamp"]
        if flt.time_range[0] and flt.time_range[1]:
            if ts >= flt.time_range[0] and ts <= flt.time_range[1]:
                return GulpDocumentFilterResult.ACCEPT
        if flt.time_range[0]:
            if ts >= flt.time_range[0]:
                return GulpDocumentFilterResult.ACCEPT
        if flt.time_range[1]:
            if ts <= flt.time_range[1]:
                return GulpDocumentFilterResult.ACCEPT

        return GulpDocumentFilterResult.SKIP


class GulpQueryFilter(GulpBaseDocumentFilter):
    """
    a GulpQueryFilter defines a filter for the query API.

    - query is built using [query_string](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query.
    - further extra key=value pairs are allowed and are intended as k: list[str]|str:
        if it is a list of values, an OR clause is built, otherwise an equality clause is built, i.e.
        - `{"event.code": ["5152", "5156"]}` becomes `(event.code: 5152 OR event.code: 5156)`
        - `{"event.code": "5152"}` becomes `event.code: 5152`
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "agent_types": ["win_evtx"],
                    "operation_ids": ["test_operation"],
                    "context_ids": ["66d98ed55d92b6b7382ffc77df70eda37a6efaa1"],
                    "source_ids": ["fa144510fd16cf5ffbaeec79d68b593f3ba7e7e0"],
                    "doc_ids": ["d0739e61e3566845838fd78012b8201d"],
                    "event_codes": ["5152"],
                    "time_range": [
                        1551385571023173120,
                        1551446406878338048,
                    ],
                    "storage_ignore_filter": False,
                }
            ]
        }
    )
    agent_types: Annotated[
        list[str],
        Field(
            description="include documents matching the given `agent.type`/s.",
        ),
    ] = None
    doc_ids: Annotated[
        list[str],
        Field(
            description="include documents matching the given `_id`/s.",
        ),
    ] = None
    operation_ids: Annotated[
        list[str],
        Field(
            description="include documents  matching the given `gulp.operation_id`/s",
        ),
    ] = None
    context_ids: Annotated[
        list[str],
        Field(
            description="""
include documents matching the given `gulp.context_id`/s.

- this must be set to the *real context_id* as on the collab database, calculated as *SHA1(operation_id+context_id)*.
""",
        ),
    ] = None
    source_ids: Annotated[
        list[str],
        Field(
            description="""
include documents matching the given `gulp.source_id`/s.
- this must be set to the *real source_id* as on the collab database, calculated as *SHA1(operation_id+context_id+source_id)*.
""",
        ),
    ] = None
    event_codes: Annotated[
        list[str],
        Field(
            description="include documents matching the given `event.code`/s.",
        ),
    ] = None

    @override
    def __str__(self) -> str:
        return super().__str__()

    def _query_string_build_or_clauses(self, field: str, values: list) -> str:
        if not values:
            return ""

        qs = "("
        for v in values:
            # if isinstance(v, str):
            #     # only enclose if there is a space in the value
            #     vv = muty.string.enclose(v) if " " in v else v
            # else:
            #     vv = v

            qs += f"{field}: {v} OR "

        qs = qs[:-4]  # remove last " OR "
        qs += ")"
        return qs

    def _query_string_build_eq_clause(self, field: str, v: int | str) -> str:
        qs = f"{field}: {v}"
        return qs

    def _query_string_build_gte_clause(self, field: str, v: int) -> str:
        qs = f"{field}: >={v}"
        return qs

    def _query_string_build_lte_clause(self, field: str, v: int) -> str:
        qs = f"{field}: <={v}"
        return qs

    def _query_string_build_exists_clause(self, field: str, exist: bool) -> str:
        if exist:
            qs = f"_exists_: {field}"
        else:
            qs = f"NOT _exists_: {field}"
        return qs

    def to_opensearch_dsl(self) -> dict:
        """
        convert to a query in OpenSearch DSL format using [query_string](https://opensearch.org/docs/latest/query-dsl/full-text/query-string/) query

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
            # disable not-an-iterable and non-subscribtable:
            # checks are in place and the pydantic model enforces the type
            # pylint: disable=E1133,E1136
            clauses: list[str] = []

            if self.agent_types:
                clauses.append(
                    self._query_string_build_or_clauses("agent.type", self.agent_types)
                )
            if self.operation_ids:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.operation_id", self.operation_ids
                    )
                )
            if self.context_ids:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.context_id", self.context_ids
                    )
                )
            if self.source_ids:
                clauses.append(
                    self._query_string_build_or_clauses(
                        "gulp.source_id", self.source_ids
                    )
                )
            if self.doc_ids:
                clauses.append(self._query_string_build_or_clauses("_id", self.doc_ids))

            if self.event_codes:
                clauses.append(
                    self._query_string_build_or_clauses("event.code", self.event_codes)
                )
            if self.time_range:
                # simple >=, <= clauses
                field = "gulp.timestamp"
                if self.time_range[0]:
                    clauses.append(
                        self._query_string_build_gte_clause(field, self.time_range[0])
                    )
                if self.time_range[1]:
                    clauses.append(
                        self._query_string_build_lte_clause(field, self.time_range[1])
                    )
            if self.model_extra:
                # extra fields
                for k, v in self.model_extra.items():
                    if isinstance(v, list):
                        # OR clauses
                        clauses.append(self._query_string_build_or_clauses(k, v))
                    else:
                        # equality
                        clauses.append(self._query_string_build_eq_clause(k, v))

            # only return non-empty clauses
            clauses = [c for c in clauses if c and c.strip()]
            # print(clauses)
            return clauses

        # build the query struct
        #
        # NOTE: default_field: _id below is an attempt to fix "field expansion matches too many fields"
        # https://discuss.elastic.co/t/no-detection-of-fields-in-query-string-query-strings-results-in-field-expansion-matches-too-many-fields/216137/2
        # (caused by "default_field" which by default is "*" and the query string is incorrectly parsed when parenthesis are used as we do, maybe this could be fixed in a later opensearch version as it is in elasticsearch)
        query_dict = {
            "query": {
                "query_string": {
                    # all clauses are ANDed, if none return all
                    "query": " AND ".join(filter(None, _build_clauses())) or "*",
                    "analyze_wildcard": True,
                    "default_field": "_id",
                }
            }
        }

        q_string = query_dict["query"]["query_string"]
        if self.query_string_parameters:
            # add provided parameters
            q_string.update(self.query_string_parameters)

        # MutyLogger.get_instance().debug('resulting query=%s' % (orjson.dumps(query_dict, option=orjson.OPT_INDENT_2).decode()))
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
                self.time_range,
                self.agent_types,
                self.operation_ids,
                self.context_ids,
                self.source_ids,
                self.event_codes,
                self.doc_ids,
                self.model_extra,
            ]
        )
