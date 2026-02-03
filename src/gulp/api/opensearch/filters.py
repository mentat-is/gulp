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
from typing import Annotated, Optional, override

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
                    ]
                }
            ]
        },
    )

    time_range: Annotated[
        Optional[tuple[int, int]],
        Field(
            description="""
a tuple representing a `gulp.timestamp` range `[ start, end ]`.

- `start` and `end` are nanoseconds from the unix epoch.
- to set only start (or end), use 0 for end (or start), e.g. `[ start, 0 ]` or `[ 0, end ]`.
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
        if not flt.time_range or len (flt.time_range) != 2:
            # no time range, accept all
            return GulpDocumentFilterResult.ACCEPT
        if flt.time_range[0] == 0 and flt.time_range[1] == 0:
            # empty time range, accept all
            return GulpDocumentFilterResult.ACCEPT
        
        # filter based on time range
        # check if ts is within the range. either start or end can be None
        # if both are None, the filter is empty and all events are accepted
        ts = doc["gulp.timestamp"]

        if flt.time_range[0] > 0 and flt.time_range[1] > 0:
            if ts >= flt.time_range[0] and ts <= flt.time_range[1]:
                return GulpDocumentFilterResult.ACCEPT
        if flt.time_range[0] > 0:
            if ts >= flt.time_range[0]:
                return GulpDocumentFilterResult.ACCEPT
        if flt.time_range[1] > 0:
            if ts <= flt.time_range[1]:
                return GulpDocumentFilterResult.ACCEPT

        return GulpDocumentFilterResult.SKIP


class GulpQueryFilter(GulpBaseDocumentFilter):
    """
    a GulpQueryFilter defines a filter for the query API.

    - query is built using a filtered bool query with terms/range filters.
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
    ] = []
    doc_ids: Annotated[
        list[str],
        Field(
            description="include documents matching the given `_id`/s.",
        ),
    ] = []
    operation_ids: Annotated[
        list[str],
        Field(
            description="include documents  matching the given `gulp.operation_id`/s",
        ),
    ] = []
    context_ids: Annotated[
        list[str],
        Field(
            description="""
include documents matching the given `gulp.context_id`/s.

- this must be set to the *real context_id* as on the collab database, calculated as *SHA1(operation_id+context_id)*.
""",
        ),
    ] = []
    source_ids: Annotated[
        list[str],
        Field(
            description="""
include documents matching the given `gulp.source_id`/s.
- this must be set to the *real source_id* as on the collab database, calculated as *SHA1(operation_id+context_id+source_id)*.
""",
        ),
    ] = []
    event_codes: Annotated[
        list[str],
        Field(
            description="include documents matching the given `event.code`/s.",
        ),
    ] = []

    @override
    def __str__(self) -> str:
        return super().__str__()

    def to_opensearch_dsl(self) -> dict:
        """
        convert to a query in OpenSearch DSL format using bool query with terms and range filters

        Returns:
            dict: a ready to be used query object for the search API, like:
            ```json
            {
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "terms": {
                                    "gulp.source_id": ["value1", "value2", ...]
                                }
                            },
                            {
                                "range": {
                                    "gulp.timestamp": {
                                        "gte": 1609459200000,
                                        "lte": 1609545600000
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            ```
        """

        def _build_filters():
            filters: list[dict] = []

            if self.agent_types:
                filters.append({"terms": {"agent.type": self.agent_types}})
            
            if self.operation_ids:
                filters.append({"terms": {"gulp.operation_id": self.operation_ids}})
            
            if self.context_ids:
                filters.append({"terms": {"gulp.context_id": self.context_ids}})
            
            if self.source_ids:
                filters.append({"terms": {"gulp.source_id": self.source_ids}})
            
            if self.doc_ids:
                filters.append({"terms": {"_id": self.doc_ids}})

            if self.event_codes:
                filters.append({"terms": {"event.code": self.event_codes}})
            
            if self.time_range and len(self.time_range) == 2:
                # range query
                field = "gulp.timestamp"
                range_clause = {}
                if self.time_range[0] > 0:
                    range_clause["gte"] = self.time_range[0]
                if self.time_range[1] > 0:
                    range_clause["lte"] = self.time_range[1]
                if range_clause:
                    filters.append({"range": {field: range_clause}})
            
            if self.model_extra:
                # extra fields
                for k, v in self.model_extra.items():
                    if isinstance(v, list):
                        # terms query for lists
                        filters.append({"terms": {k: v}})
                    else:
                        # term query for single values
                        filters.append({"term": {k: v}})

            return filters

        filters = _build_filters()
        
        # build the query struct using bool filter
        if filters:
            query_dict = {
                "query": {
                    "bool": {
                        "filter": filters
                    }
                }
            }
        else:
            # empty filter, match all
            query_dict = {
                "query": {
                    "match_all": {}
                }
            }

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
        # build merged bool-filter combining both this filter's clauses and
        # the provided DSL's filter clauses where possible
        merged_filters: list[dict] = []

        our_q = self.to_opensearch_dsl().get("query", {})
        # if our query is a bool filter, extract its filters
        if isinstance(our_q, dict) and "bool" in our_q and "filter" in our_q["bool"]:
            merged_filters.extend(our_q["bool"]["filter"])
        else:
            # if it's match_all or other query, include it as-is if present
            if our_q and not ("match_all" in our_q):
                merged_filters.append(our_q)

        # now merge the provided DSL's query
        other_q = dsl.get("query", {})
        if isinstance(other_q, dict) and "bool" in other_q and "filter" in other_q["bool"]:
            merged_filters.extend(other_q["bool"]["filter"])
        else:
            if other_q and not ("match_all" in other_q):
                merged_filters.append(other_q)

        if merged_filters:
            return {"query": {"bool": {"filter": merged_filters}}}

        # nothing to filter, return match_all
        return {"query": {"match_all": {}}}

    def is_empty(self, check_operation_ids: bool=True) -> bool:
        """
        Check if the filter is empty.

        Args:
            check_operation_ids (bool): whether to consider operation_ids in the emptiness check.
        Returns:
            bool: True if the filter is empty, False otherwise.
        """
        to_check= [
            self.time_range,
            self.agent_types,
            self.operation_ids,
            self.context_ids,
            self.source_ids,
            self.event_codes,
            self.doc_ids,
            self.model_extra,
        ]
        if not check_operation_ids:
            to_check.remove(self.operation_ids)

        return not any(to_check)