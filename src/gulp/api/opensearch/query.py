from typing import Any, Optional

from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.opensearch.filters import (
    QUERY_DEFAULT_FIELDS,
    GulpQueryFilter,
    GulpSortOrder,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.structs import GulpPluginParameters
from muty.pydantic import autogenerate_model_example_by_class


class GulpQuery(BaseModel):
    """
    A query
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "test",
                    "id": "test",
                    "q": {"query": {"match_all": {}}},
                    "tags": ["test"],
                    "external_plugin": "test_plugin",
                    "plugin_params": {
                        "custom": "parameter",
                    },
                }
            ]
        }
    )
    name: str = Field(..., description="the name/title of the query.")
    q: Any = Field(..., description="the query, usually a dict.")
    sigma_id: Optional[str] = Field(
        None, description="the id of the sigma rule, if this is a sigma query."
    )
    tags: Optional[list[str]] = Field([], description="query tags.")
    external_plugin: Optional[str] = Field(
        None, description="the external plugin to use, if this is an external query."
    )
    external_plugin_params: Optional[GulpPluginParameters] = Field(
        None,
        description="custom external plugin parameters, if this is an external query.",
    )


class GulpQueryNoteParameters(BaseModel):
    """
    to automaticallyu create notes on query matches
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "create_notes": True,
                    "note_name": "test",
                    "note_tags": ["test"],
                    "note_color": None,
                    "note_glyph_id": None,
                    "note_private": False,
                }
            ]
        }
    )
    create_notes: bool = Field(
        False,
        description="if set, creates a note for every match (default for sigma and stored queries)",
    )
    note_name: str = Field(
        None,
        description="the display name of the notes to create on match, defaults None (uses query name)",
    )
    note_tags: list[str] = Field(
        None,
        description='the tags of the notes to create on match, defaults to None (["auto", sigma rule tags (for sigma queries))',
    )
    note_color: str = Field(
        None,
        description="the color of the notes to create on match, defaults to None (use notes default color)",
    )
    note_glyph_id: str = Field(
        None,
        description="id of the glyph of the notes to create on match, defaults to None (query group glyph if set, otherwise use notes default).",
    )
    note_private: bool = Field(
        False,
        description="if set, the notes to create on match are private, default=False",
    )


class GulpQuerySigmaParameters(BaseModel):
    """
    represents options for a sigma query, to customize automatic note creation or to customize
    the conversion using specific backend/pipeline/output format.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "pipeline": None,
                    "backend": None,
                    "output_format": None,
                }
            ]
        }
    )
    plugin: str = Field(
        None,
        description="""
the plugin to be used to convert the sigma rule.

- must implement `backend`, `pipeline`, `output_format`.
""",
    )
    pipeline: str = Field(
        None,
        description="the pipeline to use when converting the sigma rule, defaults to None (use plugin's default)",
    )
    backend: str = Field(
        None,
        description="this is only used for `external` queries: the backend to use when converting the sigma rule, defaults to None (use plugin's default)",
    )
    output_format: str = Field(
        None,
        description="this is only used for `external` queries: the output format to use when converting the sigma rule, defaults to None (use plugin's default)",
    )


class GulpQueryExternalParameters(BaseModel):
    """
    parameters to be set for ingestion via `external` sources query.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "plugin": "test_plugin",
                    "uri": "http://localhost:8080",
                    "username": "user",
                    "password": "password",
                    "params": None,
                    "ingest_index": "target_index",
                    "operation_id": "operation_1",
                    "context_name": "context_1",
                    "source": "test_source",
                }
            ]
        },
        extra="allow",
    )
    plugin: Optional[str] = Field(
        None,
        description="the plugin to be used to query the external source.",
    )
    plugin_params: Optional[GulpPluginParameters] = Field(
        GulpPluginParameters(),
        description="custom plugin parameters to use for the query.",
    )
    uri: Optional[str] = Field(
        None, description="The URI to connect to the external service."
    )
    username: Optional[str] = Field(
        None, description="The username to connect to the external service."
    )
    password: Optional[str] = Field(
        None, description="The password to connect to the external service."
    )
    ingest_index: Optional[str] = Field(
        None, description="if set, ingest the results to this gulp index/datastream."
    )
    operation_id: Optional[str] = Field(
        None, description="if set, the operation id to associate with the ingestion."
    )
    context_id: Optional[str] = Field(
        None, description="if set, the context id to associate with the ingestion."
    )
    source_id: Optional[str] = Field(
        None, description="if set, the source id to associate with the ingestion."
    )


class GulpQueryAdditionalParameters(BaseModel):
    """
    additional options for a query.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "sort": {
                        "@timestamp": GulpSortOrder.ASC,
                        "_id": GulpSortOrder.ASC,
                        "event.sequence": GulpSortOrder.ASC,
                    },
                    "fields": ["@timestamp", "event.id"],
                    "limit": 1000,
                    "group": "test",
                    "name": "test",
                    "search_after": None,
                    "loop": True,
                    "sigma_parameters": autogenerate_model_example_by_class(
                        GulpQuerySigmaParameters
                    ),
                    "note_parameters": autogenerate_model_example_by_class(
                        GulpQueryNoteParameters
                    ),
                    "external_uri": "http://localhost:8080",
                    "external_credentials": ("user", "password"),
                    "external_options": None,
                }
            ]
        },
    )
    name: Optional[str] = Field(
        None,
        description="the name of the query, used for logging and debugging.",
    )
    group: Optional[str] = Field(
        None,
        description="if the query is part of a query group, this is the name of the group.",
    )
    sort: Optional[dict[str, GulpSortOrder]] = Field(
        default={
            "@timestamp": GulpSortOrder.ASC,
            "_id": GulpSortOrder.ASC,
            "event.sequence": GulpSortOrder.ASC,
        },
        description="how to sort results, default=sort by ascending `@timestamp`.",
    )
    fields: Optional[list[str] | str] = Field(
        default=QUERY_DEFAULT_FIELDS,
        description="the set of fields to include in the returned documents.<br>"
        "default=`%s` (which are forcefully included anyway), use `*` to return all fields."
        % (QUERY_DEFAULT_FIELDS),
    )
    limit: Optional[int] = Field(
        1000,
        gt=1,
        le=10000,
        description="for pagination, the maximum number of documents to return **per chunk**, default=1000 (None=return up to 10000 documents per chunk).",
    )
    search_after: Optional[list[dict]] = Field(
        None,
        description="""
for pagination, this should be set to the `search_after` returned by the previous call. 

- check [OpenSearch documentation](https://opensearch.org/docs/latest/search-plugins/searching-data/paginate/#the-search_after-parameter).

- ignored if `loop` is set.
""",
    )
    loop: Optional[bool] = Field(
        True,
        description="if set, keep querying until all documents are returned (default=True, ignores `search_after`).",
    )
    sigma_parameters: Optional[GulpQuerySigmaParameters] = Field(
        GulpQuerySigmaParameters(),
        description="if set, this is a sigma query and these are the additional parameters (backend, pipeline, output format to be used).",
    )
    note_parameters: Optional[GulpQueryNoteParameters] = Field(
        GulpQueryNoteParameters(),
        description="if set, controls how notes are created during queries.",
    )
    external_parameters: Optional[GulpQueryExternalParameters] = Field(
        GulpQueryExternalParameters(),
        description="if set, controls how external queries are performed and how the results are ingested.",
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
        if self.fields and self.fields != "*":
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

        # MutyLogger.get_instance().debug("query options: %s" % (json.dumps(n, indent=2)))
        return n


class GulpQueryHelpers:
    """
    helpers to perform queries
    """

    @staticmethod
    async def query_raw(
        user_id: str,
        req_id: str,
        ws_id: str,
        q: dict,
        index: str,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
        sess: AsyncSession = None,
    ) -> tuple[int, int]:
        """
        Perform a raw opensearch/elasticsearch DSL query using "search" API, streaming GulpDocumentChunk results to the websocket.

        Args:
            user_id(str): the user id of the requestor
            req_id(str): the request id
            ws_id(str): the websocket id
            q(dict): the dsl query in OpenSearch/Elasticsearch DSL language to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            q_options(GulpQueryAdditionalParameters, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)
            user_id(str, optional): the user id of the requestor (default=use the token to get the user id)
            sess(AsyncSession, optional): collab database session, used only if options.sigma_parameters.create_notes is set
        Returns:
            tuple[int, int]: the number of documents processed and the total number of documents found
        Raises:
            Exception: if an error occurs during the query
        """
        if not q_options:
            q_options = GulpQueryAdditionalParameters()

        if flt and not flt.is_empty():
            # merge with filter
            q = flt.merge_to_opensearch_dsl(q)

        processed, total = await GulpOpenSearch.get_instance().search_dsl(
            index=index,
            q=q,
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            q_options=q_options,
            el=el,
            sess=sess,
        )
        return processed, total

    @staticmethod
    async def query_single(
        index: str,
        doc_id: str,
        el: AsyncElasticsearch = None,
    ) -> dict:
        """
        Perform a single document query using the given document id on gulp's opensearch/elasticsearch, and return the document as a GulpDocument dictionary.

        Args:
            req_id (str): the request id
            index (str): the opensearch/elasticsearch index/datastream to target
            doc_id (str): the document id to query
            el (AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Returns:
            dict: the document as a GulpDocument dictionary

        Raises:
            ObjectNotFound: if the document is not found.
        """
        return await GulpOpenSearch.get_instance().query_single_document(
            index, doc_id, el=el
        )
