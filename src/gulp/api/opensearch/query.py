import json
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.opensearch.filters import (
    QUERY_DEFAULT_FIELDS,
    GulpQueryFilter,
    GulpSortOrder,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.structs import GulpPluginParameters


class GulpConvertedSigma(BaseModel):
    """
    A converted sigma rule
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "test",
                    "id": "test",
                    "q": {"query": {"match_all": {}}},
                    "tags": ["test"],
                    "backend": "opensearch",
                    "pipeline": "default",
                }
            ]
        }
    )
    name: str = Field(..., description="the name/title of the sigma rule.")
    id: str = Field(..., description="the id of the sigma rule.")
    q: Any = Field(..., description="the converted query.")
    tags: list[str] = Field([], description="the tags of the sigma rule.")
    backend: str = Field(..., description="the backend used to convert the sigma rule.")
    pipeline: str = Field(
        ..., description="the pipeline used to convert the sigma rule."
    )


class GulpSigmaQueryParameters(BaseModel):
    """
    represents options for a sigma query, to customize automatic note creation or to customize
    the conversion using specific backend/pipeline/output format.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "create_notes": True,
                    "note_name": "Sigma Rule",
                    "note_tags": ["auto"],
                    "note_color": None,
                    "note_glyph_id": None,
                    "note_private": False,
                    "pipeline": None,
                    "backend": None,
                    "output_format": None,
                }
            ]
        }
    )

    create_notes: bool = Field(
        True,
        description="if set, create notes on match",
    )
    note_name: str = Field(
        None,
        description="the display name of the notes to create on match, default=use sigma rule title",
    )
    note_tags: list[str] = Field(
        None,
        description='the tags of the notes to create on match, default=["auto"]',
    )
    note_color: str = Field(
        None,
        description="the color of the notes to create on match, default=use notes default",
    )
    note_glyph_id: str = Field(
        None,
        description="id of the glyph of the notes to create on match, default=use glyphs default",
    )
    note_private: bool = Field(
        False,
        description="if set, the notes to create on match are private, default=False",
    )
    pipeline: str = Field(
        None,
        description="the pipeline to use when converting the sigma rule, default=plugin's default",
    )
    backend: str = Field(
        None,
        description="the backend to use when converting the sigma rule, default=plugin's default",
    )
    output_format: str = Field(
        None,
        description="the output format to use when converting the sigma rule, default=plugin's default",
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
                        "@timestamp": "asc",
                        "_id": "asc",
                        "event.sequence": "asc",
                    },
                    "fields": ["@timestamp", "event.id"],
                    "limit": 1000,
                    "search_after": None,
                    "loop": True,
                    "sigma_parameters": None,
                    "external_uri": "http://localhost:8080",
                    "external_credentials": ("user", "password"),
                    "external_options": None,
                }
            ]
        },
    )

    sort: Optional[dict[str, GulpSortOrder]] = Field(
        default={"@timestamp": "asc", "_id": "asc", "event.sequence": "asc"},
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
    search_after: Optional[list] = Field(
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
    sigma_parameters: Optional[GulpSigmaQueryParameters] = Field(
        None,
        description="if set, this is a sigma query and these are the additional parameters (i.e. to create notes or choose a specific pipeline).",
    )
    external_uri: str = Field(
        None,
        description="for external queries: the URI to use to query the external system.",
    )
    external_credentials: tuple[str, str] = Field(
        None,
        description="for external queries: a tuple with the username and password to use to query the external system, may be None if set in the uri.",
    )

    external_options: Optional[dict] = Field(
        None,
        description="for external queries: further options to pass to the external system, format is specific to the external system and will be handled by the plugin implementing `query_external`.",
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


class GulpQuery:
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
    ) -> None:
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
        Raises:
            ObjectNotFound: if no document is found
        """
        if not q_options:
            q_options = GulpQueryAdditionalParameters()

        if flt and not flt.is_empty():
            # merge with filter
            q = flt.merge_to_opensearch_dsl(q)

        await GulpOpenSearch.get_instance().search_dsl(
            index=index,
            q=q,
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            q_options=q_options,
            el=el,
            sess=sess,
        )

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

    @staticmethod
    async def query_sigma_build(
        sigma: str,
        plugin: str,
        referenced_sigma: list[str] = None,
        backend: str = None,
        pipeline: str = None,
        output_format: str = None,
    ) -> list[GulpConvertedSigma]:
        """
        builds a sigma query for the given sigma rule using the given plugin.

        Args:
            sigma(str): the main sigma rule YAML
            plugin(str): the plugin which implements `sigma_convert` to convert the sigma rule to OpenSearch/Elasticsearch DSL
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            backend(str, optional): the backend to use when converting the sigma rule, must be implemented by the plugin and listed in its `sigma_support`
            pipeline(str, optional): the pipeline to use when converting the sigma rule, must be implemented by the plugin and listed in its `sigma_support`
            output_format(str, optional): the output format to use when converting the sigma rule, must be implemented by the plugin and listed in its `sigma_support`

        Returns:
            list[GulpConvertedSigma]: one or more converted sigma rules
        """
        try:
            # convert sigma using the plugin
            from gulp.plugin import GulpPluginBase

            p = await GulpPluginBase.load(plugin)
            converted = p.sigma_convert(
                sigma,
                referenced_sigmas=referenced_sigma,
                backend=backend,
                pipeline=pipeline,
                output_format=output_format,
            )
            return converted
        finally:
            if p:
                await p.unload()

    @staticmethod
    async def query_sigma(
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        sigma: str,
        plugin: str,
        index: str,
        referenced_sigma: list[str] = None,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query on gulp's opensearch using the given sigma rule, streaming GulpDocumentChunk results to the websocket.
        NOTE: calls `raw_query` with the converted sigma rule and filter.

        Args:
            sess(AsyncSession): the database session
            user_id(str): the user id of the requestor
            req_id(str): the request id
            ws_id(str): the websocket id
            sigma(str): the main sigma rule YAML
            plugin(str): the plugin which implements `sigma_convert` to convert the sigma rule to OpenSearch/Elasticsearch DSL, must implement backend "opensearch" and output format "dsl_lucene"
            index(str): the gulp's opensearch/elasticsearch index/datastream to target
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            q_options(GulpQueryAdditionalParameters, optional): additional options to use, refer to `GulpQueryAdditionalParameters` for more details about sigma rule options
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        if not q_options:
            # use defaults
            q_options = GulpQueryAdditionalParameters()

        if not q_options.sigma_parameters:
            # use defaults
            q_options.sigma_parameters = GulpSigmaQueryParameters()

        queries: list[GulpConvertedSigma] = GulpQuery.query_sigma_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend="opensearch",
            pipeline=q_options.sigma_parameters.pipeline,
            output_format="dsl_lucene",
        )

        for q in queries:
            # perform queries
            q_options.sigma_parameters.note_name = q.name
            q_options.sigma_parameters.note_tags = q.tags
            await GulpQuery.query_raw(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q=q.q,
                index=index,
                flt=flt,
                q_options=q_options,
                el=el,
            )

    @staticmethod
    async def query_stored(
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        stored_query_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        q_options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query on gulp's opensearch using a stored query, streaming GulpDocumentChunk results to the websocket.

        Args:
            sess(AsyncSession): the database session
            user_id(str): the user id of the requestor
            req_id(str): the request id
            ws_id(str): the websocket id
            stored_query_id(str): the id of the stored query to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            q_ptions(GulpQueryAdditionalParameters, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        # get stored query by id
        q: GulpStoredQuery = await GulpStoredQuery.get_by_id(sess, stored_query_id)
        if not q_options:
            q_options = GulpQueryAdditionalParameters()
        q_options.sigma_parameters = None

        await GulpQuery.query_raw(
            sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            q=json.loads(q.q),
            index=index,
            flt=flt,
            q_options=q_options,
            el=el,
        )

    @staticmethod
    async def query_external_stored(
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        stored_query_id: str,
        q_options: GulpQueryAdditionalParameters,
        ingest_index: str = None,
        operation_id: str = None,
        context_id: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        a shortcut for query_external to use a stored query.

        Args:
            sess(AsyncSession): the database session
            user_id(str): the user id of the requestor
            req_id (str): the request id
            ws_id (str): the websocket id
            plugin(str): the plugin to use to query the external source, must implement `query_external`
            stored_query_id (str): the id of the stored query to use
            q_options (GulpQueryAdditionalParameters): additional options to use (uri, credentials, custom options, ...)
            ingest_index(str, optional): if set, a gulp index to ingest the results to (to perform direct ingestion into gulp during query)
            operation_id (str, optional): only used with `ingest_index`, the operation to associate with. Defaults to None.
            context_id (str, optional): only used with `ingest_index`, the context to associate with. Defaults to None.
            source (str, optional): only used with `ingest_index`, indicates the log source. Defaults to None.
            plugin_params (GulpPluginParameters, optional): plugin parameters
        """
        # get stored query by id
        q: GulpStoredQuery = await GulpStoredQuery.get_by_id(sess, stored_query_id)
        return await GulpQuery.query_external(
            sess=sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            plugin=plugin,
            q=q.q,
            q_options=q_options,
            ingest_index=ingest_index,
            operation_id=operation_id,
            context_id=context_id,
            source=source,
            plugin_params=plugin_params,
        )

    @staticmethod
    async def query_external(
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        q: any,
        q_options: GulpQueryAdditionalParameters,
        ingest_index: str = None,
        operation_id: str = None,
        context_id: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        query an external source for a set of documents, using the external source query language, and optionally ingest the results to a gulp index.

        the results are converted to gulp documents and streamed to the websocket.

        Args:
            sess(AsyncSession): the database session
            user_id(str): the user id of the requestor
            req_id (str): the request id
            ws_id (str): the websocket id
            q (any): the query to use to query the external source
            q_options (GulpQueryAdditionalParameters): additional options to use (uri, credentials, custom options, ...)
            plugin(str): the plugin to use to query the external source, must implement `query_external`
            ingest_index(str, optional): if set, a gulp index to ingest the results to (to perform direct ingestion into gulp during query)
            operation_id (str, optional): only used with `ingest_index`, the operation to associate with. Defaults to None.
            context_id (str, optional): only used with `ingest_index`, the context to associate with. Defaults to None.
            source (str, optional): only used with `ingest_index`, indicates the log source. Defaults to None.
            plugin_params (GulpPluginParameters, optional): plugin parameters

        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        try:
            # load plugin
            from gulp.plugin import GulpPluginBase

            p = await GulpPluginBase.load(plugin)

            # query
            q_options.sigma_parameters = None
            await p.query_external(
                sess=sess,
                req_id=req_id,
                ws_id=ws_id,
                user_id=user_id,
                q=q,
                q_options=q_options,
                operation_id=operation_id,
                context_id=context_id,
                source=source,
                ingest_index=ingest_index,
                plugin_params=plugin_params,
            )
        finally:
            if p:
                await p.unload()

    @staticmethod
    async def query_external_single(
        req_id: str,
        plugin: str,
        q: any,
        q_options: GulpQueryAdditionalParameters,
        plugin_params: GulpPluginParameters = None,
    ) -> dict:
        """
        query a single document on an external source.

        Args:
            sess(AsyncSession): the database session
            req_id (str): the request id
            plugin (str): the plugin to use to query the external source, must implement `query_external_single`
            q (any): set to the id of the single document to query on the external source, format is plugin dependent
            q_options (GulpQueryAdditionalParameters): additional options to use (uri, credentials, custom options, ...)
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.

        Returns:
            dict: the document as a GulpDocument dictionary

        Raises:
            ObjectNotFound: if the document is not found.

        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        try:
            # load plugin
            from gulp.plugin import GulpPluginBase

            p = await GulpPluginBase.load(plugin)

            # query
            q.sigma_parameters = None
            return await p.query_external_single(
                req_id=req_id,
                q=q,
                q_options=q_options,
                plugin_params=plugin_params,
            )
        finally:
            if p:
                await p.unload()

    @staticmethod
    async def query_external_sigma(
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        sigma: str,
        q_options: GulpQueryAdditionalParameters,
        referenced_sigma: list[str] = None,
        ingest_index: str = None,
        operation_id: str = None,
        context_id: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        query an external source for a set of documents using a sigma rule, and optionally ingest the results to a gulp index.

        the results are converted to gulp documents and streamed to the websocket.

        Args:
            sess(AsyncSession): the database session
            user_id(str): the user id of the requestor
            req_id (str): the request id
            ws_id (str): the websocket id
            plugin(str): the plugin to use to query the external source, must implement `query_external`
            sigma (str): the sigma rule YAML
            q_options (GulpQueryAdditionalParameters): additional options to use (uri, credentials, custom options, ...)
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            ingest_index(str, optional): if set, a gulp index to ingest the results to (to perform direct ingestion into gulp during query)
            operation_id (str, optional): only used with `ingest_index`, the operation to associate with. Defaults to None.
            context_id (str, optional): only used with `ingest_index`, the context to associate with. Defaults to None.
            source (str, optional): only used with `ingest_index`, indicates the log source. Defaults to None.            plugin_params (GulpPluginParameters, optional): plugin parameters
        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        if not q_options.sigma_parameters:
            # use defaults
            q_options.sigma_parameters = GulpSigmaQueryParameters()

        # convert sigma
        queries: list[GulpConvertedSigma] = await GulpQuery.query_sigma_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend=q_options.sigma_parameters.backend,
            pipeline=q_options.sigma_parameters.pipeline,
            output_format=q_options.sigma_parameters.output_format,
        )

        # if we provide an index to ingest to, by default sigma note creation is set unless explicitly set to False
        if not ingest_index:
            # either, no ingestion=no notes
            q_options.sigma_parameters.create_notes = False

        try:
            # load plugin
            from gulp.plugin import GulpPluginBase

            p = await GulpPluginBase.load(plugin)

            # query
            for q in queries:
                # perform queries
                q_options.sigma_parameters.note_name = q.name
                q_options.sigma_parameters.note_tags = q.tags
                await p.query_external(
                    sess,
                    user_id=user_id,
                    req_id=req_id,
                    ws_id=ws_id,
                    q=q.q,
                    q_options=q_options,
                    operation_id=operation_id,
                    context_id=context_id,
                    source=source,
                    ingest_index=ingest_index,
                    plugin_params=plugin_params,
                )

        finally:
            if p:
                await p.unload()
