import json
from typing import Any, Optional, Self, Union

import muty.string
from pydantic import BaseModel, ConfigDict, Field, model_validator

from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.opensearch.filters import QUERY_DEFAULT_FIELDS, GulpIngestionFilter, GulpQueryFilter, GulpSortOrder
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpDocumentsChunk
from elasticsearch import AsyncElasticsearch
from sigma.backends.opensearch import OpensearchLuceneBackend
from gulp.plugin_params import GulpPluginParameters


class GulpConvertedSigma(BaseModel):
    """
    A converted sigma rule
    """
    title: str = Field(..., description="the title of the sigma rule.")
    id: str = Field(..., description="the id of the sigma rule.")
    q: Any = Field(..., description="the converted query.")
    tags: list[str] = Field([], description="the tags of the sigma rule.")
    backend: str = Field(..., description="the backend used to convert the sigma rule.")
    pipeline: str = Field(..., description="the pipeline used to convert the sigma rule.")

class GulpSigmaQueryParameters(BaseModel):
    """
    represents options for a sigma query.
    """
    create_notes: bool = Field(
        True,
        description="if set, create notes on match",
    )
    note_title: str = Field(
        None,
        description="the title of the note to create on match, defaults=sigma rule title",
    )
    note_tags: list[str] = Field(
        None,
        description="the tags of the note to create, default=[\"auto\"]",
    )
    note_color: str = Field(
        None,
        description="the color of the note to create, default=use notes default",
    )
    note_glyph: str = Field(
        None,
        description="id of the glyph of the note to create, default=use glyphs default",
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

class GulpQueryExternalParameters(BaseModel):
    """
    Parameters to query an external system.
    """

    model_config = ConfigDict(extra="allow")

    uri: str = Field(
        ...,
        description="the URI to use to query the external system.",
    )
    query: Any = Field(
        ...,
        description="the query to perform, format is specific to the external system and will be handled by the plugin implementing `query_external`.",
    )

    username: str = Field(
        None,
        description="the username to use to query the external system.",
    )
    password: str = Field(
        None,
        description="the password to use to query the external system.",
    )
    options: Optional[Any] = Field(
        None,
        description="further options to pass to the external system, format is specific to the external system and will be handled by the plugin implementing `query_external`.",
    )
    loop: Optional[bool] = Field(
        True,
        description="if set, the query will be repeated in a loop until the external system returns no more results.",
    )
    sigma_parameters: Optional[GulpSigmaQueryParameters] = Field(
        None,
        description="if set, this is a sigma query and these are the additional parameters.",
    )

class GulpQueryAdditionalParameters(BaseModel):
    """
    additional options for a query.

    may include the following extra fields in `model_extra`:
        - sigma_parameters: GulpSigmaQueryParameter
        - sigma_create_notes: bool: if set, this is a sigma query and indicates to create notes on match
        - note_title: str: for sigma queries, the title of the note to create on match, mandatory if sigma is set.
        - note_tags: list[str], optional: for sigma queries, the tags of the note to create
        - note_color: str, optional: for sigma queries, the color of the note to create
        - note_glyph: str, optional: for sigma queries, id of the glyph of the note to create.
        - sigma_pipeline: str, optional: for sigma queries, the pipeline to use when converting the sigma rule, must be implemented by `plugin` (default=plugin's default)
        - sigma_backend: str, optional: for sigma queries, the backend to use when converting the sigma rule, must be implemented by `plugin` (default=plugin's default)
        - sigma_output_format: str, optional: for sigma queries, the output format to use when converting the sigma rule, must be implemented by `plugin` (default=plugin's default)
    """
    model_config = ConfigDict(extra="allow")

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
    sigma_parameters: Optional[GulpSigmaQueryParameters] = Field(
        None,
        description="if set, this is a sigma query and these are the additional parameters.",
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

        # GulpLogger.get_logger().debug("query options: %s" % (json.dumps(n, indent=2)))
        return n

class GulpQuery:
    """
    helpers to perform queries
    """

    @staticmethod
    async def _get_requestor_user_id(token: str) -> int:
        """
        Get the user id of the requestor.

        Args:
            token(str): the authentication token

        Returns:
            int: the user id of the requestor

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
        """
        sess: GulpUserSession = await GulpUserSession.check_token_permission(token)
        return sess.user_id

    @staticmethod
    async def query_raw(
        token: str,
        dsl: dict,
        ws_id: str,
        req_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
        user_id: str = None,
    ) -> None:
        """
        Perform a raw opensearch/elasticsearch DSL query using "search" API, streaming GulpDocumentChunk results to the websocket.

        Args:
            token(str): the authentication token (if user_id is not set)
            req_id(str): the request id
            ws_id(str): the websocket id
            dsl(dict): the dsl query in OpenSearch/Elasticsearch DSL language to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalParameters, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)
            user_id(str, optional): the user id of the requestor (default=use the token to get the user id)
        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        if token is None:
            if not user_id:
                raise ValueError("if token is not set, user_id must be set")        
        else:
            user_id = await GulpQuery._get_requestor_user_id(token)

        if not options:
            options = GulpQueryAdditionalParameters()
        
        if flt:
            # merge with filter
            dsl = flt.merge_to_opensearch_dsl(dsl)

        return await GulpOpenSearch.get_instance().search_dsl(
            index=index,
            q=dsl,
            req_id=req_id,
            ws_id=ws_id,
            user_id=user_id,
            options=options,
            el=el,
        )

    @staticmethod
    async def query_gulp(
        token: str,
        req_id: str,
        ws_id: str,
        index: str,
        flt: GulpQueryFilter,
        options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query using the given filter and options, streaming GulpDocumentChunk results to the websocket.
        NOTE: calls `raw_query` with the filter converted to OpenSearch/Elasticsearch DSL.

        Args:
            token(str): the authentication token
            req_id(str): the request id
            ws_id(str): the websocket id
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter): the filter to use
            options(GulpQueryAdditionalParameters, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        if not options:
            options = GulpQueryAdditionalParameters()
        options.sigma_parameters=None
        
        dsl = flt.to_opensearch_dsl()
        return await GulpQuery.query_raw(
            token=token,
            req_id=req_id,
            ws_id=ws_id,
            dsl=dsl,
            index=index,
            options=options,
            el=el,
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
            converted = p.sigma_convert(sigma, 
                                        referenced_sigmas=referenced_sigma,
                                        backend=backend,
                                        pipeline=pipeline,
                                        output_format=output_format)
            return converted
        finally:
            if p:
                await p.unload()

    @staticmethod
    async def query_sigma(
        token: str,
        req_id: str,
        ws_id: str,
        sigma: str,
        plugin: str,
        index: str,
        referenced_sigma: list[str] = None,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query on gulp's opensearch using the given sigma rule, streaming GulpDocumentChunk results to the websocket.
        NOTE: calls `raw_query` with the converted sigma rule and filter.

        Args:
            token(str): the authentication token
            req_id(str): the request id
            ws_id(str): the websocket id
            sigma(str): the main sigma rule YAML
            plugin(str): the plugin which implements `sigma_convert` to convert the sigma rule to OpenSearch/Elasticsearch DSL, must implement backend "opensearch" and output format "dsl_lucene"
            index(str): the gulp's opensearch/elasticsearch index/datastream to target
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalParameters, optional): additional options to use, refer to `GulpQueryAdditionalParameters` for more details about sigma rule options
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        if not options:
            options = GulpQueryAdditionalParameters()
        
        if not options.sigma_parameters:
            options.sigma_parameters = GulpSigmaQueryParameters()

        queries:list[GulpConvertedSigma] = GulpQuery.query_sigma_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend="opensearch",
            pipeline=options.sigma_parameters.pipeline,
            output_format="dsl_lucene",
        )
        
        for q in queries:
            # perform queries
            options.sigma_parameters.note_title = q.title
            options.sigma_parameters.note_tags = q.tags
            return await GulpQuery.query_raw(
                token=None,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                dsl=q.q,
                index=index,
                flt=flt,
                options=options,
                el=el,
            )
        
    @staticmethod
    async def query_stored(
        token: str,
        req_id: str,
        ws_id: str,
        id: str,
        index: str,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalParameters = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query on gulp's opensearch using a stored query, streaming GulpDocumentChunk results to the websocket.

        Args:
            token(str): the authentication token
            req_id(str): the request id
            ws_id(str): the websocket id
            id(str): the id of the stored query to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalParameters, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)
        
        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        # get stored query by id
        q: GulpStoredQuery = await GulpStoredQuery.get_one_by_id(id)
        if not options:
            options = GulpQueryAdditionalParameters()
        options.sigma_parameters = None

        if q.converted:
            return await GulpQuery.query_raw(
                token=token,
                req_id=req_id,
                ws_id=ws_id,
                dsl=json.loads(q.converted),
                index=index,
                flt=flt,
                options=options,
                el=el,
            )
        raise ValueError("query.converted is not set, stored query must be preprocessed first")

    @staticmethod
    async def query_external(
        token: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        query: GulpQueryExternalParameters,
        ingest_index: str=None,
        operation: str=None,
        context: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        query an external source for a set of documents, using the external source query language, and optionally ingest the results to a gulp index.
        
        the results are converted to gulp documents and streamed to the websocket.

        Args:
            token (str): the authentication token
            req_id (str): the request id
            ws_id (str): the websocket id
            query (GulpExternalQuery): includes the query and all the necessary parameters to communicate with the external source
            plugin(str): the plugin to use to query the external source, must implement `query_external`
            ingest_index(str, optional): if set, a gulp index to ingest the results to (to perform direct ingestion into gulp during query)
            operation (str, optional): only used with `ingest_index`, the operation to associate with. Defaults to None.
            context (str, optional): only used with `ingest_index`, the context to associate with. Defaults to None.
            source (str, optional): only used with `ingest_index`, indicates the log source. Defaults to None.
            plugin_params (GulpPluginParameters, optional): plugin parameters, including i.e. in GulpPluginParameters.extra the login/pwd/token to connect to the external source, plugin dependent. Defaults to None.
        
        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
                
        try:
            # load plugin
            from gulp.plugin import GulpPluginBase
            p = await GulpPluginBase.load(plugin)

            # query
            query.sigma_parameters=None
            await p.query_external(
                req_id=req_id,
                ws_id=ws_id,
                user=user_id,
                query=query,
                operation=operation,
                context=context,
                source=source,
                ingest_index=ingest_index,
                plugin_params=plugin_params,
            )
        finally:
            if p:
                await p.unload()

    @staticmethod
    async def query_external_single(
        token: str,
        req_id: str,
        plugin: str,
        query: GulpQueryExternalParameters,
        plugin_params: GulpPluginParameters = None,
    ) -> dict:
        """
        query a single document on an external source.

        Args:
            req_id (str): the request id
            query (GulpExternalQuery): set `query.query` to the `id` of the single document to query here.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.

        Returns:
            dict: the document as a GulpDocument dictionary

        Raises:
            ObjectNotFound: if the document is not found.

        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        # check token
        await GulpQuery._get_requestor_user_id(token)
        
        try:
            # load plugin
            from gulp.plugin import GulpPluginBase        
            p = await GulpPluginBase.load(plugin)

            # query
            query.sigma_parameters=None
            return await p.query_external_single(
                req_id=req_id,
                query=query,
                plugin_params=plugin_params,
            )
        finally:
            if p:
                await p.unload()
    
    @staticmethod
    async def query_external_sigma(
        token: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        sigma: str,
        query: GulpQueryExternalParameters,
        referenced_sigma: list[str] = None,
        ingest_index: str=None,
        operation: str=None,
        context: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,        
    ) -> None:
        """
        query an external source for a set of documents using a sigma rule, and optionally ingest the results to a gulp index.

        the results are converted to gulp documents and streamed to the websocket.

        Args:
            token (str): the authentication token
            req_id (str): the request id
            ws_id (str): the websocket id
            plugin(str): the plugin to use to query the external source, must implement `query_external`
            sigma (str): the sigma rule YAML
            query (GulpExternalQuery): includes the query and all the necessary parameters to communicate with the external source.
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            ingest_index(str, optional): if set, a gulp index to ingest the results to (to perform direct ingestion into gulp during query)

        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        if not query.sigma_parameters:
            query.sigma_parameters = GulpSigmaQueryParameters()

        # convert sigma
        queries:list[GulpConvertedSigma] = await GulpQuery.query_sigma_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend=query.sigma_parameters.backend,
            pipeline=query.sigma_parameters.pipeline,
            output_format=query.sigma_parameters.output_format,
        )

        # if ingesting to our index, by default sigma note creation is set unless explicitly set to False
        if not ingest_index:
            # either, no ingestion=no notes
            query.sigma_parameters.create_notes = False
            
        try:
            # load plugin
            from gulp.plugin import GulpPluginBase
            p = await GulpPluginBase.load(plugin)

            # query
            for q in queries:
                # perform queries
                query.sigma_parameters.note_title = q.title
                query.sigma_parameters.note_tags = q.tags
                query.query = q.q
                await p.query_external(
                    req_id=req_id,
                    ws_id=ws_id,
                    user=user_id,
                    query=query,
                    operation=operation,
                    context=context,
                    source=source,
                    ingest_index=ingest_index,
                    plugin_params=plugin_params,
                )

        finally:
            if p:
                await p.unload()