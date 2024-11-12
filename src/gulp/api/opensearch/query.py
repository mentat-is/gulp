import json
from typing import Any, Optional, Union

import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.opensearch.filters import GulpIngestionFilter, GulpQueryAdditionalOptions, GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpDocumentsChunk
from elasticsearch import AsyncElasticsearch
from sigma.backends.opensearch import OpensearchLuceneBackend
from gulp.plugin import GulpPluginBase
from gulp.plugin_params import GulpPluginParameters



class GulpExternalQueryParameters(BaseModel):
    """
    Parameters to query an external system.
    """

    class Config:
        extra = "allow"

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
        options: GulpQueryAdditionalOptions = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a raw opensearch/elasticsearch DSL query using "search" API, streaming GulpDocumentChunk results to the websocket.

        Args:
            token(str): the authentication token
            req_id(str): the request id
            ws_id(str): the websocket id
            dsl(dict): the dsl query in OpenSearch/Elasticsearch DSL language to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)
        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        if not options:
            options = GulpQueryAdditionalOptions()
        
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
        options: GulpQueryAdditionalOptions = None,
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
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
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
    async def sigma_query_build(
        sigma: str,
        plugin: str,
        referenced_sigma: list[str] = None,
        backend: str = None,
        pipeline: str = None,
        output_format: str = None,
    ) -> any:
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
            any: the converted sigma rule in the output format specified
        """
        # convert sigma
        p = await GulpPluginBase.load(plugin)
        converted = p.sigma_convert(sigma, 
                                    referenced_sigmas=referenced_sigma,
                                    backend=backend,
                                    pipeline=pipeline,
                                    output_format=output_format)
        return converted

    @staticmethod
    async def sigma_query(
        token: str,
        req_id: str,
        ws_id: str,
        sigma: str,
        plugin: str,
        index: str,
        referenced_sigma: list[str] = None,
        pipeline: str = None,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
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
            index(str): the opensearch/elasticsearch index/datastream to target
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            pipeline(str, optional): the pipeline to use when converting the sigma rule (default: plugin's default, must be implemented by the plugin and listed in its `sigma_support`)
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        dsl: dict = GulpQuery.sigma_query_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend="opensearch",
            pipeline=pipeline,
            output_format="dsl_lucene",
        )
        
        # perform the query
        return await GulpQuery.query_raw(
            token=token,
            req_id=req_id,
            ws_id=ws_id,
            dsl=dsl,
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
        options: GulpQueryAdditionalOptions = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query using a stored query, streaming GulpDocumentChunk results to the websocket.

        Args:
            token(str): the authentication token
            req_id(str): the request id
            ws_id(str): the websocket id
            id(str): the id of the stored query to use
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)
        
        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        # get stored query by id
        q: GulpStoredQuery = await GulpStoredQuery.get_one_by_id(id)
        if q.converted:
            # already converted to dsl
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
        raise ValueError("stored query must be preprocessed first")

    @staticmethod
    async def query_external(
        token: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        query: GulpExternalQueryParameters,
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
        
        # load plugin
        p = await GulpPluginBase.load(plugin)
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

    @staticmethod
    async def query_external_single(
        token: str,
        req_id: str,
        plugin: str,
        id_and_params: GulpExternalQueryParameters,
        plugin_params: GulpPluginParameters = None,
    ) -> dict:
        """
        query a single document on an external source.

        Args:
            req_id (str): the request id
            id (GulpExternalQuery): set `query` to the id of the single document to query here.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.

        Returns:
            dict: the document as a GulpDocument dictionary

        Raises:
            ObjectNotFound: if the document is not found.

        Notes:
            - implementers must call super().query_external first then _initialize().<br>
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        
        # load plugin
        p = await GulpPluginBase.load(plugin)
        return await p.query_external_single(
            req_id=req_id,
            id_and_params=id_and_params,
            plugin_params=plugin_params,
        )
    
    @staticmethod
    async def external_sigma_query(
        token: str,
        req_id: str,
        ws_id: str,
        plugin: str,
        sigma: str,
        query_parameters: GulpExternalQueryParameters,
        referenced_sigma: list[str] = None,
        backend: str = None,
        pipeline: str = None,
        output_format: str = None,
        ingest_index: str=None,
        operation: str=None,
        context: str = None,
        source: str = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        
        # get sigma
        converted = await GulpQuery.sigma_query_build(
            sigma=sigma,
            plugin=plugin,
            referenced_sigma=referenced_sigma,
            backend=backend,
            pipeline=pipeline,
            output_format=output_format,
        )
        query_parameters.query = converted

        # run the external query as normal
        await GulpQuery.query_external(
            token=token,
            req_id=req_id,
            ws_id=ws_id,
            plugin=plugin,
            query=query_parameters,
            ingest_index=ingest_index,
            operation=operation,
            context=context,
            source=source,
            plugin_params=plugin_params,
            sigma=True
        )
        