import json
from typing import Optional, Union

import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.opensearch.filters import GulpQueryAdditionalOptions, GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpDocumentsChunk
from elasticsearch import AsyncElasticsearch

from gulp.plugin import GulpPluginBase



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
    query: Union[str, dict] = Field(
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
    options: Optional[dict] = Field(
        None,
        description="further options to pass to the external system, format is specific to the external system and will be handled by the plugin implementing `query_external`.",
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
    async def raw_query(
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
            dsl(dict): the dsl query in OpenSearch/Elasticsearch DSL language to use
            ws_id(str): the websocket id
            req_id(str): the request id
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
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            options=options,
            el=el,
        )

    @staticmethod
    async def gulp_query(
        token: str,
        ws_id: str,
        req_id: str,
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
            ws_id(str): the websocket id
            req_id(str): the request id
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter): the filter to use
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        dsl = flt.to_opensearch_dsl()
        return await GulpQuery.raw_query(
            token=token,
            dsl=dsl,
            ws_id=ws_id,
            req_id=req_id,
            index=index,
            options=options,
            el=el,
        )

    @staticmethod
    async def sigma_query(
        token: str,
        sigma: str,
        plugin: str,
        ws_id: str,
        req_id: str,
        index: str,
        referenced_sigma: list[str] = None,
        backend: str = None,        
        pipeline: str = None,
        output_format: str = None,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        """
        Perform a query on gulp's opensearch using the given sigma rule, streaming GulpDocumentChunk results to the websocket.
        NOTE: calls `raw_query` with the converted sigma rule and filter.

        Args:
            sigma(str): the main sigma rule YAML
            plugin(str): the plugin which implements `sigma_convert` to convert the sigma rule to OpenSearch/Elasticsearch DSL
            ws_id(str): the websocket id
            req_id(str): the request id
            index(str): the opensearch/elasticsearch index/datastream to target
            referenced_sigma(list[str], optional): if any, each element is a sigma rule YAML referenced by `name` in the main sigma rule
            backend(str, optional): the backend to use when converting the sigma rule (default: "opensearch", must be implemented by the plugin and listed in its `sigma_support`)
            pipeline(str, optional): the pipeline to use when converting the sigma rule (default: plugin's default, must be implemented by the plugin and listed in its `sigma_support`)
            output_format(str, optional): the output format to use when converting the sigma rule (default: "dsl_lucene", must be implemented by the plugin and listed in its `sigma_support`)
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the optional elasticsearch client to use (default=use gulp OpenSearch client)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        # convert sigma first
        p = await GulpPluginBase.load(plugin)
        converted = p.sigma_convert(sigma, 
                                    referenced_sigmas=referenced_sigma,
                                    backend=backend or 'opensearch',
                                    pipeline=pipeline,
                                    output_format=output_format or 'dsl_lucene')
        
        # perform the query
        return await GulpQuery.raw_query(
            token=token,
            dsl=converted,
            ws_id=ws_id,
            req_id=req_id,
            index=index,
            flt=flt,
            options=options,
            el=el,
        )

    @staticmethod
    async def sigma_query_build(
        sigma: str,
        plugin: str,
        referenced_sigma: list[str] = None,
        flt: GulpQueryFilter = None,
    ) -> dict:
        pass

    @staticmethod
    async def stored_query(
        token: str,
        id: str,
        ws_id: str,
        req_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
        el: AsyncElasticsearch = None,
    ) -> None:
        # get stored query by id
        q: GulpStoredQuery = await GulpStoredQuery.get_one_by_id(id)
        if q.converted:
            # use raw query directly
    @staticmethod
    async def external_query(
        query: GulpExternalQueryParameters,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
    ) -> GulpDocumentsChunk:
        pass

    @staticmethod
    async def external_sigma_query(
        query: GulpExternalQueryParameters,
        sigma: str,
        plugin: str,
        referenced_sigma: list[str] = None,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
    ) -> GulpDocumentsChunk:
        pass
