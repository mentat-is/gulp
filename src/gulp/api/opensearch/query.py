import json
from typing import Optional, Union

import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.collab.user_session import GulpUserSession
from gulp.api.opensearch.structs import (
    GulpQueryAdditionalOptions,
    GulpQueryFilter,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import GulpDocumentsChunk
from elasticsearch import AsyncElasticsearch


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

        Args:
            token(str): the authentication token
            ws_id(str): the websocket id
            req_id(str): the request id
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter): the filter to use
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the elasticsearch client to use (forces options.use_elasticsearch to True)

        Raises:
            MissingPermission: if the token is invalid or the user has no permission
            ObjectNotFound: if no document is found
        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        if not options:
            options = GulpQueryAdditionalOptions()

        dsl = flt.to_opensearch_dsl()
        return await GulpOpenSearch.get_instance().search_dsl(
            index=index,
            q=dsl["query"],
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            options=options,
            el=el,
        )

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
        Perform a query using the given OpenSearch/Elasticsearch DSL, streaming GulpDocumentChunk results to the websocket.

        Args:
            token(str): the authentication token
            dsl(dict): the dsl query in OpenSearch/Elasticsearch DSL language to use
            ws_id(str): the websocket id
            req_id(str): the request id
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use
            el(AsyncElasticsearch, optional): the elasticsearch client to use (forces options.use_elasticsearch to True)
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
    async def sigma_query(
        sigma: str,
        plugin: str,
        referenced_sigma: list[str] = None,
        flt: GulpQueryFilter = None,
        options: GulpQueryAdditionalOptions = None,
    ) -> None:
        """
        Perform a query using the given sigma rule, streaming GulpDocumentChunk results to the websocket.

        Args:
            sigma(str): the sigma rule to use
            plugin(str): the plugin which implements `sigma_convert` to convert the sigma rule to OpenSearch/Elasticsearch DSL
            referenced_sigma(list[str], optional): the list of referenced sigma rules
            flt(GulpQueryFilter, optional): if set, the filter to merge with the query (to restrict the search)
            options(GulpQueryAdditionalOptions, optional): additional options to use

        Raises:
            ObjectNotFound: if no document is found
        """
        # load the plugin, which must implement sigma_convert

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
        id: str, flt: GulpQueryFilter = None, options: GulpQueryAdditionalOptions = None
    ) -> GulpDocumentsChunk:
        pass

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
