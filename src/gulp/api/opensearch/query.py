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
    query: Union[str,dict] = Field(
        ...,
        description="the query to perform, format is specific to the external system and will be handled by the plugin implementing `query_external`.")
    
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
    async def gulp_query(token: str, ws_id: str, req_id: str, index: str, flt: GulpQueryFilter, options: GulpQueryAdditionalOptions=None) -> None:
        """
        Perform a query using the given filter and options.

        Args:
            token(str): the authentication token
            ws_id(str): the websocket id
            req_id(str): the request id
            index(str): the opensearch/elasticsearch index/datastream to target
            flt(GulpQueryFilter): the filter to use
            options(GulpQueryAdditionalOptions, optional): additional options to use

        """
        user_id = await GulpQuery._get_requestor_user_id(token)
        if not options:
            options = GulpQueryAdditionalOptions()

        dsl = flt.to_opensearch_dsl()
        return await GulpOpenSearch.get_instance().search_dsl(dsl['query'], flt, options)


    @staticmethod
    async def raw_query(token: str, ws_dsl: dict, req_id: str, index: str, user_id: str=None, flt: GulpQueryFilter=None, options: GulpQueryAdditionalOptions=None) -> None:
        if not user_id:
            # check token
            user_id = await GulpQuery._get_requestor_user_id(token)

        if flt:
            # merge with filter
            dsl = flt.merge_to_opensearch_dsl(dsl)

    @staticmethod
    async def sigma_query(sigma: str, plugin: str, referenced_sigma: list[str]=None, flt: GulpQueryFilter=None, options: GulpQueryAdditionalOptions=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def sigma_query_build(sigma: str, plugin: str, referenced_sigma: list[str]=None, flt: GulpQueryFilter=None) -> dict:
        pass

    @staticmethod
    async def stored_query(id: str, flt: GulpQueryFilter=None, options: GulpQueryAdditionalOptions=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def external_query(query: GulpExternalQueryParameters, flt: GulpQueryFilter=None, options: GulpQueryAdditionalOptions=None) -> GulpDocumentsChunk:
        pass

    @staticmethod
    async def external_sigma_query(query: GulpExternalQueryParameters, sigma: str, plugin: str, referenced_sigma: list[str]=None, flt: GulpQueryFilter=None, options: GulpQueryAdditionalOptions=None) -> GulpDocumentsChunk:
        pass
        
