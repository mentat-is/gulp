import json
from typing import Optional, Union

import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.opensearch.structs import (
    GulpQueryFilter,
)

class GulpDocumentsChunk(BaseModel):
    """
    Represents a chunk of GulpDocument dictionaries returned by a query or sent during ingestion.
    """
    docs: list[dict] = Field(
        None,
        description="the documents returned by the query.",
    )
    total_hits: Optional[int] = Field(
        0,
        description="the total number of hits.",
    )
    
class GulpExternalQuery(BaseModel):
    """
    Represents a query to an external system
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
    async def gulp_query(flt: GulpQueryFilter) -> GulpDocumentsChunk:
        pass

    @staticmethod
    async def raw_query(dsl: dict, flt: GulpQueryFilter=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def external_query(query: GulpExternalQuery, flt: GulpQueryFilter=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def sigma_query(sigma: str, plugin: str=None, referenced_sigmas: list[str]=None, flt: GulpQueryFilter=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def external_sigma_query_build(sigma: str, plugin: str=None, referenced_sigmas: list[str]=None, flt: GulpQueryFilter=None) -> GulpDocumentsChunk:
        pass
    
    @staticmethod
    async def stored_query(query_id: int, flt: GulpQueryFilter=None) -> GulpDocumentsChunk:
        pass
    
