"""
Module for handling stored query operations through REST API.

This module provides endpoints for creating, updating, deleting, retrieving, and listing
stored queries. Stored queries are reusable queries that can be shared with other users.

The module includes the following endpoints:
- POST /stored_query_create: Create a new stored query
- PATCH /stored_query_update: Update an existing stored query
- DELETE /stored_query_delete: Delete a stored query
- GET /stored_query_get_by_id: Get a specific stored query by ID
- POST /stored_query_list: List stored queries with optional filtering

Stored queries can be either Sigma rules (YAML format) or Gulp raw queries (JSON format).
When using Sigma rules, tags are automatically extracted from the rule's tags and level.
"""

from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.stored_query import GulpStoredQuery
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.opensearch.sigma import sigma_to_tags
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from gulp.structs import GulpPluginParameters

router: APIRouter = APIRouter()


@router.post(
    "/stored_query_create",
    tags=["stored_query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": GulpStoredQuery.example(),
                    }
                }
            }
        }
    },
    summary="creates a stored_query.",
    description="""
creates a stored query.

a stored query is a *reusable* query which may be shared with other users.

- `token` needs `edit` permission.
- `q` is either a YAML string if `q` is a `sigma rule` or a `JSON` string if `q` is a `gulp raw query`.
- if `q` represents a `sigma rule`, `plugin` must be set to the plugin implementing `sigma_convert` to be used for conversion.
- if `q` is a `sigma rule`, `tags` are extracted from the rule `tags` and `level` (provided `tags` are also added, if any).
""",
)
async def stored_query_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    name: Annotated[str, Depends(APIDependencies.param_display_name)],
    q: Annotated[
        str,
        Body(
            description="a query as string, may be YAML (sigma rule) or JSON string (gulp raw query)."
        ),
    ],
    q_groups: Annotated[
        Optional[list[str]],
        Body(
            description="if set, one or more `query groups` to associate with this query.",
        ),
    ] = None,
    plugin: Annotated[
        str,
        Query(
            description="If `q` is a sigma YAML, this is the plugin implementing `sigma_convert` to be used for conversion."
        ),
    ] = None,
    plugin_params: Annotated[
        Optional[GulpPluginParameters],
        Body(
            description="if set, a dictionary of parameters to be passed to the plugin."
        ),
    ] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)

    if plugin:
        # it's a sigma rule, get tags
        sigma_tags = await sigma_to_tags(plugin, q, plugin_params)
        if tags:
            # add to provided tags
            for tt in sigma_tags:
                if tt not in tags:
                    tags.append(tt)
        else:
            tags = sigma_tags

    try:
        object_data = {
            "name": name,
            "q": q,
            "q_groups": q_groups,
            "plugin": plugin,
            "plugin_params": (
                plugin_params.model_dump(exclude_none=True) if plugin_params else None
            ),
            "tags": tags,
            "description": description,
            "glyph_id": glyph_id,
        }
        d = await GulpStoredQuery.create(
            token,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            object_data=object_data,
            private=private,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/stored_query_update",
    tags=["stored_query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": GulpStoredQuery.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing stored_query.",
    description="""
- `tags` is ignored if q is a `sigma rule`: tags are extracted from the rule `tags` and `level`.
- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
""",
)
async def stored_query_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)],
    q: Annotated[
        str,
        Body(
            description="a query as string, may be YAML (sigma rule) or JSON string (gulp raw query)."
        ),
    ],
    q_groups: Annotated[
        Optional[list[str]],
        Body(
            description="if set, one or more `query groups` to associate with this query.",
        ),
    ] = None,
    plugin: Annotated[
        str,
        Query(
            description="If `q` is a sigma YAML, this is the plugin implementing `sigma_convert` to be used for conversion."
        ),
    ] = None,
    plugin_params: Annotated[
        Optional[GulpPluginParameters],
        Body(
            description="if set, a dictionary of parameters to be passed to the plugin."
        ),
    ] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["plugin_params"] = (
        plugin_params.model_dump(exclude_none=True) if plugin_params else None
    )
    ServerUtils.dump_params(params)
    try:
        if not any([q, q_groups, tags, description, glyph_id, plugin]):
            raise ValueError(
                "At least one of q, q_groups, tags, description, glyph_id, plugin must be provided."
            )

        # get rule
        r: dict = await GulpStoredQuery.get_by_id_wrapper(
            token,
            obj_id,
            permission=[GulpUserPermission.EDIT],
        )
        if plugin and not r.get("plugin"):
            raise ValueError(
                "Cannot set plugin for a stored raw query, delete rule and recreate instead."
            )
        if plugin and r.get("plugin") != plugin:
            raise ValueError(
                "Cannot change plugin for a stored sigma query, delete query and recreate instead."
            )

        # handle tags if q is a sigma rule
        existing_tags = []
        if q and plugin:
            # it's a sigma rule, get tags and replace
            sigma_tags = await sigma_to_tags(plugin, q, plugin_params)
            existing_tags = sigma_tags
        else:
            if tags:
                existing_tags = tags

        d = {}
        d["tags"] = existing_tags

        if name:
            d["name"] = name
        if q:
            d["q"] = q
        if q_groups:
            d["q_groups"] = q_groups
        if description:
            d["description"] = description
        if glyph_id:
            d["glyph_id"] = glyph_id
        if plugin:
            d["plugin"] = plugin
        if plugin_params:
            d["plugin_params"] = plugin_params.model_dump(exclude_none=True)

        d = await GulpStoredQuery.update_by_id(
            token,
            obj_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            d=d,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/stored_query_delete",
    tags=["stored_query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": {"id": "obj_id"},
                    }
                }
            }
        }
    },
    summary="deletes a stored_query.",
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def stored_query_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpStoredQuery.delete_by_id(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/stored_query_get_by_id",
    tags=["stored_query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": GulpStoredQuery.example(),
                    }
                }
            }
        }
    },
    summary="gets a stored_query.",
)
async def stored_query_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpStoredQuery.get_by_id_wrapper(
            token,
            obj_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/stored_query_list",
    tags=["stored_query"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701278479259,
                        "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                        "data": [
                            GulpStoredQuery.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list stored_queries, optionally using a filter.",
    description="",
)
async def stored_query_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpStoredQuery.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
