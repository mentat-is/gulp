"""
This module provides REST API endpoints for managing links in GULP.

The link endpoints allows creation, update, deletion, retrieval, and listing of links
between documents. Links establish relationships between a source document and one or more
target documents, with customizable properties like name, color, tags, and glyph.

Endpoints:
- POST /link_create: Create a new link between documents
- PATCH /link_update: Update an existing link's properties
- DELETE /link_delete: Delete a link by ID
- GET /link_get_by_id: Retrieve a link by ID
- POST /link_list: List links with optional filtering

Each endpoint requires authentication via a token parameter and returns responses
in JSend format.
"""

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.link import GulpLink
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies

router: APIRouter = APIRouter()


@router.post(
    "/link_create",
    tags=["link"],
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
                        "data": GulpLink.example(),
                    }
                }
            }
        }
    },
    summary="creates a link.",
    description="""
creates a link between a source document and one (or more) target documents.

- `token` needs `edit` permission.
- default `color` is `red`.
""",
)
async def link_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    doc_id_from: Annotated[str, Query(description="the source document ID.")],
    doc_ids: Annotated[list[str], Body(description="One or more target document IDs.")],
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    description: Annotated[str, Depends(APIDependencies.param_description_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        object_data = GulpLink.build_dict(
            operation_id=operation_id,
            glyph_id=glyph_id,
            tags=tags,
            color=color or "red",
            description=description,
            name=name,
            doc_id_from=doc_id_from,
            doc_ids=doc_ids,
        )
        d = await GulpLink.create(
            token,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
            private=private,
            operation_id=operation_id,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/link_update",
    tags=["link"],
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
                        "data": GulpLink.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing link.",
    description="""
- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
""",
)
async def link_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    doc_ids: Annotated[
        list[str], Body(description="One or more target document IDs.")
    ] = None,
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    description: Annotated[str, Depends(APIDependencies.param_description_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([doc_ids, name, description, tags, glyph_id, color]):
            raise ValueError(
                "At least one of doc_ids, name, description, tags, glyph_id, color must be provided."
            )
        d = {}
        d["doc_ids"] = doc_ids
        d["description"] = description
        d["name"] = name
        d["tags"] = tags
        d["glyph_id"] = glyph_id
        d["color"] = color
        d = await GulpLink.update_by_id(
            obj_id,
            token,
            d,
            ws_id=ws_id,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/link_delete",
    tags=["link"],
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
    summary="deletes a link.",
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def link_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpLink.delete_by_id(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/link_get_by_id",
    tags=["link"],
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
                        "data": GulpLink.example(),
                    }
                }
            }
        }
    },
    summary="gets a link.",
)
async def link_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpLink.get_by_id_wrapper(
            token,
            obj_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/link_list",
    tags=["link"],
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
                            GulpLink.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list links, optionally using a filter.",
    description="",
)
async def link_list_handler(
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
        d = await GulpLink.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
