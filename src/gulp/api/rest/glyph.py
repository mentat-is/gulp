"""
This module provides FastAPI endpoints for glyph operations in the GULP API.

It handles the creation, updating, deletion, retrieval, and listing of glyphs
with appropriate authorization checks. Glyphs are images with metadata that can
be stored and managed through this API.

The module includes handlers for:
- Creating new glyphs with images (limited to 16kb)
- Updating existing glyphs
- Deleting glyphs
- Retrieving glyphs by ID
- Listing glyphs with optional filtering

Each endpoint enforces permission checking based on user tokens and
follows JSend response formatting standards.

"""

from typing import Annotated, Union

from fastapi import APIRouter, Depends, File, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse
from pydantic import Field

from gulp.api.collab.glyph import GulpGlyph
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies
from sqlalchemy.ext.asyncio import AsyncSession

router: APIRouter = APIRouter()


def _read_img_file(file: UploadFile) -> bytes:
    """
    reads the image file and returns the data as bytes.

    also checks the file size, which must be less than 16kb.

    Args:
        file (UploadFile): the image file.

    Returns:
        bytes: the image data.
    """
    data = file.file.read()
    if len(data) > 16 * 1024:
        raise ValueError("The file size must be less than 16kb.")
    return data


@router.post(
    "/glyph_create",
    tags=["glyph"],
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
                        "data": GulpGlyph.example(),
                    }
                }
            }
        }
    },
    summary="creates a glyph.",
    description="""
- `token` needs `edit` permission.
- max `img` size is `16kb`, all common image file formats (.png, .jpg, ...) are accepted.
""",
)
async def glyph_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    img: Annotated[UploadFile, File(description="an image file.")],
    private: Annotated[bool, Depends(APIDependencies.param_private)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
    name: Annotated[
        str, Field(description="an optional name, either the uploaded filename is used")
    ] = None,
) -> JSONResponse:
    params = locals()
    params["img"] = img.filename
    ServerUtils.dump_params(params)
    try:
        data = _read_img_file(img)
        d: dict = await GulpGlyph.create(
            token,
            name=name,
            permission=[GulpUserPermission.EDIT],
            private=private,
            img=data,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/glyph_update",
    tags=["glyph"],
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
                        "data": GulpGlyph.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing glyph.",
    description="""
- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
- max `img` size is `16kb`, all common image file formats (.png, .jpg, ...) are accepted.
""",
)
async def glyph_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
    name: Annotated[str, Depends(APIDependencies.param_name)],
    img: Annotated[UploadFile, File(description="optional image file.")] = None,
) -> JSONResponse:
    params = locals()
    params["img"] = img.filename if img else None
    ServerUtils.dump_params(params)
    sess: AsyncSession = None
    try:
        if not any([name, img]):
            raise ValueError("At least one of name or img must be provided.")
        async with GulpCollab.get_instance().session as sess:
            obj: GulpGlyph
            _, obj, _ = await GulpGlyph.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )
            if name:
                obj.name = name
            if img and isinstance(img, UploadFile):
                data = _read_img_file(img)
                obj.img = data

            dd: dict = await obj.update(sess)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        if sess:
            await sess.rollback()
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/glyph_delete",
    tags=["glyph"],
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
    summary="deletes a glyph.",
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def glyph_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpGlyph.delete_by_id_wrapper(
            token,
            obj_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/glyph_get_by_id",
    tags=["glyph"],
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
                        "data": GulpGlyph.example(),
                    }
                }
            }
        }
    },
    summary="gets a glyph.",
)
async def glyph_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    sess: AsyncSession = None
    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpGlyph
            _, obj, _ = await GulpGlyph.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(exclude_none=True)
            )
    except Exception as ex:
        if sess:
            await sess.rollback()
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/glyph_list",
    tags=["glyph"],
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
                            GulpGlyph.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list glyphs, optionally using a filter.",
    description="""
- **using paging through `flt.limit` and `flt.offset` is highly advised to avoid having a large response**.
""",
)
async def glyph_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[GulpCollabFilter, Depends(APIDependencies.param_collab_flt)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)],
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpGlyph.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
