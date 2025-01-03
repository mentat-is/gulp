"""
gulp glyphs management rest api
"""

from muty.jsend import JSendException, JSendResponse
from typing import Annotated, Optional
from fastapi import APIRouter, Depends, File, UploadFile
from fastapi.responses import JSONResponse
from gulp.api.collab.glyph import GulpGlyph
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
)
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from gulp.api.rest.structs import APIDependencies

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
    name: Annotated[
        str,
        Depends(APIDependencies.param_display_name_optional),
    ] = None,
    private: Annotated[
        bool, Depends(APIDependencies.param_private_optional)
    ] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["img"] = img.filename
    ServerUtils.dump_params(params)
    try:
        data = _read_img_file(img)
        d = {
            "name": name,
            "img": data,
        }

        d = await GulpGlyph.create(
            token,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            object_data=d,
            permission=[GulpUserPermission.EDIT],
            private=private,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    img: Annotated[Optional[UploadFile], File(description="an image file.")] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["img"] = img.filename if img else None
    ServerUtils.dump_params(params)
    try:
        if not any([name, img]):
            raise ValueError("At least one of name or img must be provided.")
        d = {}
        if name:
            d["name"] = name
        if img:
            data = _read_img_file(img)
            d["img"] = data
        d = await GulpGlyph.update_by_id(
            token,
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
            d=d,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpGlyph.delete_by_id(
            token,
            object_id,
            ws_id=None,  # do not propagate on the websocket
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": object_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    object_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpGlyph.get_by_id_wrapper(
            token,
            object_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


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
    description="",
)
async def glyph_list_handler(
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
        d = await GulpGlyph.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
