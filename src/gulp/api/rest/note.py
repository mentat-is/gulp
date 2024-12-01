"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from typing import Annotated
from fastapi import APIRouter, Body, Depends, Header, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.note import GulpNote
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpCollabObject,
    GulpUserPermission,
)
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.rest.server_utils import (
    APIDependencies,
    ServerUtils,
)

router: APIRouter = APIRouter()


@router.post(
    "/note_create",
    tags=["note"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="creates a note.",
    description="""
creates a note on the collab database.

- `token` needs **edit** permission.

- a note can be pinned at a certain time (via the `time_pin` parameter) or associated with documents (via the `docs` parameter).    
""",
)
async def note_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    context_id: Annotated[
        str,
        Depends(APIDependencies.param_context_id),
    ],
    source_id: Annotated[
        str,
        Depends(APIDependencies.param_source_id),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    text: Annotated[
        str, Body(description="the text of the note.", example="this is a note")
    ],
    time_pin: Annotated[
        int,
        Query(
            description="timestamp to pin the note to, in nanoseconds from the unix epoch, ignored if `docs` is set."
        ),
    ] = 0,
    docs: Annotated[
        list[GulpBasicDocument],
        Body(
            description="the documents associated with the note, ignored if `time_pin` is set."
        ),
    ] = None,
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if docs and time_pin:
            raise ValueError("docs and time_pin cannot be both set.")
        if not docs and not time_pin:
            raise ValueError("either docs or time_pin must be set.")

        object_data = GulpNote.build_dict(
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            glyph_id=glyph_id,
            tags=tags,
            color=color or "yellow",
            name=name,
            private=private,
            docs=docs,
            time_pin=time_pin,
            text=text,
        )
        d = await GulpCollabObject.create(
            GulpNote,
            token,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.patch(
    "/note_update",
    tags=["note"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="updates an existing note.",
)
async def note_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    note_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    docs: Annotated[
        list[GulpBasicDocument],
        Body(description="documents to be associated with the note."),
    ] = None,
    time_pin: Annotated[
        int,
        Body(
            description="timestamp to pin the note to, in nanoseconds from the unix epoch."
        ),
    ] = None,
    text: Annotated[
        str, Body(description="note text.", example="the newnote text")
    ] = None,
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    # we cannot have both docs and time_pin set
    if docs and time_pin:
        raise ValueError("docs and time_pin cannot be both set.")
    try:
        prev_text = None
        async with GulpCollab.get_instance().session() as sess:
            # get the previous text of the note first
            n: GulpNote = await GulpNote.get_by_id(sess, note_id, with_for_update=True)
            prev_text: str = n.text

        # ensure only one in time_pin and docs is set
        if time_pin:
            d["docs"] = None
            d["time_pin"] = time_pin
        else:
            d["docs"] = [
                doc.model_dump(exclude_none=True, exclude_defaults=True) for doc in docs
            ]
            d["time_pin"] = None
        if text:
            # also save previous text
            d["previous_text"] = prev_text
            d["text"] = text

        d["name"] = name
        d["tags"] = tags
        d["glyph_id"] = glyph_id
        d["color"] = color
        d["private"] = private
        d = await GulpCollabObject.update_by_id(
            GulpNote,
            token,
            note_id,
            ws_id=ws_id,
            req_id=req_id,
            d=d,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.delete(
    "/note_delete",
    tags=["note"],
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
    summary="deletes a note.",
)
async def note_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    note_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpCollabObject.delete_by_id(
            GulpNote,
            token,
            note_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": note_id})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/note_get_by_id",
    tags=["note"],
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
                        "data": {"CollabObj"},
                    }
                }
            }
        }
    },
    summary="gets a note.",
)
async def note_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    note_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpCollabObject.get_by_id(
            GulpNote,
            token,
            note_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.post(
    "/note_list",
    tags=["note"],
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
                            {"CollabObj"},
                        ],
                    }
                }
            }
        }
    },
    summary="list notes, optionally using a filter.",
    description="",
)
async def note_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpCollabObject.get_by_filter(
            GulpNote,
            token,
            flt,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex
