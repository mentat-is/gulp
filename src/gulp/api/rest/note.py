"""
This module implements REST API endpoints for managing notes in the Gulp system.

Notes are text annotations that can be associated with documents or pinned to a specific time.
They can be created, updated, retrieved, and deleted through the provided API endpoints.

Endpoints:
- POST /note_create: Create a new note
- PATCH /note_update: Update an existing note
- DELETE /note_delete: Delete a note
- GET /note_get_by_id: Retrieve a note by ID
- POST /note_list: List notes with optional filtering

Notes have various attributes including text content, color, associated documents,
time pinning, tags, and permissions.
"""

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
import muty.time
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.note import GulpNote, GulpNoteEdit
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies

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
                        "data": GulpNote.example(),
                    }
                }
            }
        }
    },
    summary="creates a note related to one (or more) documents, or pinned at a certain time.",
    description="""
creates a note.

- `token` needs `edit` permission.
- a note can be pinned at a certain time via the `time_pin` parameter, or associated with one or more documents via the `docs` parameter.
- default `color` is `yellow`.
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
    params = locals()
    params["docs"] = "%d documents" % (len(docs) if docs else 0)
    ServerUtils.dump_params(params)

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
            docs=docs,
            time_pin=time_pin,
            text=text,
        )
        d = await GulpNote.create(
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
                        "data": GulpNote.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing note.",
    description="""
- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
""",
)
async def note_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    docs: Annotated[
        list[GulpBasicDocument],
        Body(description="documents to be associated with the note."),
    ] = None,
    time_pin: Annotated[
        int,
        Query(
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
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    params["docs"] = "%d documents" % (len(docs) if docs else 0)
    ServerUtils.dump_params(params)

    try:
        # we cannot have both docs and time_pin set
        if docs and time_pin:
            raise ValueError("docs and time_pin cannot be both set.")
        if not any([docs, time_pin, text, name, tags, glyph_id, color]):
            raise ValueError(
                "at least one of docs, time_pin, text, name, tags, glyph_id, color must be set."
            )
        async with GulpCollab.get_instance().session() as sess:
            n: GulpNote = await GulpNote.get_by_id(sess, obj_id)
            print("---> existing note: %s" % (n))
            
            s = await GulpUserSession.check_token(
                sess, token, permission=GulpUserPermission.EDIT, obj=n
            )

            d = {}

            # ensure only one in time_pin and docs is set
            if time_pin:
                d["docs"] = None
                d["time_pin"] = time_pin
            if docs:
                d["docs"] = [
                    doc.model_dump(
                        by_alias=True, exclude_none=True, exclude_defaults=True
                    )
                    for doc in docs
                ]
                d["time_pin"] = 0

            # update
            d["name"] = name
            d["tags"] = tags
            d["text"] = text
            d["glyph_id"] = glyph_id
            d["color"] = color
            d["last_editor_id"] = s.user_id
            time_updated = muty.time.now_msec()
            d["time_updated"] = time_updated

            # add an edit
            p = GulpNoteEdit(
                user_id=s.user_id, text=text, timestamp=time_updated
            )
            d["edits"] = n.edits or []
            d["edits"].append(p.model_dump(exclude_none=True))
            dd: dict = await n.update(sess, d=d, ws_id=ws_id, user_id=s.user_id, req_id=req_id)
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def note_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpNote.delete_by_id(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                        "data": GulpNote.example(),
                    }
                }
            }
        }
    },
    summary="gets a note.",
)
async def note_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpNote.get_by_id_wrapper(
            token,
            obj_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


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
                            GulpNote.example(),
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
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpNote.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
