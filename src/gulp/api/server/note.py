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

import muty.time
from fastapi import APIRouter, Body, Depends, File, Query, UploadFile
from fastapi.responses import JSONResponse, Response
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.note import GulpNote, GulpNoteAttachment, GulpNoteEdit
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.structs import GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies

router: APIRouter = APIRouter()
MAX_NOTE_ATTACHMENT_SIZE = 32 * 1024 * 1024


async def _read_note_attachment(attachment: UploadFile) -> bytes:
    """Read one upload while enforcing the 32 MiB attachment limit."""
    data = await attachment.read(MAX_NOTE_ATTACHMENT_SIZE + 1)
    if len(data) > MAX_NOTE_ATTACHMENT_SIZE:
        raise JSendException(
            "note attachment exceeds the 32 MiB limit", status_code=413
        )
    return data


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
        str, Body(description="the text of the note.", examples=["this is a note"])
    ],
    name: Annotated[str, Depends(APIDependencies.param_name)],
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = None,
    time_pin: Annotated[
        int,
        Query(
            description="timestamp to pin the note to, in nanoseconds from the unix epoch, ignored if `docs` is set."
        ),
    ] = 0,
    doc: Annotated[
        GulpBasicDocument,
        Body(
            description="the document associated with the note, ignored if `time_pin` is set."
        ),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["doc"] = str(doc) if doc else "None"
    ServerUtils.dump_params(params)

    try:
        if doc and time_pin:
            raise ValueError("doc and time_pin cannot be both set.")
        if not doc and not time_pin:
            raise ValueError("either doc or time_pin must be set.")

        async with GulpCollab.get_instance().session() as sess:
            # check token on operation
            s: GulpUserSession
            s, _, _ = await GulpOperation.get_by_id_wrapper(
                sess, token, operation_id, GulpUserPermission.EDIT
            )
            user_id: str = s.user.id

            n: GulpNote = await GulpNote.create_internal(
                sess,
                user_id,
                name=name,
                operation_id=operation_id,
                glyph_id=glyph_id,
                tags=tags,
                color=color,
                private=private,
                ws_id=ws_id,
                req_id=req_id,
                context_id=context_id,
                source_id=source_id,
                doc=(doc.model_dump(by_alias=True, exclude_none=True) if doc else None),
                doc_id=(doc.id.lower() if doc else None),
                time_pin=time_pin,
                text=text,
            )
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=n.to_dict(exclude_none=True))
            )
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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    name: Annotated[str, Depends(APIDependencies.param_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    doc: Annotated[
        GulpBasicDocument,
        Body(description="document associated with the note."),
    ] = None,
    time_pin: Annotated[
        int,
        Query(
            description="timestamp to pin the note to, in nanoseconds from the unix epoch."
        ),
    ] = None,
    text: Annotated[
        str, Body(description="note text.", examples=["the new note text"])
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["doc"] = str(doc) if doc else "None"
    ServerUtils.dump_params(params)

    try:
        # we cannot have both docs and time_pin set
        if doc and time_pin:
            raise ValueError("doc and time_pin cannot be both set.")
        if not any([doc, time_pin, text, name, tags, glyph_id, color]):
            raise ValueError(
                "at least one of doc, time_pin, text, name, tags, glyph_id, color must be set."
            )
        async with GulpCollab.get_instance().session() as sess:
            # check permissions on both operation and object
            s: GulpUserSession
            obj: GulpNote
            s, obj, _ = await GulpNote.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )

            # update
            if name:
                obj.name = name
            if tags:
                obj.tags = tags
            if glyph_id:
                obj.glyph_id = glyph_id
            if color:
                obj.color = color
            if text:
                obj.text = text
                obj.text_id = None
                obj.shared_text = None
            # ensure only one in time_pin and docs is set
            if time_pin:
                obj.doc = None
                obj.time_pin = time_pin
            elif doc:
                obj.doc = doc.model_dump(
                    by_alias=True, exclude_none=True, exclude_defaults=True
                )
                obj.doc_id = doc.id.lower()
            # add an edit
            p = GulpNoteEdit(
                user_id=s.user.id, text=text, timestamp=muty.time.now_msec()
            )
            if not obj.edits:
                obj.edits = []
            obj.edits.append(p.model_dump(exclude_none=True))  # trigger ORM update
            obj.last_editor_id = s.user.id

            dd: dict = await obj.update(
                sess, ws_id=ws_id, req_id=req_id, user_id=s.user.id
            )
            return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/note_add_attachment",
    tags=["note"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={413: {"description": "Attachment exceeds the 32 MiB limit."}},
    summary="adds one binary attachment to a note.",
    description="""
Uploads one `multipart/form-data` attachment, limited to 32 MiB.

- `token` needs `edit` access to the note.
- `title` defaults to the uploaded filename.
- `mime_type` defaults to the upload content type, then `application/octet-stream`.
- emits the standard note `collab_update` websocket event.
""",
)
async def note_add_attachment_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    attachment: Annotated[
        UploadFile, File(description="the binary attachment, up to 32 MiB")
    ],
    title: Annotated[
        str | None, Query(description="attachment title; defaults to the filename")
    ] = None,
    mime_type: Annotated[
        str | None,
        Query(description="MIME type override; defaults to the upload content type"),
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["attachment"] = attachment.filename
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            s, note, _ = await GulpNote.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )
            data = await _read_note_attachment(attachment)
            metadata = await GulpNoteAttachment.create_from_upload(
                sess,
                note,
                s.user.id,
                data,
                attachment.filename,
                attachment.content_type,
                title=title,
                mime_type=mime_type,
            )
            await note.update(
                sess,
                ws_id=ws_id,
                req_id=req_id,
                user_id=s.user.id,
            )
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=metadata.to_dict())
            )
    except JSendException:
        raise
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/note_delete_attachment",
    tags=["note"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="deletes one attachment from a note.",
    description="""
- `token` needs `edit` access to the note.
- emits the standard note `collab_update` websocket event.
""",
)
async def note_delete_attachment_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    attachment_id: Annotated[str, Query(description="the attachment ID")],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            s, note, _ = await GulpNote.get_by_id_wrapper(
                sess,
                token,
                obj_id,
                permission=GulpUserPermission.EDIT,
            )
            metadata = next(
                (a for a in note.attachments if a.id == attachment_id), None
            )
            if metadata is None:
                from gulp.structs import ObjectNotFound

                raise ObjectNotFound(
                    f'attachment "{attachment_id}" not found on note "{obj_id}"'
                )
            await GulpNoteAttachment.delete_objects([attachment_id])
            note.attachments.remove(metadata)
            await sess.commit()
            await note.update(
                sess,
                ws_id=ws_id,
                req_id=req_id,
                user_id=s.user.id,
            )
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data={"id": attachment_id, "note_id": obj_id},
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/note_list_attachments",
    tags=["note"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    summary="lists attachment metadata for a note.",
    description="`token` needs read access to the note.",
)
async def note_list_attachments_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            _, note, _ = await GulpNote.get_by_id_wrapper(sess, token, obj_id)
            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id,
                    data=[a.to_dict() for a in note.attachments],
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/note_get_attachment",
    tags=["note"],
    summary="downloads one note attachment.",
    description="`token` needs read access to the note. Returns the raw stored bytes.",
    responses={
        200: {
            "content": {"application/octet-stream": {}},
            "description": "Raw attachment bytes using the stored MIME type.",
        }
    },
)
async def note_get_attachment_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    attachment_id: Annotated[str, Query(description="the attachment ID")],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> Response:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            _, note, _ = await GulpNote.get_by_id_wrapper(sess, token, obj_id)
            metadata = next(
                (a for a in note.attachments if a.id == attachment_id), None
            )
            if metadata is None:
                from gulp.structs import ObjectNotFound

                raise ObjectNotFound(
                    f'attachment "{attachment_id}" not found on note "{obj_id}"'
                )

        from urllib.parse import quote

        from gulp.api.s3_api import GulpS3

        data = await GulpS3.get_instance().download_data(attachment_id)
        return Response(
            content=data,
            media_type=metadata.mime_type,
            headers={
                "Content-Disposition": (
                    "attachment; filename*=utf-8''" + quote(metadata.title, safe="")
                )
            },
        )
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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpNote.delete_by_id_wrapper(
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
    obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())

    try:
        async with GulpCollab.get_instance().session() as sess:
            obj: GulpNote
            _, obj, _ = await GulpNote.get_by_id_wrapper(
                sess,
                token,
                obj_id,
            )
            return JSendResponse.success(
                req_id=req_id, data=obj.to_dict(exclude_none=True)
            )
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
    description="""    
- **using paging through `flt.limit` and `flt.offset` is highly advised to avoid having a large response**.
- `operation_id` is set in `flt.operation_ids` automatically.
""",
)
async def note_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    flt: Annotated[
        GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
    ] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
) -> JSONResponse:
    params = locals()
    params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
    ServerUtils.dump_params(params)
    try:
        d = await GulpNote.get_by_filter_wrapper(
            token,
            flt,
            operation_id=operation_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
