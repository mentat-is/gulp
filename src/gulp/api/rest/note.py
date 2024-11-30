"""
This module contains the REST API for gULP (gui Universal Log Processor).
"""

from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from typing import Annotated
from fastapi import APIRouter, Body, Depends, Header, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.note import GulpNote
from gulp.api.collab.structs import GulpUserPermission
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

        async with ServerUtils.get_collab_session() as sess:
            # needs at least edit permission
            s = await GulpUserSession.check_token(
                sess, token, [GulpUserPermission.EDIT]
            )

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
            n: GulpNote = await GulpNote.create(
                sess, object_data, owner_id=s.user_id, ws_id=ws_id, req_id=req_id
            )
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data=n.to_dict(exclude_none=True))
            )
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
    async with GulpCollab.get_instance().session() as sess:
        # get the note by id
        n: GulpNote = await GulpNote.get_by_id(sess, note_id)
        
        # token needs at least edit permission
        s = await GulpUserSession.check_token(
            sess, token, [GulpUserPermission.EDIT], obj=n
        )

        # ensure only one is set
        if time_pin:
            n.docs = None
            n.time_pin = time_pin
        if docs:
            n.docs = docs
            n.time_pin = None
            
        if text:
            n.text = text
        if name:
            n.name = name
        if tags:
            n.tags = tags
        if glyph_id:
            n.glyph_id = glyph_id
        if color:
            n.color = color
        if private is not None:
            n.private = private

        # ensure note have "docs" or "time_pin" set, not both
        if self.docs and "time_pin" in d:
            self.docs = None
        if self.time_pin and "docs" in d:
            self.time_pin = None
    try:
        #
        if (
            events is None
            and text is None
            and name is None
            and tags is None
            and glyph_id is None
            and color is None
            and private is None
        ):
            raise InvalidArgument(
                "at least one of event_ids, text, name, glyph_id, tags, color, private must be set."
            )

        o = await GulpCollabObject.update(
            await collab_api.session(),
            token,
            req_id,
            note_id,
            ws_id,
            name=name,
            txt=text,
            events=events,
            glyph_id=glyph_id,
            tags=tags,
            data={"color": color} if color is not None else None,
            type=GulpCollabType.NOTE,
            private=private,
        )
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


# import muty.crypto
# import muty.file
# import muty.jsend
# import muty.list
# import muty.log
# import muty.os
# import muty.string
# import muty.uploadfile

# import gulp.api.collab_api as collab_api
# import gulp.api.rest.collab_utility as collab_utility
# import gulp.plugin
# import gulp.structs
# import gulp.utils
# from gulp.api.collab.base import (
#     GulpAssociatedEvent,
#     GulpCollabFilter,
#     GulpCollabLevel,
#     GulpCollabObject,
#     GulpCollabType,
# )
# from gulp.structs import InvalidArgument

# _app: APIRouter = APIRouter()


# @_app.post(
#     "/note_list",
#     tags=["note"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701278479259,
#                         "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
#                         "data": [
#                             "CollabObj",
#                         ],
#                     }
#                 }
#             }
#         }
#     },
#     summary="list notes, optionally using a filter.",
#     description="available filters: id, owner_id, operation_id, context, src_file, name(=name), text, time_start(=pin time), time_end, events, tags, private_only, level, limit, offset.",
# )
# async def note_list_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     flt: Annotated[GulpCollabFilter, Body()] = None,
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)
#     return await collab_utility.collabobj_list(token, req_id, GulpCollabType.NOTE, flt)


# @_app.get(
#     "/note_get_by_id",
#     tags=["note"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701278479259,
#                         "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
#                         "data": {"CollabObj"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="gets a note.",
# )
# async def note_get_by_id_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     note_id: Annotated[int, Query(description="id of the note to be retrieved.")],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)
#     return await collab_utility.collabobj_get_by_id(
#         token, req_id, GulpCollabType.NOTE, note_id
#     )


# @_app.delete(
#     "/note_delete",
#     tags=["note"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701278479259,
#                         "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
#                         "data": {"id": 1},
#                     }
#                 }
#             }
#         }
#     },
#     summary="deletes a note.",
# )
# async def note_delete_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_DELETE_EDIT_TOKEN)],
#     note_id: Annotated[int, Query(description="id of the note to be deleted.")],
#     ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)
#     return await collab_utility.collabobj_delete(
#         token, req_id, GulpCollabType.NOTE, note_id, ws_id=ws_id
#     )


# @_app.put(
#     "/note_update",
#     tags=["note"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701278479259,
#                         "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
#                         "data": {"CollabObj"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="updates an existing note.",
# )
# async def note_update_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
#     note_id: Annotated[int, Query(description="id of the note to be updated.")],
#     ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
#     events: Annotated[list[GulpAssociatedEvent], Body()] = None,
#     text: Annotated[str, Body()] = None,
#     name: Annotated[str, Body()] = None,
#     tags: Annotated[list[str], Body()] = None,
#     glyph_id: Annotated[
#         int, Query(description="optional new glyph ID for the note.")
#     ] = None,
#     color: Annotated[
#         str, Query(description="optional new note color in #rrggbb or css-name format.")
#     ] = None,
#     private: Annotated[bool, Query(description="whether the note is private.")] = None,
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)
#     try:
#         if (
#             events is None
#             and text is None
#             and name is None
#             and tags is None
#             and glyph_id is None
#             and color is None
#             and private is None
#         ):
#             raise InvalidArgument(
#                 "at least one of event_ids, text, name, glyph_id, tags, color, private must be set."
#             )

#         o = await GulpCollabObject.update(
#             await collab_api.session(),
#             token,
#             req_id,
#             note_id,
#             ws_id,
#             name=name,
#             txt=text,
#             events=events,
#             glyph_id=glyph_id,
#             tags=tags,
#             data={"color": color} if color is not None else None,
#             type=GulpCollabType.NOTE,
#             private=private,
#         )
#         return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.post(
#     "/note_create",
#     tags=["note"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701278479259,
#                         "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
#                         "data": {"CollabObj"},
#                     }
#                 }
#             }
#         }
#     },
#     summary='creates a note, associated with events ( { "id": ..., "@timestamp": ..."} ) or pinned at a certain time.',
# )
# async def note_create_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_EDIT_TOKEN)],
#     operation_id: Annotated[
#         int, Query(description="operation to be associated with this note.")
#     ],
#     context: Annotated[
#         str, Query(description="context to be associated with this note.")
#     ],
#     src_file: Annotated[
#         str, Query(description="source file to be associated with this note.")
#     ],
#     ws_id: Annotated[str, Query(description=gulp.structs.API_DESC_WS_ID)],
#     text: Annotated[str, Body()],
#     name: Annotated[str, Body()],
#     time_pin: Annotated[
#         int,
#         Query(description="timestamp to pin the note to."),
#     ] = None,
#     events: Annotated[list[GulpAssociatedEvent], Body()] = None,
#     tags: Annotated[
#         list[str],
#         Body(),
#     ] = None,
#     glyph_id: Annotated[int, Query(description="glyph ID for the new note.")] = None,
#     color: Annotated[
#         str,
#         Query(
#             description='optional color in #rrggbb or css-name format, default is "green".'
#         ),
#     ] = "green",
#     private: Annotated[bool, Query(description=gulp.structs.API_DESC_PRIVATE)] = False,
#     level: Annotated[
#         GulpCollabLevel, Query(description=gulp.structs.API_DESC_COLLAB_LEVEL)
#     ] = GulpCollabLevel.DEFAULT,
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp.utils.ensure_req_id(req_id)

#     try:
#         if events is None and time_pin is None:
#             raise InvalidArgument("events and time_pin cannot be both None.")
#         if events is not None and time_pin is not None:
#             raise InvalidArgument("events and time_pin cannot be both set.")

#         # MutyLogger.get_instance().debug('events=%s' % (events))
#         o = await GulpCollabObject.create(
#             await collab_api.session(),
#             token,
#             req_id,
#             GulpCollabType.NOTE,
#             ws_id=ws_id,
#             operation_id=operation_id,
#             name=name,
#             context=context,
#             src_file=src_file,
#             txt=text,
#             time_start=time_pin,
#             events=events,
#             glyph_id=glyph_id,
#             tags=tags,
#             data={"color": color},
#             private=private,
#             level=level,
#         )
#         return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=o.to_dict()))
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# def router() -> APIRouter:
#     """
#     Returns this module api-router, to add it to the main router

#     Returns:
#         APIRouter: The APIRouter instance
#     """
#     global _app
#     return _app
