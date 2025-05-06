"""
This module defines the REST API endpoints for managing stories in the Gulp application.

Stories are collections that group together multiple documents, allowing for organization
and association of related content. The module provides endpoints for:

- Creating new stories with associated documents
- Updating existing stories (modifying properties or document associations)
- Deleting stories
- Retrieving individual stories by ID
- Listing stories with optional filtering

All operations require appropriate authentication tokens with the necessary permissions.
"""

from typing import Annotated, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

from gulp.api.collab.highlight import GulpHighlight
from gulp.api.collab.link import GulpLink
from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.story import GulpStory, GulpStoryEntry, GulpStoryHighlightEntry, GulpStoryLinkEntry, GulpStoryNoteEntry
from gulp.api.collab.structs import COLLABTYPE_HIGHLIGHT, COLLABTYPE_LINK, COLLABTYPE_NOTE, GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.query import GulpQueryHelpers
from gulp.api.rest.server_utils import ServerUtils
from gulp.api.rest.structs import APIDependencies

router: APIRouter = APIRouter()

async def _fetch_highlights(
    sess: AsyncSession,
    operation_id: str,
    user_id: str,
    highlight_ids: list[str],    
) -> list[GulpStoryHighlightEntry]:
    """
    fetch highlights
    
    Args:
        sess (AsyncSession): The database session.
        operation_id (str): The operation ID.
        user_id (str): The ID of the user.
        highlight_ids (list[str]): A list of highlight IDs to fetch.
    
    Returns:
        list[dict]: A list of GulpStoryHighlightEntry dictionaries containing metadata about the highlights.
    """
    highlights: list[dict] = []

    flt: GulpCollabFilter = GulpCollabFilter(
        operation_ids=[operation_id],
        ids=highlight_ids,
        types=[COLLABTYPE_HIGHLIGHT])
    
    # fetch highlights using the filter
    h_objs: list[GulpHighlight] = await GulpHighlight.get_by_filter(sess, flt, user_id=user_id, throw_if_not_found=False)
    if not h_objs:
        # empty result
        return []

    for h in h_objs:
        hh = GulpStoryHighlightEntry(
            id=h.id,
            name=h.name,
            time_range=h.time_range,
            source_id=h.source_id,      
        )
        # add the note to the list        
        highlights.append(hh.model_dump(exclude_none=True))

    return highlights

async def _fetch_links_for_document(
    sess: AsyncSession,
    operation_id: str,
    user_id: str,
    doc_id: str,    
) -> list[dict]:
    """
    fetch links for the given document ID.
    
    Args:
        sess (AsyncSession): The database session.
        operation_id (str): The operation ID.
        user_id (str): The ID of the user.
        doc_id (str): The document ID to fetch links for.
    
    Returns:
        list[dict]: A list of GulpStoryLinkEntry dictionaries containing metadata about the links.
    """
    links: list[dict] = []

    flt: GulpCollabFilter = GulpCollabFilter(
        operation_ids=[operation_id],
        doc_id_from=[doc_id],
        types=[COLLABTYPE_LINK])
    
    # fetch notes using the filter
    l_objs: list[GulpLink] = await GulpLink.get_by_filter(sess, flt, user_id=user_id, throw_if_not_found=False)
    if not l_objs:
        # empty result
        return []

    for l in l_objs:
        ll = GulpStoryLinkEntry(
            id=l.id,
            name=l.name,
            doc_ids=l.doc_ids,
        )
        links.append(ll.model_dump(exclude_none=True))

    return links

async def _fetch_notes_for_document(
    sess: AsyncSession,
    operation_id: str,
    user_id: str,
    doc_id: str,    
) -> list[dict]:
    """
    fetch notes for the given document ID.

    Args:
        sess (AsyncSession): The database session.
        operation_id (str): The operation ID.
        user_id (str): The ID of the user.
        doc_id (str): The document ID to fetch notes for.
    
    Returns:
        list[dict]: A list of GulpStoryNoteEntry dictionaries containing metadata about the notes.
    """
    notes: list[dict] = []

    flt: GulpCollabFilter = GulpCollabFilter(
        operation_ids=[operation_id],
        doc_ids=[doc_id],
        types=[COLLABTYPE_NOTE])
    
    # fetch notes using the filter
    n_objs: list[GulpNote] = await GulpNote.get_by_filter(sess, flt, user_id=user_id, throw_if_not_found=False)
    if not n_objs:
        # empty result
        return []

    for n in n_objs:
        # add the note to the list
        nn = GulpStoryNoteEntry(
            id=n.id,
            name=n.name,
            text=n.text)
        notes.append(nn.model_dump(exclude_none=True))
    return notes


async def _gulp_story_entry_from_document(
    sess: AsyncSession,
    operation_id: str,
    user_id: str,
    doc_id: str,
    include_whole_document: bool = False,
) -> dict:
    """
    build a story entry for the given document ID.

    Args:
        sess (AsyncSession): The database session.
        operation_id (str): The operation ID.
        user_id (str): The ID of the user.
        doc_id (str): The document ID to create the story entry for.
        include_whole_document (bool): Whether to include the whole document in the story entry.
    
    Returns:
        dict: A dictionary representing the story entry, including links and notes if any
    """
    
    # fetch the document itself
    doc = await GulpQueryHelpers.query_single(index=operation_id, doc_id=doc_id)

    # fetch links
    links = await _fetch_links_for_document(sess, operation_id, user_id, doc_id)
        
    # fetch notes
    notes = await _fetch_notes_for_document(sess, operation_id, user_id, doc_id)

    # remove original from doc
    doc.pop("event.original",None)
    if doc.get("event.duration", None) == 1:
        # and also remove event.duration if it is 1
        doc.pop("event.duration", None)

    # create the story entry
    gse: GulpStoryEntry = GulpStoryEntry(
        id=doc.pop("_id", None),
        operation_id=doc.pop("gulp.operation_id", None),
        context_id=doc.pop("gulp.context_id", None),
        source_id=doc.pop("gulp.source_id", None),
        event_code=doc.pop("event.code", None),
        agent_type=doc.pop("agent.type", None),
        timestamp=doc.pop("@timestamp", None),
        duration=doc.pop("event.duration", None),
        links=links,
        notes=notes)
    gse = gse.model_dump(exclude_none=True)

    if include_whole_document:
        # add the rest of the document to the story entry
        for k, v in doc.items():
            if not k in gse and v is not None:
                gse[k] = v
 
    return gse
    
@router.post(
    "/story_create",
    tags=["story"],
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
                        "data": GulpStory.example(),
                    }
                }
            }
        }
    },
    summary="creates a story.",
    description="""
creates a story, which provides an overview of the incident through annotated documents, links and highlights.

for each document in `doc_ids`, the related notes and links are retrieved and stored in the story.

if `highlight_ids` are provided, the highlights are also stored in the story.

- `token` needs `edit` permission.
- default `color` is `blue`.
""",
)
async def story_create_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    operation_id: Annotated[
        str,
        Depends(APIDependencies.param_operation_id),
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    name: Annotated[str, Query(description="Story title.")],
    doc_ids: Annotated[
        list[str], Body(description="One or more target document IDs.")
    ],
    highlight_ids: Annotated[
        list[str], Body(description="One or more highlight IDs.")
    ] = None,
    include_whole_documents: Annotated[
        bool,
        Query(
            description="Whether to include whole documents in the story (default=no).",            
        )]=False,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    private: Annotated[bool, Depends(APIDependencies.param_private_optional)] = False,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            # get operation and check acl
            op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
            s = await GulpUserSession.check_token(
                sess, token, obj=op
            )
            user_id = s.user_id

            entries: list[dict] = []
            highlights: list[dict] = []
            if highlight_ids:
                # fetch highlights
                highlights = await _fetch_highlights(sess, operation_id, user_id, highlight_ids)

            # fetch entries
            for doc_id in doc_ids:
                # fetch the document and create the story entry
                entry = await _gulp_story_entry_from_document(sess, operation_id, user_id, doc_id, include_whole_document=include_whole_documents)
                entries.append(entry)

        object_data = GulpStory.build_dict(
            operation_id=operation_id,
            glyph_id=glyph_id,
            description=description,    
            color=color or "blue",
            name=name,
            tags=tags,
            entries=entries,
            highlights=highlights,
        )
        d = await GulpStory.create(
            token,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
            permission=[GulpUserPermission.EDIT],
            private=private,
            operation_id=operation_id,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.patch(
    "/story_update",
    tags=["story"],
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
                        "data": GulpStory.example(),
                    }
                }
            }
        }
    },
    summary="updates an existing story.",
    description="""
for each document in `doc_ids`, the related notes and links are retrieved and stored in the story.

if `highlight_ids` are provided, the highlights are also stored in the story.

- `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
""",
)
async def story_update_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    doc_ids: Annotated[
        list[str], Body(description="One or more target document IDs.")
    ] = None,
    highlight_ids: Annotated[
        list[str], Body(description="One or more highlight IDs.")
    ] = None,
    include_whole_documents: Annotated[
        bool,
        Query(
            description="Whether to include whole documents in the story (default=no).",            
        )]=False,
    description: Annotated[
        str, Depends(APIDependencies.param_description_optional)
    ] = None,
    name: Annotated[str, Depends(APIDependencies.param_display_name_optional)] = None,
    tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
    glyph_id: Annotated[str, Depends(APIDependencies.param_glyph_id_optional)] = None,
    color: Annotated[str, Depends(APIDependencies.param_color_optional)] = None,
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        if not any([doc_ids, highlight_ids, name, description, tags, glyph_id, color]):
            raise ValueError(
                "at least one of doc_ids, name, description, tags, glyph_id, color must be set."
            )
        
        entries: list[dict] = []
        highlights: list[dict] = []
        if doc_ids or highlight_ids:
            async with GulpCollab.get_instance().session() as sess:
                s = await GulpUserSession.check_token(
                    sess, token, permission=GulpUserPermission.EDIT, obj=obj_id
                )
                obj: GulpStory = await GulpStory.get_by_id(sess, obj_id)

                user_id = s.user_id 
                if highlight_ids:
                    # fetch highlights
                    highlights = await _fetch_highlights(sess, obj.operation_id, user_id, highlight_ids)

                # fetch entries
                if doc_ids:
                    for doc_id in doc_ids:
                        # fetch the document and create the story entry
                        entry = await _gulp_story_entry_from_document(sess, obj.operation_id, user_id, doc_id, include_whole_document=include_whole_documents)
                        entries.append(entry)

        # build the object data
        d = {}
        d["entries"] = entries or None
        d["highlights"] = highlights or None
        d["description"] = description
        d["name"] = name
        d["tags"] = tags
        d["glyph_id"] = glyph_id
        d["color"] = color

        d = await GulpStory.update_by_id(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
            d=d,
        )
        return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.delete(
    "/story_delete",
    tags=["story"],
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
    summary="deletes a story.",
    description="""
- `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
""",
)
async def story_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    ServerUtils.dump_params(locals())
    try:
        await GulpStory.delete_by_id(
            token,
            obj_id,
            ws_id=ws_id,
            req_id=req_id,
        )
        return JSendResponse.success(req_id=req_id, data={"id": obj_id})
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.get(
    "/story_get_by_id",
    tags=["story"],
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
                        "data": GulpStory.example(),
                    }
                }
            }
        }
    },
    summary="gets an story.",
)
async def story_get_by_id_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    obj_id: Annotated[str, Depends(APIDependencies.param_object_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        d = await GulpStory.get_by_id_wrapper(
            token,
            obj_id,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex


@router.post(
    "/story_list",
    tags=["story"],
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
                            GulpStory.example(),
                        ],
                    }
                }
            }
        }
    },
    summary="list stories, optionally using a filter.",
    description="",
)
async def story_list_handler(
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
        d = await GulpStory.get_by_filter_wrapper(
            token,
            flt,
        )
        return JSendResponse.success(req_id=req_id, data=d)
    except Exception as ex:
        raise JSendException(req_id=req_id) from ex
