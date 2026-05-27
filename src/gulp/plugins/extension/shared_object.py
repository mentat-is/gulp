from typing import Annotated, Literal, Optional, override

from fastapi import Body, Depends, Query
from fastapi.responses import JSONResponse
from git import exc
from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger
from sqlalchemy import ARRAY, Boolean, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.sigma import sigma_to_tags
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import GulpRedisBroker
from gulp.plugin import GulpPluginBase, GulpPluginType

COLLABTYPE_SHARED_OBJECT = "shared_object"


class Plugin(GulpPluginBase):

    class GulpSharedObject(GulpCollabBase, type=COLLABTYPE_SHARED_OBJECT):
        """
        a shared object in the gulp collaboration system
        """

        __table_args__ = {"extend_existing": True}

        obj_type: Mapped[str] = mapped_column(
            String,
            doc="the type of the shared object, e.g. 'query', 'dashboard', etc. This is used for informational purposes and does not affect behavior.",
        )

        obj: Mapped[dict] = mapped_column(
            MutableDict.as_mutable(JSONB),
            default_factory=dict,
            doc="The shared object data, customizable by the client and use case. For example, it could be a raw query dict, a dashboard definition, a notebook content, etc.",
        )

        @override
        @classmethod
        def example(cls) -> dict:
            d = super().example()
            d["obj"] = {"key": "value"}
            d["obj_type"] = "something"
            return d

    def __init__(
        self,
        path: str,
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, module_name, pickled, **kwargs)

        if self.is_running_in_main_process():
            # in the main process, add the API route during initialization

            # create
            GulpServer.get_instance().add_api_route(
                "/shared_object_create",
                self.shared_object_create_handler,
                methods=["POST"],
                tags=["shared_object"],
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
                                    "data": Plugin.GulpSharedObject.example(),
                                }
                            }
                        }
                    }
                },
                summary="creates a shared_object.",
                description="""
            creates a shared object.

            a shared object is a *reusable* object which may be shared with other users.

            - `token` needs `edit` permission.
            """,
            )

            # update
            GulpServer.get_instance().add_api_route(
                "/shared_object_update",
                self.shared_object_update_handler,
                methods=["PATCH"],
                tags=["shared_object"],
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
                                    "data": Plugin.GulpSharedObject.example(),
                                }
                            }
                        }
                    }
                },
                summary="updates an existing shared_object.",
                description="""
            - `token` needs `edit` permission (or be the owner of the object, or admin) to update the object.
            """,
            )

            # delete
            GulpServer.get_instance().add_api_route(
                "/shared_object_delete",
                self.shared_object_delete_handler,
                methods=["DELETE"],
                tags=["shared_object"],
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
                summary="deletes a shared_object.",
                description="""
            - `token` needs either to have `delete` permission, or be the owner of the object, or be an admin.
            """,
            )

            # get
            GulpServer.get_instance().add_api_route(
                "/shared_object_get_by_id",
                self.shared_object_get_by_id_handler,
                methods=["GET"],
                tags=["shared_object"],
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
                                    "data": Plugin.GulpSharedObject.example(),
                                }
                            }
                        }
                    }
                },
                summary="gets a shared object.",
            )

            # list
            GulpServer.get_instance().add_api_route(
                "/shared_object_list",
                self.shared_object_list_handler,
                methods=["POST"],
                tags=["shared_object"],
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
                                        Plugin.GulpSharedObject.example(),
                                    ],
                                }
                            }
                        }
                    }
                },
                summary="list shared objects, optionally using a filter.",
                description="""
                - **using paging through `flt.limit` and `flt.offset` is highly advised to avoid having a large response**.
                """,
            )

        else:
            MutyLogger.get_instance().debug("initialized in worker process")

    @override
    def desc(self) -> str:
        return "implement shared objects: reusable objects that can be shared between users."

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "Shared objects"

    async def shared_object_create_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        name: Annotated[str, Depends(APIDependencies.param_name)],
        obj_type: Annotated[
            str,
            Query(
                description="the type of the shared object, depending on the use case, e.g. 'query', 'dashboard', 'notebook', 'other'."
            ),
        ],
        obj: Annotated[
            dict,
            Body(
                description="the object itself, as a dict. The content and structure of this dict is opaque to the system and is determined by the client and use case. For example, it could be a raw query dict, a dashboard definition, a notebook content, etc."
            ),
        ],
        tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
        description: Annotated[
            str, Depends(APIDependencies.param_description_optional)
        ] = None,
        glyph_id: Annotated[
            str, Depends(APIDependencies.param_glyph_id_optional)
        ] = None,
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> JSONResponse:
        params = locals()
        params.pop("self")
        ServerUtils.dump_params(params)

        try:
            d = await Plugin.GulpSharedObject.create(
                token,
                name=name,
                tags=tags,
                description=description,
                glyph_id=glyph_id,
                private=False,
                permission=[GulpUserPermission.EDIT],
                obj=obj,
                obj_type=obj_type,
            )
            return JSONResponse(JSendResponse.success(req_id=req_id, data=d))
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    async def shared_object_update_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
        name: Annotated[str, Depends(APIDependencies.param_name_optional)] = None,
        obj: Annotated[
            dict,
            Body(
                description="the updated shared object as a dict. The content and structure of this dict is opaque to the system and is determined by the client and use case. For example, it could be a raw query dict, a dashboard definition, a notebook content, etc."
            ),
        ] = None,
        tags: Annotated[list[str], Depends(APIDependencies.param_tags_optional)] = None,
        description: Annotated[
            str, Depends(APIDependencies.param_description_optional)
        ] = None,
        glyph_id: Annotated[
            str, Depends(APIDependencies.param_glyph_id_optional)
        ] = None,
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> JSONResponse:
        params = locals()
        params.pop("self")
        ServerUtils.dump_params(params)

        try:
            # check license first
            self.check_license()
            if not any([obj, tags, description, glyph_id, name]):
                raise ValueError(
                    "At least one of obj, name, tags, description, glyph_id, must be provided."
                )

            async with GulpCollab.get_instance().session() as sess:
                # check if shared object exists
                s_obj: Plugin.GulpSharedObject
                _, s_obj, _ = await Plugin.GulpSharedObject.get_by_id_wrapper(
                    sess, token, obj_id, permission=[GulpUserPermission.EDIT]
                )

                if tags:
                    s_obj.tags = tags
                if name:
                    s_obj.name = name
                if obj:
                    s_obj.obj = obj
                if description:
                    s_obj.description = description
                if glyph_id:
                    s_obj.glyph_id = glyph_id

                dd: dict = await s_obj.update(sess)
                return JSONResponse(JSendResponse.success(req_id=req_id, data=dd))
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    async def shared_object_delete_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
        ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> JSONResponse:
        params = locals()
        params.pop("self")
        ServerUtils.dump_params(params)

        try:
            await Plugin.GulpSharedObject.delete_by_id_wrapper(
                token,
                obj_id,
                ws_id=ws_id,
                req_id=req_id,
            )
            return JSendResponse.success(req_id=req_id, data={"id": obj_id})
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    async def shared_object_get_by_id_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        obj_id: Annotated[str, Depends(APIDependencies.param_obj_id)],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> JSendResponse:
        params = locals()
        params.pop("self")
        ServerUtils.dump_params(params)
        try:
            async with GulpCollab.get_instance().session() as sess:
                s_obj: Plugin.GulpSharedObject
                _, s_obj, _ = await Plugin.GulpSharedObject.get_by_id_wrapper(
                    sess,
                    token,
                    obj_id,
                )
                return JSendResponse.success(
                    req_id=req_id, data=s_obj.to_dict(exclude_none=True)
                )
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    async def shared_object_list_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        flt: Annotated[
            GulpCollabFilter, Depends(APIDependencies.param_collab_flt_optional)
        ] = None,
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> JSONResponse:
        params = locals()
        params["flt"] = flt.model_dump(exclude_none=True, exclude_defaults=True)
        params.pop("self")
        ServerUtils.dump_params(params)

        try:
            d = await Plugin.GulpSharedObject.get_by_filter_wrapper(
                token,
                flt,
            )
            return JSendResponse.success(req_id=req_id, data=d)
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    @override
    async def post_init(self, **kwargs):
        MutyLogger.get_instance().debug("creating shared objects table ...")
        await GulpCollab.get_instance().create_table(Plugin.GulpSharedObject.__table__)

        # this object must be broadcasted
        GulpRedisBroker.get_instance().add_broadcast_type(COLLABTYPE_SHARED_OBJECT)
