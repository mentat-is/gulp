import os
import base64
import muty.file
import gulp.config
import gulp.gulp
import gulp.plugin

from muty.jsend import JSendException, JSendResponse
from typing import Annotated
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.rest.server_utils import (
    ServerUtils,
)
from gulp.api.collab.structs import (
    GulpCollabFilter,
    GulpUserPermission,
    MissingPermission,
)

from gulp.api.rest.structs import APIDependencies
from gulp.api.rest.test_values import TEST_REQ_ID

router: APIRouter = APIRouter()


@router.post(
    "/request_cancel",
    tags=["request"],
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
                        "data": {"id": "test_req"},
                    }
                }
            }
        }
    },
    summary="cancel a request.",
    description="""
set a running request `status` to `CANCELED`.

- `token` needs `admin` permission or to be the owner of the request.
""",
)
async def request_cancel_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id_to_cancel: Annotated[
        str, Query(description="request id to cancel.", example=TEST_REQ_ID)
    ],
    ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSONResponse:
    params = locals()
    ServerUtils.dump_params(params)
    try:
        async with GulpCollab.get_instance().session() as sess:
            stats: GulpIngestionStats = await GulpIngestionStats.get_by_id(
                sess, req_id_to_cancel
            )
            s = await GulpUserSession.check_token(
                sess, token, obj=stats, enforce_owner=True
            )
            await stats.cancel(sess, ws_id, s.user_id)
        return JSendResponse.success(req_id=req_id, data={"id": req_id_to_cancel})
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/plugin_list",
    tags=["plugin_utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1734609216840,
                        "req_id": "8494de7b-e722-437a-a1c7-80341a0f3b27",
                        "data": [
                            {
                                "display_name": "csv",
                                "type": "ingestion",
                                "desc": "generic CSV file processor",
                                "filename": "csv",
                                "sigma_support": [],
                                "additional_parameters": [
                                    {
                                        "name": "delimiter",
                                        "type": "str",
                                        "default_value": ",",
                                        "desc": "delimiter for the CSV file",
                                        "required": False,
                                    }
                                ],
                                "depends_on": [],
                                "tags": [],
                                "version": "1.0",
                            },
                            {
                                "display_name": "raw",
                                "type": "ingestion",
                                "desc": "Raw events ingestion plugin",
                                "filename": "raw",
                                "sigma_support": [],
                                "additional_parameters": [
                                    {
                                        "name": "ignore_mapping",
                                        "type": "bool",
                                        "default_value": False,
                                        "desc": "if set, mapping will be ignored and fields in the resulting GulpDocuments will be ingested as is. (default: False, mapping works as usual and unmapped fields will be prefixed with 'gulp.unmapped')",
                                        "required": False,
                                    }
                                ],
                                "depends_on": [],
                                "tags": [],
                                "version": "1.0",
                            },
                            {
                                "display_name": "csv",
                                "type": "ingestion",
                                "desc": "stacked plugin on top of csv example",
                                "filename": "stacked_example",
                                "sigma_support": [],
                                "additional_parameters": [],
                                "depends_on": [],
                                "tags": [],
                                "version": "1.0",
                            },
                            {
                                "display_name": "win_evtx",
                                "type": "ingestion",
                                "desc": "Windows EVTX log file processor.",
                                "filename": "win_evtx",
                                "sigma_support": [
                                    {
                                        "backends": [
                                            {
                                                "name": "opensearch",
                                                "description": "OpenSearch Lucene backend for pySigma",
                                            }
                                        ],
                                        "pipelines": [
                                            {
                                                "name": "ecs_windows",
                                                "description": "ECS Mapping for windows event logs ingested with Winlogbeat or Gulp.",
                                            },
                                            {
                                                "name": "ecs_windows_old",
                                                "description": "ECS Mapping for windows event logs ingested with Winlogbeat<=6.x",
                                            },
                                        ],
                                        "output_formats": [
                                            {
                                                "name": "dsl_lucene",
                                                "description": "DSL with embedded Lucene queries.",
                                            }
                                        ],
                                    }
                                ],
                                "additional_parameters": [],
                                "depends_on": [],
                                "tags": [],
                                "version": "1.0",
                            },
                            {
                                "display_name": "extension_example",
                                "type": "extension",
                                "desc": "Extension example.",
                                "filename": "example",
                                "sigma_support": [],
                                "additional_parameters": [],
                                "depends_on": [],
                                "tags": [],
                                "version": "1.0",
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="list available plugins.",
)
async def plugin_list_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    ServerUtils.dump_params(locals())
    try:
        async with GulpCollab.get_instance().session() as sess:
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)

            l = await gulp.plugin.GulpPluginBase.list()
            return JSONResponse(JSendResponse.success(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.get(
    "/plugin_get",
    tags=["plugin_utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701546711919,
                        "req_id": "ddfc094f-4a5b-4a23-ad1c-5d1428b57706",
                        "data": {"win_evt.py": "base64 file content here"},
                    }
                }
            }
        }
    },
    summary="get plugin content (i.e. for editing and reupload).",
)
async def plugin_get_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    plugin: Annotated[
        str,
        Query(
            description='filename of the plugin to retrieve content for, i.e. "plugin.py", "paid/paid_plugin.py"'
        ),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    try:
        async with GulpCollab.get_instance().session() as sess:
            # should only admins be able to read all( including paid) plugins?
            await GulpUserSession.check_token(sess, token, GulpUserPermission.READ)
            path_plugins = gulp.config.GulpConfig.get_instance().path_plugins()
            file_path = muty.file.safe_path_join(
                path_plugins, plugin, allow_relative=True
            )

            # read file content
            f = await muty.file.read_file_async(file_path)
            filename = os.path.basename(file_path)

            return JSONResponse(
                JSendResponse.success(
                    req_id=req_id, data={filename: base64.b64encode(f).decode()}
                )
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@router.delete(
    "/plugin_delete",
    tags=["plugin_utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1701266243057,
                        "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
                        "data": {"filename": "win_evtx.py"},
                    }
                }
            }
        }
    },
    summary="deletes an existing plugin file.",
)
async def plugin_delete_handler(
    token: Annotated[str, Depends(APIDependencies.param_token)],
    plugin: Annotated[
        str,
        Query(
            description='filename of the plugin to be deleted, i.e. "plugin.py", "paid/paid_plugin.py"'
        ),
    ],
    req_id: Annotated[str, Depends(APIDependencies.ensure_req_id)] = None,
) -> JSendResponse:
    try:
        async with GulpCollab.get_instance().session() as sess:
            # should only admins be able to read all( including paid) plugins?
            await GulpUserSession.check_token(sess, token, GulpUserPermission.ADMIN)

            path_plugins = gulp.config.GulpConfig.get_instance().path_plugins()
            file_path = muty.file.safe_path_join(
                path_plugins, plugin, allow_relative=True
            )

            # delete file
            await muty.file.delete_file_or_dir_async(file_path, ignore_errors=False)
            return JSONResponse(
                JSendResponse.success(req_id=req_id, data={"filename": plugin})
            )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.post(
#     "/plugin_upload",
#     tags=["plugin_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701266243057,
#                         "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
#                         "data": {"filename": "custom_plugin.py"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="upload a .py plugin file.",
#     description="file will be uploaded to the `gulp/plugins` directory (which can be overridden by `PATH_PLUGINS` environment variable)",
# )
# async def plugin_upload_handler(
#     token: Annotated[
#         str, Header(description=gulp.structs.API_DESC_TOKEN + " with EDIT permission")
#     ],
#     plugin: Annotated[UploadFile, File(description="the plugin file")],
#     filename: Annotated[
#         str,
#         Query(
#             description='the filename to save the plugin as, i.e. "plugin.py", "paid/paid_plugin.py"'
#         ),
#     ],
#     plugin_type: Annotated[
#         gulp.structs.GulpPluginType,
#         Query(description=gulp.structs.API_DESC_PLUGIN_TYPE),
#     ] = gulp.structs.GulpPluginType.INGESTION,
#     allow_overwrite: Annotated[
#         bool, Query(description="if set, will overwrite an existing plugin file.")
#     ] = True,
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(
#             await collab_api.session(), token, GulpUserPermission.EDIT
#         )
#         path_plugins = GulpConfig.get_instance().path_plugins(plugin_type)
#         plugin_path = muty.file.safe_path_join(
#             path_plugins, filename, allow_relative=True
#         )

#         if not filename.lower().endswith(".py") and not filename.lower().endswith(
#             ".pyc"
#         ):
#             raise gulp.structs.InvalidArgument("plugin must be a .py/.pyc file.")

#         if not allow_overwrite:
#             # overwrite disabled
#             if os.path.exists(plugin_path):
#                 raise gulp.structs.ObjectAlreadyExists(
#                     "plugin %s already exists." % (filename)
#                 )

#         # ok, write file
#         await muty.uploadfile.to_path(plugin, dest_dir=os.path.dirname(plugin_path))
#         return JSONResponse(
#             muty.jsend.success_jsend(req_id=req_id, data={"filename": filename})
#         )
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.get(
#     "/plugin_tags",
#     tags=["plugin_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1716386497525,
#                         "req_id": "99ca9c5b-7f84-4181-87bc-9ffc2064406b",
#                         "data": ["process", "file" "network"],
#                     }
#                 }
#             }
#         }
#     },
#     summary="get tags for the given plugin, if they are set: this allow to better identify plugin capabilities.",
# )
# async def plugin_tags_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     plugin: Annotated[str, Query(description=gulp.structs.API_DESC_PLUGIN)],
#     plugin_type: Annotated[
#         gulp.structs.GulpPluginType,
#         Query(description=gulp.structs.API_DESC_PLUGIN_TYPE),
#     ] = gulp.structs.GulpPluginType.INGESTION,
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:

#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(await collab_api.session(), token)
#         l = await gulp.plugin.get_plugin_tags(plugin, plugin_type)
#         return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.get(
#     "/version",
#     tags=["utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701266243057,
#                         "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
#                         "data": {"version": "gulp v0.0.9 (muty v0.2)"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="get gULP version.",
# )
# async def get_version_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(await collab_api.session(), token)
#         return JSONResponse(
#             muty.jsend.success_jsend(
#                 req_id=req_id, data={"version": gulp_utils.version_string()}
#             )
#         )
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.post(
#     "/mapping_file_upload",
#     tags=["mapping_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701266243057,
#                         "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
#                         "data": {"filename": "custom_mapping.json"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="upload a JSON mapping file.",
#     description="file will be uploaded to the `gulp/mapping_files` directory (which can be overridden by `PATH_MAPPING_FILES` environment variable)",
# )
# async def mapping_file_upload_handler(
#     token: Annotated[
#         str, Header(description=gulp.structs.API_DESC_TOKEN + " with EDIT permission.")
#     ],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
#     mapping_file: Annotated[
#         UploadFile, File(description="the mapping json file")
#     ] = None,
#     allow_overwrite: Annotated[
#         bool, Query(description="if set, will overwrite an existing mapping file.")
#     ] = True,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(
#             await collab_api.session(), token, GulpUserPermission.EDIT
#         )
#         filename = os.path.basename(mapping_file.filename)
#         if not filename.lower().endswith(".json"):
#             raise gulp.structs.InvalidArgument("mapping_file must be a JSON file.")

#         full_mapping_file_path = GulpConfig.build_mapping_file_path(filename)
#         if not allow_overwrite:
#             # overwrite disabled
#             if os.path.exists(full_mapping_file_path):
#                 raise gulp.structs.ObjectAlreadyExists(
#                     "mapping file %s already exists." % (filename)
#                 )

#         # ok, write file

#         await muty.uploadfile.to_path(
#             mapping_file,
#             dest_dir=GulpConfig.get_instance().path_mapping_files(),
#         )
#         return JSONResponse(
#             muty.jsend.success_jsend(req_id=req_id, data={"filename": filename})
#         )
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.get(
#     "/mapping_file_list",
#     tags=["mapping_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1725380334348,
#                         "req_id": "7b37b846-e3e9-441f-bb4f-b0177ed76d86",
#                         "data": [
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "autopsy_webform_autofill.json",
#                                 "mapping_ids": ["Autopsy Web Form Autofill"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "JLECmd_csv.json",
#                                 "mapping_ids": [
#                                     "custom_destinations",
#                                     "automatic_destinations",
#                                 ],
#                             },
#                             {
#                                 "metadata": {"plugin": ["sqlite"]},
#                                 "filename": "firefox_sqlite.json",
#                                 "mapping_ids": ["moz_places", "moz_annos"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "PECmd_csv.json",
#                                 "mapping_ids": ["timeline", "pecmd"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "RecentFileCacheParser_csv.json",
#                                 "mapping_ids": ["recentfilecacheparser"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["sqlite"]},
#                                 "filename": "chrome_history.json",
#                                 "mapping_ids": ["urls", "downloads"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "autopsy_webhistory.json",
#                                 "mapping_ids": ["Autopsy Web History"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["apache_error_clf"]},
#                                 "filename": "apache_error_clf.json",
#                                 "mapping_ids": [],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "mftecmd_csv.json",
#                                 "mapping_ids": ["record", "boot", "j", "sds"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["systemd_journal"]},
#                                 "filename": "systemd_journal.json",
#                                 "mapping_ids": [],
#                             },
#                             {
#                                 "metadata": {"plugin": ["win_evtx", "csv"]},
#                                 "filename": "windows.json",
#                                 "mapping_ids": [],
#                             },
#                             {
#                                 "metadata": {"plugin": ["sqlite"]},
#                                 "filename": "chrome_webdata.json",
#                                 "mapping_ids": ["autofill"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "autopsy_usbdevices.json",
#                                 "mapping_ids": ["Autopsy USBDevice"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["apache_access_clf"]},
#                                 "filename": "apache_access_clf.json",
#                                 "mapping_ids": [],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "SrumECmd.json",
#                                 "mapping_ids": [
#                                     "appresourceuseinfo",
#                                     "apptimelineprovider",
#                                     "energyusage",
#                                     "networkconnections",
#                                     "networkusages",
#                                     "vfuprov",
#                                 ],
#                             },
#                             {
#                                 "metadata": {"plugin": ["csv"]},
#                                 "filename": "LECmd_csv.json",
#                                 "mapping_ids": ["lecmd"],
#                             },
#                             {
#                                 "metadata": {"plugin": ["pcap"]},
#                                 "filename": "pcap.json",
#                                 "mapping_ids": [],
#                             },
#                         ],
#                     }
#                 }
#             }
#         }
#     },
#     summary="lists available mapping files.",
# )
# async def mapping_file_list_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(await collab_api.session(), token)
#         path = GulpConfig.get_instance().path_mapping_files()
#         MutyLogger.get_instance().debug("listing mapping files in %s" % (path))
#         files = await muty.file.list_directory_async(path)

#         # purge paths
#         purged = []
#         for f in files:
#             if f.lower().endswith(".json"):
#                 # read file and get tags
#                 content = await muty.file.read_file_async(f)
#                 js = json.loads(content)
#                 mtd = js.get("metadata", {})
#                 base_f = os.path.basename(f)

#                 # get also mapping_id for each mapping
#                 mappings = js.get("mappings", [])
#                 mapping_ids = []
#                 for m in mappings:
#                     opts = m.get("options", {})
#                     mm = opts.get("mapping_id", None)
#                     if mm:
#                         mapping_ids.append(mm)
#                 purged.append(
#                     {"metadata": mtd, "filename": base_f, "mapping_ids": mapping_ids}
#                 )

#         return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=purged))
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex) from ex


# @_app.get(
#     "/mapping_file_get",
#     tags=["mapping_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701266243057,
#                         "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
#                         "data": {"windows.json": "base64 file content here"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="get a mapping file (i.e. for editing and reupload).",
#     description="file content is returned as base64.",
# )
# async def mapping_file_get_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_TOKEN)],
#     mapping_file: Annotated[
#         str, Query(description='the mapping file to get (i.e. "windows.json")')
#     ],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(await collab_api.session(), token)
#         file_path = GulpConfig.build_mapping_file_path(mapping_file)

#         # read file content
#         f = await muty.file.read_file_async(file_path)
#         filename = os.path.basename(file_path)
#         return JSONResponse(
#             muty.jsend.success_jsend(
#                 req_id=req_id, data={filename: base64.b64encode(f).decode()}
#             )
#         )
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex, status_code=404) from ex


# @_app.delete(
#     "/mapping_file_delete",
#     tags=["mapping_utility"],
#     response_model=JSendResponse,
#     response_model_exclude_none=True,
#     responses={
#         200: {
#             "content": {
#                 "application/json": {
#                     "example": {
#                         "status": "success",
#                         "timestamp_msec": 1701266243057,
#                         "req_id": "fb2759b8-b0a0-40cc-bc5b-b988f72255a8",
#                         "data": {"filename": "windows.json"},
#                     }
#                 }
#             }
#         }
#     },
#     summary="deletes an existing mapping file.",
# )
# async def mapping_file_delete_handler(
#     token: Annotated[str, Header(description=gulp.structs.API_DESC_ADMIN_TOKEN)],
#     mapping_file: Annotated[
#         str, Query(description='the mapping file to delete (i.e. "windows.json")')
#     ],
#     req_id: Annotated[str, Query(description=gulp.structs.API_DESC_REQID)] = None,
# ) -> JSendResponse:
#     req_id = gulp_utils.ensure_req_id(req_id)
#     try:
#         await GulpUserSession.check_token(
#             await collab_api.session(), token, GulpUserPermission.ADMIN
#         )
#         file_path = GulpConfig.build_mapping_file_path(mapping_file)

#         # delete file
#         await muty.file.delete_file_or_dir_async(file_path, ignore_errors=False)
#         return JSONResponse(
#             muty.jsend.success_jsend(req_id=req_id, data={"filename": mapping_file})
#         )
#     except Exception as ex:
#         raise JSendException(req_id=req_id, ex=ex, status_code=404) from ex


# def router() -> APIRouter:
#     """
#     Returns this module api-router, to add it to the main router

#     Returns:
#         APIRouter: The APIRouter instance
#     """
#     global _app
#     return _app
