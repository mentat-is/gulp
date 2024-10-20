import base64
import json
import os
from typing import Annotated

import muty.crypto
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, File, Header, Query, UploadFile
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

import gulp.api.collab_api as collab_api
import gulp.config as config
import gulp.defs
import gulp.plugin
import gulp.utils as gulp_utils
from gulp.api.collab.base import GulpUserPermission
from gulp.api.collab.session import UserSession
from gulp.utils import logger

_app: APIRouter = APIRouter()


@_app.get(
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
                        "timestamp_msec": 1723204355167,
                        "req_id": "fc21e3c5-fcbf-4fda-a9bb-5776ef418dfd",
                        "data": [
                            {
                                "display_name": "win_evtx",
                                "type": "ingestion",
                                "desc": "Windows EVTX log file processor.",
                                "filename": "win_evtx.py",
                                "internal": False,
                                "options": [],
                                "depends_on": [],
                                "tags": [],
                                "event_type_field": "event.code",
                                "version": "1.0",
                            },
                            {
                                "display_name": "raw",
                                "type": "ingestion",
                                "desc": "Raw events ingestion plugin.",
                                "filename": "raw.py",
                                "internal": False,
                                "options": [],
                                "depends_on": [],
                                "tags": [],
                                "event_type_field": "event.code",
                                "version": "1.0",
                            },
                            {
                                "display_name": "stacked",
                                "type": "ingestion",
                                "desc": "example plugin stacked over the CSV plugin",
                                "filename": "stacked_example.py",
                                "internal": False,
                                "options": [],
                                "depends_on": [],
                                "tags": [],
                                "event_type_field": "event.code",
                                "version": "1.0",
                            },
                            {
                                "display_name": "csv",
                                "type": "ingestion",
                                "desc": "generic CSV file processor",
                                "filename": "csv.py",
                                "internal": False,
                                "options": [
                                    {
                                        "name": "delimiter",
                                        "type": "str",
                                        "default": ",",
                                        "desc": "delimiter for the CSV file",
                                    }
                                ],
                                "depends_on": [],
                                "tags": [],
                                "event_type_field": "event.code",
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
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        l = await gulp.plugin.list_plugins()
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
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
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    plugin: Annotated[
        str,
        Query(
            description='filename of the plugin to retrieve content for, i.e. "plugin.py", "paid/paid_plugin.py"'
        ),
    ],
    plugin_type: Annotated[
        gulp.defs.GulpPluginType,
        Query(description=gulp.defs.API_DESC_PLUGIN_TYPE),
    ] = gulp.defs.GulpPluginType.INGESTION,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        path_plugins = config.path_plugins(plugin_type)
        file_path = muty.file.safe_path_join(path_plugins, plugin, allow_relative=True)

        # read file content
        f = await muty.file.read_file_async(file_path)
        filename = os.path.basename(file_path)
        return JSONResponse(
            muty.jsend.success_jsend(
                req_id=req_id, data={filename: base64.b64encode(f).decode()}
            )
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.delete(
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
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    plugin: Annotated[
        str,
        Query(
            description='filename of the plugin to be deleted, i.e. "plugin.py", "paid/paid_plugin.py"'
        ),
    ],
    plugin_type: Annotated[
        gulp.defs.GulpPluginType,
        Query(description=gulp.defs.API_DESC_PLUGIN_TYPE),
    ] = gulp.defs.GulpPluginType.INGESTION,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )

        path_plugins = config.path_plugins(plugin_type)
        file_path = muty.file.safe_path_join(path_plugins, plugin, allow_relative=True)

        # delete file
        await muty.file.delete_file_or_dir_async(file_path, ignore_errors=False)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"filename": plugin})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/plugin_upload",
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
                        "data": {"filename": "custom_plugin.py"},
                    }
                }
            }
        }
    },
    summary="upload a .py plugin file.",
    description="file will be uploaded to the `gulp/plugins` directory (which can be overridden by `PATH_PLUGINS` environment variable)",
)
async def plugin_upload_handler(
    token: Annotated[
        str, Header(description=gulp.defs.API_DESC_TOKEN + " with EDIT permission")
    ],
    plugin: Annotated[UploadFile, File(description="the plugin file")],
    filename: Annotated[
        str,
        Query(
            description='the filename to save the plugin as, i.e. "plugin.py", "paid/paid_plugin.py"'
        ),
    ],
    plugin_type: Annotated[
        gulp.defs.GulpPluginType,
        Query(description=gulp.defs.API_DESC_PLUGIN_TYPE),
    ] = gulp.defs.GulpPluginType.INGESTION,
    allow_overwrite: Annotated[
        bool, Query(description="if set, will overwrite an existing plugin file.")
    ] = True,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.EDIT
        )
        path_plugins = config.path_plugins(plugin_type)
        plugin_path = muty.file.safe_path_join(
            path_plugins, filename, allow_relative=True
        )

        if not filename.lower().endswith(".py") and not filename.lower().endswith(
            ".pyc"
        ):
            raise gulp.defs.InvalidArgument("plugin must be a .py/.pyc file.")

        if not allow_overwrite:
            # overwrite disabled
            if os.path.exists(plugin_path):
                raise gulp.defs.ObjectAlreadyExists(
                    "plugin %s already exists." % (filename)
                )

        # ok, write file
        await muty.uploadfile.to_path(plugin, dest_dir=os.path.dirname(plugin_path))
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"filename": filename})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/plugin_tags",
    tags=["plugin_utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1716386497525,
                        "req_id": "99ca9c5b-7f84-4181-87bc-9ffc2064406b",
                        "data": ["process", "file" "network"],
                    }
                }
            }
        }
    },
    summary="get tags for the given plugin, if they are set: this allow to better identify plugin capabilities.",
)
async def plugin_tags_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    plugin: Annotated[str, Query(description=gulp.defs.API_DESC_PLUGIN)],
    plugin_type: Annotated[
        gulp.defs.GulpPluginType,
        Query(description=gulp.defs.API_DESC_PLUGIN_TYPE),
    ] = gulp.defs.GulpPluginType.INGESTION,
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:

    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        l = await gulp.plugin.get_plugin_tags(plugin, plugin_type)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=l))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/version",
    tags=["utility"],
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
                        "data": {"version": "gulp v0.0.9 (muty v0.2)"},
                    }
                }
            }
        }
    },
    summary="get gULP version.",
)
async def get_version_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        return JSONResponse(
            muty.jsend.success_jsend(
                req_id=req_id, data={"version": gulp_utils.version_string()}
            )
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.post(
    "/mapping_file_upload",
    tags=["mapping_utility"],
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
                        "data": {"filename": "custom_mapping.json"},
                    }
                }
            }
        }
    },
    summary="upload a JSON mapping file.",
    description="file will be uploaded to the `gulp/mapping_files` directory (which can be overridden by `PATH_MAPPING_FILES` environment variable)",
)
async def mapping_file_upload_handler(
    token: Annotated[
        str, Header(description=gulp.defs.API_DESC_TOKEN + " with EDIT permission.")
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
    mapping_file: Annotated[
        UploadFile, File(description="the mapping json file")
    ] = None,
    allow_overwrite: Annotated[
        bool, Query(description="if set, will overwrite an existing mapping file.")
    ] = True,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.EDIT
        )
        filename = os.path.basename(mapping_file.filename)
        if not filename.lower().endswith(".json"):
            raise gulp.defs.InvalidArgument("mapping_file must be a JSON file.")

        full_mapping_file_path = gulp_utils.build_mapping_file_path(filename)
        if not allow_overwrite:
            # overwrite disabled
            if os.path.exists(full_mapping_file_path):
                raise gulp.defs.ObjectAlreadyExists(
                    "mapping file %s already exists." % (filename)
                )

        # ok, write file

        await muty.uploadfile.to_path(
            mapping_file,
            dest_dir=config.path_mapping_files(),
        )
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"filename": filename})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/mapping_file_list",
    tags=["mapping_utility"],
    response_model=JSendResponse,
    response_model_exclude_none=True,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "status": "success",
                        "timestamp_msec": 1725380334348,
                        "req_id": "7b37b846-e3e9-441f-bb4f-b0177ed76d86",
                        "data": [
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "autopsy_webform_autofill.json",
                                "mapping_ids": ["Autopsy Web Form Autofill"],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "JLECmd_csv.json",
                                "mapping_ids": [
                                    "custom_destinations",
                                    "automatic_destinations",
                                ],
                            },
                            {
                                "metadata": {"plugin": ["sqlite"]},
                                "filename": "firefox_sqlite.json",
                                "mapping_ids": ["moz_places", "moz_annos"],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "PECmd_csv.json",
                                "mapping_ids": ["timeline", "pecmd"],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "RecentFileCacheParser_csv.json",
                                "mapping_ids": ["recentfilecacheparser"],
                            },
                            {
                                "metadata": {"plugin": ["sqlite"]},
                                "filename": "chrome_history.json",
                                "mapping_ids": ["urls", "downloads"],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "autopsy_webhistory.json",
                                "mapping_ids": ["Autopsy Web History"],
                            },
                            {
                                "metadata": {"plugin": ["apache_error_clf"]},
                                "filename": "apache_error_clf.json",
                                "mapping_ids": [],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "mftecmd_csv.json",
                                "mapping_ids": ["record", "boot", "j", "sds"],
                            },
                            {
                                "metadata": {"plugin": ["systemd_journal"]},
                                "filename": "systemd_journal.json",
                                "mapping_ids": [],
                            },
                            {
                                "metadata": {"plugin": ["win_evtx", "csv"]},
                                "filename": "windows.json",
                                "mapping_ids": [],
                            },
                            {
                                "metadata": {"plugin": ["sqlite"]},
                                "filename": "chrome_webdata.json",
                                "mapping_ids": ["autofill"],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "autopsy_usbdevices.json",
                                "mapping_ids": ["Autopsy USBDevice"],
                            },
                            {
                                "metadata": {"plugin": ["apache_access_clf"]},
                                "filename": "apache_access_clf.json",
                                "mapping_ids": [],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "SrumECmd.json",
                                "mapping_ids": [
                                    "appresourceuseinfo",
                                    "apptimelineprovider",
                                    "energyusage",
                                    "networkconnections",
                                    "networkusages",
                                    "vfuprov",
                                ],
                            },
                            {
                                "metadata": {"plugin": ["csv"]},
                                "filename": "LECmd_csv.json",
                                "mapping_ids": ["lecmd"],
                            },
                            {
                                "metadata": {"plugin": ["pcap"]},
                                "filename": "pcap.json",
                                "mapping_ids": [],
                            },
                        ],
                    }
                }
            }
        }
    },
    summary="lists available mapping files.",
)
async def mapping_file_list_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        path = config.path_mapping_files()
        logger().debug("listing mapping files in %s" % (path))
        files = await muty.file.list_directory_async(path)

        # purge paths
        purged = []
        for f in files:
            if f.lower().endswith(".json"):
                # read file and get tags
                content = await muty.file.read_file_async(f)
                js = json.loads(content)
                mtd = js.get("metadata", {})
                base_f = os.path.basename(f)

                # get also mapping_id for each mapping
                mappings = js.get("mappings", [])
                mapping_ids = []
                for m in mappings:
                    opts = m.get("options", {})
                    mm = opts.get("mapping_id", None)
                    if mm:
                        mapping_ids.append(mm)
                purged.append(
                    {"metadata": mtd, "filename": base_f, "mapping_ids": mapping_ids}
                )

        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data=purged))
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex) from ex


@_app.get(
    "/mapping_file_get",
    tags=["mapping_utility"],
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
                        "data": {"windows.json": "base64 file content here"},
                    }
                }
            }
        }
    },
    summary="get a mapping file (i.e. for editing and reupload).",
    description="file content is returned as base64.",
)
async def mapping_file_get_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_TOKEN)],
    mapping_file: Annotated[
        str, Query(description='the mapping file to get (i.e. "windows.json")')
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(await collab_api.session(), token)
        file_path = gulp_utils.build_mapping_file_path(mapping_file)

        # read file content
        f = await muty.file.read_file_async(file_path)
        filename = os.path.basename(file_path)
        return JSONResponse(
            muty.jsend.success_jsend(
                req_id=req_id, data={filename: base64.b64encode(f).decode()}
            )
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex, status_code=404) from ex


@_app.delete(
    "/mapping_file_delete",
    tags=["mapping_utility"],
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
                        "data": {"filename": "windows.json"},
                    }
                }
            }
        }
    },
    summary="deletes an existing mapping file.",
)
async def mapping_file_delete_handler(
    token: Annotated[str, Header(description=gulp.defs.API_DESC_ADMIN_TOKEN)],
    mapping_file: Annotated[
        str, Query(description='the mapping file to delete (i.e. "windows.json")')
    ],
    req_id: Annotated[str, Query(description=gulp.defs.API_DESC_REQID)] = None,
) -> JSendResponse:
    req_id = gulp_utils.ensure_req_id(req_id)
    try:
        await UserSession.check_token(
            await collab_api.session(), token, GulpUserPermission.ADMIN
        )
        file_path = gulp_utils.build_mapping_file_path(mapping_file)

        # delete file
        await muty.file.delete_file_or_dir_async(file_path, ignore_errors=False)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"filename": mapping_file})
        )
    except Exception as ex:
        raise JSendException(req_id=req_id, ex=ex, status_code=404) from ex


def router() -> APIRouter:
    """
    Returns this module api-router, to add it to the main router

    Returns:
        APIRouter: The APIRouter instance
    """
    global _app
    return _app
