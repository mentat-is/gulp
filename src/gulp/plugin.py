"""Gulp plugin base class and plugin utilities."""

import asyncio
import inspect
import ipaddress
import json
import os
from abc import ABC, abstractmethod
from copy import deepcopy
from enum import StrEnum
import sys
from types import ModuleType
from typing import Any, Callable, Optional

import json5
import muty.dynload
import muty.file
import muty.log
import muty.pydantic
import muty.time
from muty.log import MutyLogger
from opensearchpy import Field
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import (GulpRequestStats, PreviewDone,
                                   RequestCanceledError, SourceCanceledError)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import (GulpMapping, GulpMappingField,
                                     GulpMappingFile)
from gulp.api.opensearch.filters import (QUERY_DEFAULT_FIELDS,
                                         GulpDocumentFilterResult,
                                         GulpIngestionFilter, GulpQueryFilter)
from gulp.api.opensearch.query import (GulpQuery, GulpQueryHelpers,
                                       GulpQueryParameters)
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import (WSDATA_DOCUMENTS_CHUNK, WSDATA_ENRICH_DONE, WSDATA_INGEST_SOURCE_DONE, WSDATA_USER_LOGIN, WSDATA_USER_LOGOUT, GulpDocumentsChunkPacket,
                             GulpIngestSourceDonePacket, GulpQueryDonePacket,
                             GulpWsSharedQueue)
from gulp.config import GulpConfig
from gulp.structs import (GulpMappingParameters, GulpPluginCustomParameter,
                          GulpPluginParameters, ObjectNotFound)


class GulpPluginType(StrEnum):
    """
    specifies the plugin types

    - INGESTION: support ingestion
    - EXTERNAL: support query to/ingestion from external sources
    - EXTENSION: extension plugin
    - UI: UI plugin, i.e. a ts/tsx file served by the backend
    """

    INGESTION = "ingestion"
    EXTENSION = "extension"
    EXTERNAL = "external"
    ENRICHMENT = "enrichment"
    UI = "ui"


class GulpPluginCacheMode(StrEnum):
    """
    specifies the plugin cache mode
    """

    FORCE = "force"  # force load into cache if not already loaded
    IGNORE = "ignore"  # always load from disk
    DEFAULT = "default"  # use configuration value (cache enabled/disabled)


class GulpInternalEvent(BaseModel):
    """
    Gulp internal event, broadcasted by engine to plugins registered via GulpPluginEventQueues.register()
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "event_type": "login",
                    "timestamp_msec": 123456789,
                    "data": {"user_id": "user123", "ip": "192.168.2.10"}
                }
            ]
        },
    )
    type: str = Field(
        ...,
        description="The type of the event.",
    )
    timestamp_msec: int = Field(
        ...,
        description="The timestamp of the event in milliseconds since epoch.",
    )
    data: dict = Field(
        ...,
        description="Arbitrary data for the event.",
    )

class GulpPluginEntry(BaseModel):
    """
    Gulp plugin entry for the plugin_list API
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "display_name": "win_evtx",
                    "type": ["ingestion"],
                    "desc": "Windows Event Log plugin.",
                    "path": "/path/to/win_evtx.py",
                    "filename": "win_evtx.py",
                    "sigma_support": True,
                    "regex": r"^\[.*\]",
                    "custom_parameters": [
                        muty.pydantic.autogenerate_model_example_by_class(
                            GulpPluginCustomParameter
                        ),
                    ],
                    "tags": ["event", "windows", "log"],
                    "version": "1.0",
                }
            ]
        },
    )
    display_name: str = Field(
        ...,
        description="The plugin display name.",
    )
    type: list[GulpPluginType] = Field(
        ...,
        description="The supported plugin types.",
    )
    desc: Optional[str] = Field(
        None,
        description="A description of the plugin.",
    )
    path: str = Field(
        ...,
        description="The file path associated with the plugin.",
    )
    data: Optional[dict] = Field(
        None,
        description="Arbitrary data for the UI.",
    )
    filename: str = Field(
        ...,
        description="This is the bare filename without extension (aka the `internal plugin name`, to be used as `plugin` throughout the whole gulp API).",
    )
    custom_parameters: Optional[list[GulpPluginCustomParameter]] = Field(
        [],
        description="A list of custom parameters this plugin supports.",
    )
    depends_on: Optional[list[str]] = Field(
        [],
        description="A list of plugins this plugin depends on.",
    )
    tags: Optional[list[str]] = Field(
        [],
        description="A list of tags for the plugin.",
    )
    version: Optional[str] = Field(
        None,
        description="The plugin version.",
    )
    regex: Optional[str] = Field(
        None,
        description="A regex to identify the data type (i.e. to identify the file header), for ingestion plugin only.",
    )
    ui: Optional[str] = Field(
        None,
        description="HTML frame to be rendered in the UI, i.e. for custom plugin panel.",
    )

class GulpUiPluginMetadata(BaseModel):
    """
    metadata for an UI (ts/tsx) plugin served by the backend
    
    an UI plugin metadata is a companion JSON file with the same name of the ts/tsx plugin + ".json" extension (i.e. my_ui_plugin.tsx.json) in the `$PLUGINS/ui` directory.

    format of the json file is the following

    ```json
    {
        // plugin display name
        "display_name": "Test UI Plugin",
        // description
        "desc": "A plugin for testing UI components",
        // plugin version
        "version": "1.0.0",
        // the related gulp plugin
        "plugin": "some_gulp_plugin.py",
        // true if the related gulp plugin is an extension
        "extension": true
        // filename and path of the TSX plugin to be served will be added by list_ui_plugin API
        // "filename": "test_ui_plugin.tsx",
        // "path": "src/gulp/plugins/ui/test_ui_plugin.tsx"
        //
        // other fields may be added as well, they are not used by the engine but may be useful for the UI
    }
    ```
    """
    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "display_name": "My UI Plugin",
                    "plugin": "gulp_plugin.py",
                    "extension": True,
                    "version": "1.0.0",
                    "desc": "A description of my UI plugin.",

                }
            ]
        },
    )
    display_name: str = Field(
        ...,
        description="The plugin display name.",
    )
    plugin: str = Field(
        ...,
        description="The related Gulp plugin.",
    )
    extension: bool = Field(
        ...,
        description="True if the related Gulp plugin is an extension.",
    )
    version: Optional[str] = Field(
        None,
        description="The plugin version.",
    )
    desc: Optional[str] = Field(
        None,
        description="A description of the plugin.",
    )

class GulpPluginCache:
    """
    Plugin cache singleton.
    """

    _instance: "GulpPluginCache" = None

    def __init__(self):
        self._initialized: bool = True
        self._cache: dict = {}

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpPluginCache":
        """
        Get the plugin cache instance.

        Returns:
            GulpPluginCache: The plugin cache instance.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def clear(self):
        """
        Clear the cache.
        """
        if not GulpConfig.get_instance().plugin_cache_enabled():
            return

        self._cache = {}

    def add(self, plugin: ModuleType, name: str) -> None:
        """
        Add a plugin to the cache.

        Args:
            plugin (PluginBase): The plugin to add to the cache.
            name (str): The name of the plugin.
        """
        if name not in self._cache:
            MutyLogger.get_instance().debug("adding plugin %s to cache" % (name))
            self._cache[name] = plugin

    def get(self, name: str) -> ModuleType:
        """
        Get a plugin from the cache.

        Args:
            name (str): The name of the plugin to get.
        Returns:
            PluginBase: The plugin if found in the cache, otherwise None.
        """
        p = self._cache.get(name, None)
        if p:
            MutyLogger.get_instance().debug("found plugin %s in cache !" % (name))
        return p

    def remove(self, name: str):
        """
        Remove a plugin from the cache.

        Args:
            name (str): The name of the plugin to remove from the cache.
        """
        if name in self._cache:
            MutyLogger.get_instance().debug("removing plugin %s from cache" % (name))
            del self._cache[name]


class GulpInternalEventsManager:
    """
    Singleton class to manage internal events broadcasted from core to plugins
    """

    _instance: "GulpInternalEventsManager" = None
    EVENT_LOGIN: str = WSDATA_USER_LOGIN    # data={ "user_id": str, "ip": str }
    EVENT_LOGOUT: str = WSDATA_USER_LOGOUT  # data={ "user_id": str, "ip": str }
    EVENT_INGEST: str = "ingestion"


    def __init__(self):
        self._initialized: bool = True

        # every dict have a "plugin" key which is a GulpPluginBase instance, and a "types" key with a list of event types this plugin is interested in
        self._plugins: dict[str, dict] = {}

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpInternalEventsManager":
        """
        get the manager instance.

        Returns:
            GulpInternalEventsManager: The internal events manager instance.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def clear(self):
        """
        Clear the internal events manager plugins list
        """
        self._plugins = {}

    def register(self, plugin: "GulpPluginBase", types: list[str] = None) -> None:
        """
        Register a plugin to receive internal events.

        Args:
            plugin (GulpPluginBase): The plugin to register.
            types (list[str], optional): The list of event types the plugin is interested in. If not set, the plugin will receive all events.
        """
        name: str = plugin.bare_filename
        if name not in self._plugins.keys():
            MutyLogger.get_instance().debug("registering plugin %s to receive internal events: %s" % (name, types))
            self._plugins[name] = {
                "plugin_instance": plugin,  # the plugin instance
                "types": types if types else []
            }
        else:
            MutyLogger.get_instance().warning("plugin %s already registered to receive internal events" % (name))

    def deregister(self, plugin: str) -> None:
        """
        Stop a plugin from receiving internal events.

        Args:
            plugin (str): The name of the plugin to unregister.
        """
        if plugin in self._plugins.keys():
            MutyLogger.get_instance().debug("deregistering plugin %s from receiving internal events" % (plugin))
            del self._plugins[plugin]
        else:
            MutyLogger.get_instance().debug("plugin %s not registered to receive internal events" % (plugin))

    async def broadcast_event(self, t: str, data: dict) -> None:
        """
        Broadcast an event to all registered plugin event queues.

        Args:
            t: str: The type of the event.
            data: dict: The data to send with the event.
        Returns:
            None
        """
        ev: GulpInternalEvent = GulpInternalEvent(type=t, timestamp_msec=muty.time.now_msec(), data=data)
        for plugin, entry in self._plugins.items():
            p: GulpPluginBase = entry["plugin_instance"]
            if t in entry["types"]:
                try:
                    MutyLogger.get_instance().debug("broadcasting event %s to plugin %s" % (t, p.bare_filename))
                    await p.internal_event_callback(ev)
                except Exception as e:
                    MutyLogger.get_instance().exception(e)

class GulpPluginBase(ABC):
    """
    Base class for all Gulp plugins.
    """
    # these are used to initialize the license stub code
    _license_stub: "GulpPluginBase" = None 
    _license_func: callable = None

    @classmethod
    def load_pickled(
        cls, path: str, extension: bool, cache_mode: GulpPluginCacheMode
    ) -> "GulpPluginBase":
        """
        load a plugin with pickled=True, used by __reduce__

        Args:
            path (str): path to the plugin
            extension (bool): whether this is an extension plugin
            cache_mode (GulpPluginCacheMode): cache mode to use

        Returns:
            GulpPluginBase: the loaded plugin instance
        """
        return cls.load_sync(path, extension, cache_mode, pickled=True)

    def __reduce__(self) -> tuple:
        """
        define how the object should be pickled when passed to multiprocessing.

        this method is automatically called by the pickle module when serializing the object.
        it instructs pickle how to reconstruct the object when unpickling.

        Args:
            none

        Returns:
            tuple: a tuple containing:
                - callable: the function to call to recreate the object
                - args: arguments to pass to the callable
                - state: the object's state to restore after recreation

        Notes:
            - plugins are loaded with ignore_cache=True and pickled=True when passed to workers
            - the extension status is determined from the plugin type
        """
        # print("********************************** REDUCE *************************************************")
        # determine if this is an extension plugin
        extension = GulpPluginType.EXTENSION in self.type()

        # return the reconstruction information as a tuple
        return (
            GulpPluginBase.load_pickled,  # callable to recreate the object
            (self.path, extension, GulpPluginCacheMode.IGNORE),  # args for callable
            self.__dict__,  # object state to restore
        )

    def __init__(
        self,
        path: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        """
        Initialize a new instance of the class.

        Args:
            path (str): The file path associated with the plugin.
            pickled (bool, optional, INTERNAL): Whether the plugin is pickled. Defaults to False.
                this should not be changed, as it is used by the pickle module to serialize the object when it is passed to the multiprocessing module.
        Returns:
            None

        """
        # print("********************************** INIT *************************************************")
        super().__init__()
        
        # plugin file path
        self.path: str = path
        #print("*** GulpPluginBase.__init__ (%s, %s) called!!!! ***" % (path, self.display_name()))

        # to have faster access to the plugin filename
        self.filename = os.path.basename(self.path)
        self.bare_filename = os.path.splitext(self.filename)[0]

        # to have faster access to the plugin name
        self.name = self.display_name()

        # tell if the plugin has been pickled by the multiprocessing module (internal)
        self._pickled: bool = pickled

        # if set, this plugin have another plugin on top
        self._stacked: bool = False

        #
        # the followin  are stored in the plugin instance at the query/ingest entrypoint
        #

        # SQLAlchemy session
        self._sess: AsyncSession = None

        # ingestion stats
        self._stats: GulpRequestStats = None

        # for ingestion, the mappings to apply
        self._mappings: dict[str, GulpMapping] = {}
        # for ingestion, the key in the mappings dict to be used
        self._mapping_id: str = None
        # calling user
        self._user_id: str = None
        # current gulp operation
        self._operation_id: str = None
        # current gulp context
        self._context_id: str = None
        # current file being ingested
        self._file_path: str = None
        # original file path, if any
        self._original_file_path: str = None
        # current source id
        self._source_id: str = None
        # opensearch index to ingest into
        self._ingest_index: str = None
        self._raw_ingestion: bool = False

        # this is retrieved from the index to check types during ingestion
        self._index_type_mapping: dict = {}
        # if the plugin is processing an external query
        self._external_query: bool = False
        # websocket to stream data to
        self._ws_id: str = None
        # current request id
        self._req_id: str = None

        # for stacked plugins
        self._upper_record_to_gulp_document_fun: Callable = None
        self._upper_enrich_documents_chunk_fun: Callable = None
        self._upper_instance: GulpPluginBase = None

        # to bufferize gulpdocuments
        self._docs_buffer: list[dict] = []

        # this is used by the engine to generate extra documents from a single gulp document
        self._extra_docs: list[dict] = []

        # to keep track of processed/failed records
        self._records_processed_per_chunk: int = 0
        self._records_failed_per_chunk: int = 0
        self._is_source_failed: bool = False
        self._req_canceled: bool = False
        self._source_error: str = None
        self._tot_skipped_in_source: int = 0
        self._tot_failed_in_source: int = 0
        self._tot_processed_in_source: int = 0
        self._tot_ingested_in_source: int = 0

        # to keep track of ingested chunks
        self._chunks_ingested: int = 0

        # additional options
        self._ingestion_enabled: bool = True

        # enrichment during ingestion may be disabled also if _enrich_documents_chunk is implemented
        self._enrich_during_ingestion: bool = True
        self._enrich_index: str = None
        self._tot_enriched: int = 0

        self._plugin_params: GulpPluginParameters = GulpPluginParameters()

        # to minimize db requests to postgres to get context and source at every record
        self._ctx_cache: dict = {}
        self._src_cache: dict = {}
        self._operation: GulpOperation = None

        # in preview mode, ingestion and stats are disabled
        self._preview_mode = False
        self._preview_chunk: list[dict] = []

    @staticmethod
    def _init_license_stub(**kwargs) -> None:
        """
        to be called in __init__ to set the license stub and function pointers

        NOTE: this is only needed to check license during i.e. rest API calls exported by the plugin: license check **ALWAYS** happen anyway every time the plugin is called (so, this is not needed for i.e. pure ingestion/query plugins)

        Args:
            kwargs: **kwargs is passed by the stub and must contain
                - license_stub: pointer to stub "self"
                - license_func: pointer to stub.check_license

            if any of the needed kwargs is not provided, the plugin runs as not licensed (no checks)
        """
        # print("********************************** INIT LICENSE STUB *************************************************")
        # print("GulpPluginBase._init_license_stub called with kwargs: %s" % (kwargs))
        GulpPluginBase._license_stub = kwargs.get("license_stub", None)
        GulpPluginBase._license_func = kwargs.get("license_func", None)

    @staticmethod
    def _check_license() -> None:
        """
        check the license for the plugin, if the plugin is licensed.
        this is called by the plugin when it needs to check the license out of its __init__ method.

        Raises:
            Exception: if the license is not valid or expired
        """
        # print("********************************** CHECK LICENSE *************************************************")
        # print("GulpPluginBase._license_func: %s, GulpPluginBase._license_stub: %s" % (GulpPluginBase._license_func, GulpPluginBase._license_stub))
        if GulpPluginBase._license_func and GulpPluginBase._license_stub:
            GulpPluginBase._license_func(GulpPluginBase._license_stub)

    @abstractmethod
    def display_name(self) -> str:
        """
        Returns the plugin display name.
        """

    @abstractmethod
    def type(self) -> list[GulpPluginType]:
        """
        the supported plugin types.
        """

    def is_running_in_main_process(self) -> bool:
        """
        Returns True if the plugin is running in the main process.
        """
        return not self._pickled

    def version(self) -> str:
        """
        Returns plugin version.
        """
        return ""

    def desc(self) -> str:
        """
        Returns a description of the plugin.
        """
        return ""

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        """
        this is to be used by the UI to list the supported options, and their types, for a plugin.

        NOTE: after `_initialize` has been called these are accessible by the plugin through `self._plugin_params.custom_parameters` dictionary using GulpPluginCustomParameter.name as the key.

        Returns:
            list[GulpPluginCustomParameter]: a list of custom parameters this plugin supports.
        """
        return []

    def data(self) -> dict:
        """
        Returns plugin data: this is an arbitrary dictionary that can be used to store any data.
        """
        return {}

    def ui(self) -> str:
        """
        Returns HTML frame to be rendered in the UI, i.e. for custom plugin panel
        """
        return None

    def depends_on(self) -> list[str]:
        """
        Returns a list of plugins this plugin depends on (plugin file name with/withouy py/pyc).
        """
        return []

    def regex(self) -> str:
        """
        A regex to identify the data type (i.e. to identify the file header), for ingestion plugin only
        """
        return None

    def tags(self) -> list[str]:
        """
        returns a list of tags for the plugin. Tags are used to aid filtering of plugins/query filters in the UI.
        - "event"
        - "network"
        - "file"
        - "process"
        - "threat"
        - "threat.enrichments"
        - ...
        """
        return []

    def plugin_params(self) -> GulpPluginParameters:
        """
        returns the plugin parameters used
        """
        return self._plugin_params

    def preview_chunk(self) -> list[dict]:
        """
        returns the accumulated preview chunk
        """
        return self._preview_chunk

    async def internal_event_callback(self, ev: GulpInternalEvent) -> None:
        """
        this is called by the engine to broadcast an event to the plugin.

        Args:
            ev (GulpInternalEvent): the event
        """
        return
    
    async def post_init(self, **kwargs):
        """
        this is called after the plugin has been initialized, to allow for any post-initialization tasks.

        Args:
            kwargs: additional arguments to pass
        """
        return

    def broadcast_ingest_internal_event(self) -> None:
        """
        broadcast internal ingest metrics event to plugins registered to the GulpInternalEventsManager.EVENT_INGEST event
        """
        d = {
            "plugin": self.bare_filename,
            "user_id": self._user_id,
            "plugin_params": self._plugin_params.model_dump(exclude_none=True) if self._plugin_params else None,
            "status": str(self._stats.status) if self._stats else str(GulpRequestStatus.ONGOING),
            "errors": [self._source_error] if self._source_error else [],
            "req_id": self._req_id,
            "ingested": self._tot_ingested_in_source,
            "failed": self._tot_failed_in_source,
            "skipped": self._tot_skipped_in_source,
            "processed": self._tot_processed_in_source,
            "source_id": self._source_id,
            "context_id": self._context_id,
            "operation_id": self._operation_id,
            "file_path": self._original_file_path if self._original_file_path else self._file_path,
        }
        GulpWsSharedQueue.get_instance().put_internal_event(msg=GulpInternalEventsManager.EVENT_INGEST,params=d)


    async def _ingest_chunk_and_or_send_to_ws(
        self,
        data: list[dict],
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
        all_fields_on_ws: bool = False,
    ) -> tuple[int, int]:
        """
        processes a chunk of GulpDocument dictionaries

        the chunk is sent to the websocket, and if ingestion is enabled (default) the documents are ingested in the opensearch index.

        Args:
            data (list[dict]): the documents to process
            flt (GulpIngestionFilter, optional): the ingestion filter to apply. Defaults to None.
            wait_for_refresh (bool, optional): whether to wait for refresh after ingestion. Defaults to False.
            all_fields_on_ws (bool, optional): whether to send all fields to the websocket. Defaults to False (only default fields)
        Returns:
            tuple[int, int]: the number of ingested documents and the number of skipped documents
        """
        MutyLogger.get_instance().debug(
            f"processing chunk of {len(data)} documents with plugin {self.name}"
        )
        if not data:
            return 0, 0

        el = GulpOpenSearch.get_instance()
        skipped: int = 0
        ingested_docs: list[dict] = []
        success_after_retry: bool = False

        # MutyLogger.get_instance().debug(json.dumps(data, indent=2))
        # MutyLogger.get_instance().debug('flushing ingestion buffer, len=%d' % (len(self.buffer)))
        if self._ingestion_enabled:
            # perform ingestion, ingested_docs may be different from data in the end due to skipped documents
            skipped, ingestion_errors, ingested_docs, success_after_retry = (
                await el.bulk_ingest(
                    self._ingest_index,
                    data,
                    flt=flt,
                    wait_for_refresh=wait_for_refresh,
                )
            )
            # print(json.dumps(ingested_docs, indent=2))
            if ingestion_errors > 0:
                # NOTE: errors here means something wrong with the format of the documents, and must be fixed ASAP.
                # ideally, function should NEVER append errors and the errors total should be the same before and
                # after this function returns (this function may only change the skipped total, which means some duplicates were found).
                if (
                    GulpConfig.get_instance().debug_abort_on_opensearch_ingestion_error()
                ):
                    raise Exception(
                        "opensearch ingestion errors means GulpDocument contains invalid data, review errors on collab db!"
                    )
        else:
            # use full chunk as ingested docs
            ingested_docs = data

        # send ingested docs to websocket
        if flt:
            # copy filter to avoid changing the original, if any,
            flt = deepcopy(flt)

            # ensure data on ws is filtered
            flt.storage_ignore_filter = False

        ws_docs = [
            (  # use all fields on ws if requested
                doc
                if all_fields_on_ws
                else {
                    # either use only a minimal fields set
                    field: doc[field]
                    for field in QUERY_DEFAULT_FIELDS
                    if field in doc
                }
            )
            for doc in ingested_docs
            if GulpIngestionFilter.filter_doc_for_ingestion(doc, flt)
            == GulpDocumentFilterResult.ACCEPT
        ]
        if ws_docs:
            # MutyLogger.get_instance().debug("_ingest_chunk_and_or_send_to_ws: %s" % (json.dumps(ws_docs, indent=2)))
            # send documents to the websocket
            chunk = GulpDocumentsChunkPacket(
                docs=ws_docs,
                num_docs=len(ws_docs),
                # wait for refresh is set only on the last chunk
                last=wait_for_refresh,
                chunk_number=self._chunks_ingested,
            )
            data = chunk.model_dump(exclude_none=True)
            MutyLogger.get_instance().debug(
                f"sending chunk of {len(ws_docs)} documents to ws {
                    self._ws_id}"
            )
            GulpWsSharedQueue.get_instance().put(
                type=WSDATA_DOCUMENTS_CHUNK,
                ws_id=self._ws_id,
                user_id=self._user_id,
                req_id=self._ws_id,
                data=data,
            )
            self._chunks_ingested += 1

        # check if the request is cancelled
        canceled = await GulpRequestStats.is_canceled(self._sess, self._req_id)
        if canceled:
            self._req_canceled = True
            MutyLogger.get_instance().warning(
                "_process_docs_chunk: request %s cancelled" % (self._req_id)
            )

        l: int = len(ingested_docs)
        if success_after_retry:
            # do not count skipped
            return l + skipped, 0

        # MutyLogger.get_instance().debug("returning %d ingested, %d skipped, success_after_retry=%r" % (l, skipped, success_after_retry))
        return l, skipped

    async def _get_or_create_context(self, doc: dict, context_field_name: str) -> str:
        """
        get context from cache or create new one

        Args:
            doc (dict): document containing context info
            context_field_name (str): field name for context id

        Returns:
            str: context id
        """
        # check cache first
        if context_field_name in self._ctx_cache:
            return self._ctx_cache[context_field_name]

        # cache miss - create new context (or get existing)
        context_name = doc.get(context_field_name, None)
        if context_name and context_field_name == 'gulp.context_id':
            # name is already a context id, use that (raw documents case)
            ctx_id = context_name
        else:
            ctx_id = None
        context: GulpContext
        context, _ = await self._operation.add_context(
            self._sess, self._user_id, context_name, self._ws_id, self._req_id, ctx_id=ctx_id
        )

        # update cache
        self._ctx_cache[context_field_name] = context.id
        return context.id

    async def _get_or_create_source(
        self, doc: dict, context_id: str, source_field_name: str, cache_key: str
    ) -> str:
        """
        get source from cache or create new one

        Args:
            doc (dict): document containing source info
            context_id (str): parent context id
            source_field (str): field name for source id
            cache_key (str): cache key for source

        Returns:
            str: source id
        """
        # check cache first
        if cache_key in self._src_cache:
            return self._src_cache[cache_key]

        # cache miss - create new source (or get existing)
        source_name = doc.get(source_field_name, "default")
        if source_name and source_field_name == 'gulp.source_id':
            # name is already a source id, use that (raw documents case)
            src_id = source_name
        else:
            src_id = None

        # fetch context object
        context: GulpContext = await GulpContext.get_by_id(self._sess, context_id)

        # create source
        plugin = self.bare_filename
        mapping_parameters = self._plugin_params.mapping_parameters        
        source, _ = await context.add_source(
            self._sess, self._user_id, source_name, ws_id=self._ws_id, req_id=self._req_id, src_id=src_id,
            plugin=plugin, mapping_parameters=mapping_parameters
        )

        
        # update cache
        self._src_cache[cache_key] = source.id
        return source.id

    async def _add_context_and_source_from_doc(self, doc: dict) -> tuple[str, str]:
        """
        this function extracts context ID and source ID from the document, and creates the corresponding GulpContext and/or GulpSource if they do not exists.

        a cache is used to limit the queries.

        NOTE: plugin_params.custom_parameters should have "context_field" and "source_field" set to indicate the context/source name fields in the document.
            such fields will be used to generate context/source ids and create proper GulpContext/GulpSource on the collab database.
            in case "context_field" and/or "source_field" are not set, they are set to "gulp.context_id" and "gulp.source_id".

        Args:
            doc (dict): the document to extract context and source from.

        Returns:
            tuple[str, str]: the context and source id.
        """
        # get field names from parameters or use defaults
        context_field_name = self._plugin_params.custom_parameters.get(
            "context_field", None)
        if not context_field_name:
            # use default field name
            context_field_name='gulp.context_id'

        source_field_name = self._plugin_params.custom_parameters.get(
            "source_field", None)
        if not source_field_name:
            # use default field name
            source_field_name='gulp.source_id'

        # for no ingestion case, just get values from doc or use defaults
        # MutyLogger.get_instance().debug("ingest_index: %s, operation=%s" % (self._ingest_index, self._operation))
        if not self._ingest_index:
            self._context_id = doc.get(context_field_name, "default")
            self._source_id = doc.get(source_field_name, "default")
            return self._context_id, self._source_id

        # lazy load operation object
        if not self._operation:
            self._operation = await GulpOperation.get_by_id(
                self._sess, self._operation_id
            )

        # create cache key for source lookup
        source_cache_key = f"{context_field_name}-{source_field_name}"

        # check if we have both context and source in cache (if we have source in cache, we have context too)
        if source_cache_key in self._src_cache:
            context_id = self._ctx_cache[context_field_name]
            source_id = self._src_cache[source_cache_key]
            return context_id, source_id

        # handle context creation/retrieval
        context_id = await self._get_or_create_context(doc, context_field_name)

        # handle source creation/retrieval
        source_id = await self._get_or_create_source(
            doc, context_id, source_field_name, source_cache_key
        )

        return context_id, source_id

    async def query_external(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        q: Any,
        plugin_params: GulpPluginParameters,
        q_options: GulpQueryParameters = None,
        index: str = None,
        **kwargs,
    ) -> tuple[int, int, str] | tuple[int, list[dict]]:
        """
        query an external source and stream results, converted to gulpdocument dictionaries, to the websocket.

        optionally ingest them.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): the user performing the query
            req_id (str): the request id
            ws_id (str): the websocket id
            operation_id (str): the operation id
            q(Any): the query to perform, format is plugin specific
            plugin_params (GulpPluginParameters): the plugin parameters, they are mandatory here (custom_parameters should usually be set with specific external source parameters, i.e. how to connect)
            q_options (GulpQueryParameters): additional query options.
            index (str, optional): the gulp's operation index to ingest into during query, may be None for no ingestion
            kwargs: additional keyword arguments
        Notes:
            - implementers must call super().query_external first
            - this function *MUST NOT* raise exceptions.

        Returns (q_options.preview_mode=False):
            tuple[int, int, str]: total documents found, total processed(ingested, usually=found unless errors), query_name

        Returns (q_options.preview_mode=True):
            q_options.preview_mode=True: tuple[int, list[dict]]: total_hits (if available), a preview(chunk) of the documents

        Raises:
            ObjectNotFound: if no document is found.
        """
        MutyLogger.get_instance().debug(
            "GulpPluginBase.query_external: q=%s, index=%s, operation_id=%s, q_options=%s, plugin_params=%s, kwargs=%s"
            % (q, index, operation_id, q_options, plugin_params, kwargs)
        )
        self._sess = sess
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._external_query = True
        self._enrich_during_ingestion = False
        self._operation_id = operation_id

        # setup internal state to be able to call process_record as during ingestion
        self._stats = None

        if index:
            # ingestion is enabled, set index
            self._ingest_index = index
        else:
            # just query, no ingestion
            self._ingestion_enabled = False

        if q_options.preview_mode:
            self._preview_mode = True
            self._ingest_index = None
            self._ingestion_enabled = False
            MutyLogger.get_instance().warning("external query preview mode enabled !")

        # initialize
        await self._initialize(plugin_params=plugin_params)
        return (0, 0, None)

    async def ingest_raw(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        chunk: bytes,
        stats: GulpRequestStats = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:
        """
        ingest a chunk of arbitrary data

        - it is the responsibility of the plugin to process the chunk and convert it to GulpDocuments ready to be ingested
        - it is the responsibility of the plugin to create context and source, i.e. from each document data.

        NOTE: to ingest pre-processed GulpDocuments, use the raw plugin which implements ingest_raw.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation_id (str): id of the operation on collab database.
            chunk: bytes: a raw bytes buffer containing the raw data to be converted to GulpDocuments.
            stats (GulpRequestStats, optional): The ingestion stats.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.

        Returns:
            GulpRequestStatus: The status of the ingestion.

        Notes:
            - implementers must call super().ingest_raw first
            - this function *MUST NOT* raise exceptions.
        """
        self._sess = sess
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._operation_id = operation_id
        self._ingest_index = index
        self._raw_ingestion = True
        self._stats = stats

        MutyLogger.get_instance().debug(
            f"ingesting raw,  num documents={len(chunk)}, plugin {self.name}, user_id={user_id}, operation_id={
                operation_id}, index={index}, ws_id={ws_id}, req_id={req_id}"
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)
        return GulpRequestStatus.ONGOING

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        """
        to be implemented in a plugin to enrich a chunk of documents, called by GulpOpenSearch.search_dsl during loop for each chunk.

        NOTE: do not call this function directly: this is called by the engine right before ingesting a chunk of documents in _flush_buffer()
            also, this is also called by the engine when enriching documents on-demand through the enrich_documents function.

            THIS FUNCTION MUST NOT GENERATE EXCEPTIONS, on failure it should (i.e. cannot enrich one or more documents) it should return the original document/s instead.

        Args:
            docs (list[dict]): the GulpDocuments as dictionaries, to be enriched
            kwargs: additional keyword arguments, the following are guaranteed to be set:
                - total_hits : total hits for the query
                - chunk_num: the chunk number, 0 based
                - last: whether this is the last chunk
        """
        return docs

    async def _enrich_documents_chunk_wrapper(self, docs: list[dict], **kwargs):
        last = kwargs.get("last", False)

        # call the plugin function
        docs = await self._enrich_documents_chunk(docs, **kwargs)
        self._tot_enriched += len(docs)
        MutyLogger.get_instance().debug(f"enriched ({self.name}) {len(docs)} documents")

        # update the documents
        last = kwargs.get("last", False)
        await GulpOpenSearch.get_instance().update_documents(
            self._enrich_index, docs, wait_for_refresh=last
        )

        if docs:
            # send the enriched documents to the websocket
            chunk = GulpDocumentsChunkPacket(
                docs=docs,
                num_docs=len(docs),
                chunk_number=kwargs.get("chunk_num", 0),
                total_hits=kwargs.get("total_hits", 0),
                last=last,
                enriched=True,
            )
            GulpWsSharedQueue.get_instance().put(
                type=WSDATA_DOCUMENTS_CHUNK,
                ws_id=self._ws_id,
                user_id=self._user_id,
                req_id=self._req_id,
                data=chunk.model_dump(exclude_none=True),
            )

        if last:
            # also send a GulpQueryDonePacket
            p = GulpQueryDonePacket(
                status=GulpRequestStatus.DONE,
                total_enriched=self._tot_enriched,
                total_hits=kwargs.get("total_hits", 0),
            )
            GulpWsSharedQueue.get_instance().put(
                type=WSDATA_ENRICH_DONE,
                ws_id=self._ws_id,
                user_id=self._user_id,
                req_id=self._req_id,
                data=p.model_dump(exclude_none=True),
            )

    async def enrich_documents(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> int:
        """
        to be implemented in a plugin to enrich a chunk of GulpDocuments dictionaries on-demand.

        the resulting documents will be streamed to the websocket `ws_id` as GulpDocumentsChunkPacket.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            operation_id (str): id of the operation on collab database.
            index (str): the index to query and enrich
            flt(GulpQueryFilter, optional): a filter to restrict the documents to enrich. Defaults to None.
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.
            kwargs: additional keyword arguments:
                - rq (dict): a raw query to be used, additionally to the filter

        Returns:
            int: the total number of enriched documents

        NOTE: implementers must implement _enrich_documents_chunk, call self._initialize() and then super().enrich_documents
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment" % (self.name)
            )

        # await self._initialize(plugin_params=plugin_params)

        self._user_id = user_id
        self._req_id = req_id
        self._ws_id = ws_id
        self._operation_id = operation_id
        self._enrich_index = index

        # check if the caller provided a raw query to be used
        rq = kwargs.get("rq", None)
        q: dict = {}
        if rq:
            # raw query provided by the caller
            if not flt or flt.is_empty():
                # no filter, use the raw query
                q = rq
            else:
                # merge raw query and filter
                qq = flt.to_opensearch_dsl()
                q = GulpQueryHelpers.merge_queries(qq, rq)
        else:
            # no raw query, just filter
            if not flt or flt.is_empty():
                # match all query
                q = {"query": {"match_all": {}}}
            else:
                # convert filter
                q = flt.to_opensearch_dsl()

        # force return all fields
        q_options = GulpQueryParameters(fields="*")
        _, matched, _ = await GulpQueryHelpers.query_raw(
            sess=sess,
            user_id=self._user_id,
            req_id=self._req_id,
            ws_id=self._ws_id,
            q=q,
            index=index,
            q_options=q_options,
            callback_chunk=self._enrich_documents_chunk_wrapper,
        )
        return matched

    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        operation_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        """
        to be implemented in a plugin to enrich a single document on-demand.

        Args:
            sess (AsyncSession): The database session.
            doc (dict): the document to enrich
            operation_id (str): id of the operation on collab database.
            index (str): the index to query
            plugin_params (GulpPluginParameters): the plugin parameters
        Returns:
            dict: the enriched document

        NOTE: implementers must implement _enrich_documents_chunk, call self._initialize() and then super().enrich_single_document
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment" % (self.name)
            )

        self._operation_id = operation_id
        # await self._initialize(plugin_params=plugin_params)

        # get the document
        doc = await GulpQueryHelpers.query_single(index, doc_id)

        # enrich
        docs = await self._enrich_documents_chunk([doc])
        # MutyLogger.get_instance().debug("docs=%s" % (docs))
        if not docs:
            raise ObjectNotFound("document not suitable for enrichment")

        # update the document
        await GulpOpenSearch.get_instance().update_documents(
            index, docs, wait_for_refresh=True
        )
        return docs[0]

    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> GulpRequestStatus:
        """
        ingests a file containing records in the plugin specific format.

        Args:
            sess (AsyncSession): The database session.
            stats (GulpRequestStats): The ingestion stats.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation_id (str): id of the operation on collab database.
            context_id (str): id of the context on collab database.
            source_id (str): id of the source on collab database.
            file_path (str): path to the file being ingested.
            original_file_path (str, optional): the original file path. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.
            kwargs: additional keyword arguments
                - preview_mode (bool, optional): whether to ingest in preview mode. Defaults to False.
        Returns:
            GulpRequestStatus: The status of the ingestion.

        Notes:
            - implementers must call super().ingest_file first
            - this function *MUST NOT* raise exceptions.
        """
        self._sess = sess
        self._stats = stats
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._operation_id = operation_id
        self._context_id = context_id
        self._ingest_index = index
        self._file_path = file_path
        self._original_file_path = original_file_path
        self._source_id = source_id
        preview_mode = kwargs.get("preview_mode", False)
        if preview_mode:
            # preview mode
            self._preview_mode = True
            self._ingestion_enabled = False

        MutyLogger.get_instance().debug(
            f"ingesting file source_id={source_id}, file_path={file_path}, original_file_path={original_file_path}, plugin {self.name}, user_id={user_id}, operation_id={operation_id}, \
                plugin_params={plugin_params}, flt={flt}, context_id={context_id}, index={index}, ws_id={ws_id}, req_id={req_id}, preview_mode={self._preview_mode}"
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)

        # add mapping parameters to source
        await self._update_source_mapping_parameters()
        return GulpRequestStatus.ONGOING

    async def load_plugin_direct(
        self,
        plugin: str,
        sess: AsyncSession = None,
        stats: GulpRequestStats = None,
        user_id: str = None,
        req_id: str = None,
        ws_id: str = None,
        index: str = None,
        operation_id: str = None,
        context_id: str = None,
        source_id: str = None,
        file_path: str = None,
        original_file_path: str = None,
        plugin_params: GulpPluginParameters = None,
        cache_mode: GulpPluginCacheMode = GulpPluginCacheMode.DEFAULT,
    ) -> "GulpPluginBase":
        """
        loads and initializes a plugin to use its methods directly from another plugin, bypassing the engine.

        Args:
            plugin (str): the plugin to load.
            sess (AsyncSession, optional): The database session. Defaults to None.
            stats (GulpRequestStats, optional): The ingestion stats. Defaults to None.
            user_id (str, optional): The user performing the ingestion (id on collab database). Defaults to None.
            req_id (str, optional): The request ID. Defaults to None.
            ws_id (str, optional): The websocket ID to stream on. Defaults to None.
            index (str, optional): The name of the target opensearch/elasticsearch index or datastream. Defaults to None.
            operation_id (str, optional): id of the operation on collab database. Defaults to None.
            context_id (str, optional): id of the context on collab database. Defaults to None.
            source_id (str, optional): id of the source on collab database. Defaults to None.
            file_path (str, optional): path to the file being ingested. Defaults to None.
            original_file_path (str, optional): the original file path. Defaults to None.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.
            cache_mode (GulpPluginCacheMode, optional): the cache mode for the plugin. Defaults to GulpPluginCacheMode.DEFAULT.
        Returns:
            GulpPluginBase: the loaded plugin.
        """
        if not plugin_params:
            plugin_params = GulpPluginParameters()

        lower = await GulpPluginBase.load(plugin, cache_mode=cache_mode)

        # initialize private fields
        # pylint: disable=W0212
        lower._sess = sess
        lower._stats = stats
        lower._ws_id = ws_id
        lower._req_id = req_id
        lower._user_id = user_id
        lower._operation_id = operation_id
        lower._context_id = context_id
        lower._ingest_index = index
        lower._file_path = file_path
        lower._original_file_path = original_file_path
        lower._source_id = source_id
        await lower._initialize(plugin_params)
        return lower

    async def setup_stacked_plugin(
        self,
        plugin: str,
        *args,
        cache_mode: GulpPluginCacheMode = GulpPluginCacheMode.DEFAULT,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        setup the caller plugin as the "upper" plugin in a stack, and load "plugin" as the lower plugin.
        
        this must be called i.e. in "ingest_file"
        
        - the caller calls setup_stacked_plugin with the plugin to load as lower plugin
        - it then calls lower "ingest_file" and let the lower plugin process the file.

        the engine takes care of postprocessing data:

        - for any record, calls the upper plugin _record_to_gulp_document function after the lower
        - when flushing a chunk of documents to opensearch, calls the upper plugin _enrich_documents function after the lower

        Args:
            plugin (str): the plugin to load as lower
            *args: additional arguments to pass to the plugin constructor.
            cache_mode (GulpPluginCacheMode, optional): the cache mode for the plugin. Defaults to GulpPluginCacheMode.DEFAULT.
            **kwargs: additional keyword arguments to pass to the plugin constructor.
        Returns:
            PluginBase: the loaded plugin
        """
        lower = await GulpPluginBase.load(
            plugin, extension=False, cache_mode=cache_mode, *args, **kwargs
        )

        # set upper plugin functions in lower, so it can call them after processing data itself
        # pylint: disable=W0212
        lower._upper_record_to_gulp_document_fun = self._record_to_gulp_document
        lower._upper_enrich_documents_chunk_fun = self._enrich_documents_chunk
        lower._upper_instance = self

        # set the lower plugin as stacked
        lower._stacked = True
        return lower

    def _finalize_process_record(self, doc: GulpDocument) -> list[dict]:
        """
        finalize processing a record, generating extra documents if needed.

        Args:
            doc (GulpDocument): the gulp document to be used as base for extra documents.

        Returns:
            list[dict]: the final list of documents to be ingested (doc is always the first one).

        NOTE: called by the engine, do not call this function directly.
        """
        if not self._extra_docs:
            return [doc.model_dump(by_alias=True)]

        base_doc_dump = doc.model_dump()
        # MutyLogger.get_instance().debug(json.dumps("base doc:\n%s" % (base_doc_dump), indent=2))
        extra_docs = []
        for extra_fields in self._extra_docs:
            # MutyLogger.get_instance().debug("creating new doc with %s\nand\n%s" % (json.dumps(extra_fields, indent=2), json.dumps(base_doc_dump, indent=2)))

            new_doc_data = {**base_doc_dump, **extra_fields}

            # also add link to the base document
            new_doc_data["gulp.base_document_id"] = doc.id

            # default event code must be ignored for the extra document (since the extra document, by design, has a different event code)
            new_doc = GulpDocument(
                self, **new_doc_data, __ignore_default_event_code__=True
            )
            # MutyLogger.get_instance().debug(
            #     "creating new doc with base=\n%s\ndata=\n%s\nnew_doc=%s"
            #     % (
            #         json.dumps(base_doc_dump, indent=2),
            #         json.dumps(new_doc_data, indent=2),
            #         json.dumps(new_doc.model_dump(), indent=2),
            #     )
            # )
            extra_docs.append(new_doc.model_dump(by_alias=True))

        return [doc.model_dump(by_alias=True)] + extra_docs

    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        """
        to be implemented in a plugin to convert a record to a GulpDocument.

        parses a record and calls _process_key for each key/value pair, generating
        a dictionary to be merged in the final gulp document.

        NOTE: called by the engine, do not call this function directly.
            THIS FUNCTION MUST NOT GENERATE EXCEPTIONS, on failure it should return None to indicate a malformed record.

        Args:
            record (any): the record to convert.
                NOTE: in a stacked plugin, this method always receives `record` ad `dict` (GulpDocument.model_dump()) and returns the same `dict` after processing.
            record_idx (int): the index of the record in the source
            kwargs: additional keyword arguments
        Returns:
            GulpDocument: the GulpDocument, or None to skip processing (i.e. plugin detected malformed record)

        """
        raise NotImplementedError("not implemented!")

    async def _process_with_stacked_plugin(
        self, docs: list[dict], record_idx: int, **kwargs
    ) -> list[dict]:
        """
        processes documents with stacked plugin.

        Args:
            docs (list[dict]): documents to process
            record_idx (int): the index of the record
            kwargs: additional keyword arguments

        Returns:
            list[dict]: processed documents
        """
        for i, doc in enumerate(docs):
            # pylint: disable=E1102
            docs[i] = await self._upper_record_to_gulp_document_fun(
                doc, record_idx, **kwargs
            )
        return docs

    async def _record_to_gulp_documents_wrapper(
        self, record: Any, record_idx: int, **kwargs
    ) -> list[dict]:
        """
        turn a record in one or more gulp documents, taking care of calling lower plugin if any.

        Args:
            record (any): the record to convert
            record_idx (int): the index of the record in the source
            kwargs: additional keyword arguments, they will be passed to plugin's `_record_to_gulp_documennt`
        Returns:
            list[dict]: zero or more GulpDocument dictionaries

        NOTE: called by the engine, do not call this function directly.
        """

        # process documents with our record_to_gulp_document
        doc = await self._record_to_gulp_document(record, record_idx, **kwargs)
        if not doc:
            return []

        docs = self._finalize_process_record(doc)

        # apply upper plugin processing if stacked
        if self._upper_record_to_gulp_document_fun:
            docs = await self._process_with_stacked_plugin(docs, record_idx, **kwargs)

        return docs

    def selected_mapping(self) -> GulpMapping:
        """
        Returns the selected (self._mapping_id) mapping or an empty one.

        NOTE: just a shortcut for self._mappings.get(self._mapping_id, GulpMapping())

        Returns:
            GulpMapping: The selected mapping.
        """
        return self._mappings.get(self._mapping_id, GulpMapping())

    @staticmethod
    def build_unmapped_key(source_key: str) -> str:
        """
        builds a "gulp.unmapped" key from a source key.

        Args:
            source_key (str): The source key to map.
        Returns:
            str: The unmapped key.
        """
        # remove . and spaces from the key
        sk = source_key.replace(".", "_").replace(" ", "_")
        return f"{GulpOpenSearch.UNMAPPED_PREFIX}.{sk}"

    def _try_map_ecs(
        self,
        fields_mapping: GulpMappingField,
        d: dict,
        source_key: str,
        source_value: Any,
    ) -> dict:
        """
        tries to map a source key to an ECS key.

        Args:
            fields_mapping (GulpMappingField): The mapping field.
            d (dict): The document to update.
            source_key (str): The source key to map.
            source_value (any): The source value to map.
        Returns:
            dict: a dict with the mapped key and value.
        """

        # print(fields_mapping)
        mapping = fields_mapping.ecs
        if isinstance(mapping, str):
            # single mapping
            mapping = [mapping]

        force_type = fields_mapping.force_type is not None
        if mapping:
            # source key is mapped, add the mapped key to the document
            for k in mapping:
                kk, vv = self._type_checks(k, source_value, force_type_set=force_type)
                if vv:
                    d[kk] = vv
        else:
            # unmapped key
            d[GulpPluginBase.build_unmapped_key(source_key)] = source_value

        return d

    def _process_key(self, source_key: str, source_value: Any) -> dict:
        """
        Maps the source key, generating a dictionary to be merged in the final gulp document.

        also updates self._extra_docs with dictionaries indicating extra documents to be generated,
        to be post-processed in _finalize_process_record().

        Args:
            source_key (str): The source key to map.
            source_value (any): The source value to map.
        Returns:
            a dictionary to be merged in the final gulp document
        """

        if not source_value:
            return {}

        # check if we have a mapping for source_key
        mapping = self.selected_mapping()
        if mapping.allow_prefixed:
            # consider only the last part (i.e. "this_is_a_sourcekey" -> "sourcekey")
            source_key = source_key.split("_")[-1]

        if mapping.exclude and source_key in mapping.exclude:
            # ignore this key
            return {}
        if mapping.include and source_key not in mapping.include:
            # ignore this key
            return {}

        fields_mapping = mapping.fields.get(source_key)
        if not fields_mapping:
            # missing mapping at all (no ecs and no timestamp field)
            return {GulpPluginBase.build_unmapped_key(source_key): source_value}

        d = {}
        if fields_mapping.force_type:
            # force value to the given type
            t = fields_mapping.force_type
            if t == "int":
                try:
                    source_value = int(source_value)
                except ValueError:
                    source_value = 0
            elif t == "float":
                try:
                    source_value = float(source_value)
                except ValueError:
                    source_value = 0.0
            elif t == "str":
                source_value = str(source_value)
            # MutyLogger.get_instance().warning(f"value {source_value} forced to {t}")

        if (
            fields_mapping.multiplier
            and isinstance(source_value, int)
            and fields_mapping.multiplier > 1
        ):
            # apply multiplier
            source_value = int(source_value * fields_mapping.multiplier)

        if fields_mapping.is_timestamp_chrome:
            # timestamp chrome, turn to nanoseconds from epoch
            source_value = muty.time.chrome_epoch_to_nanos_from_unix_epoch(
                int(source_value)
            )

        if fields_mapping.extra_doc_with_event_code:
            # this will trigger the creation of an extra document
            # with the given event code in _finalize_process_record()
            extra = {
                "event_code": str(fields_mapping.extra_doc_with_event_code),
                "timestamp": source_value,
            }

            # this will trigger the removal of field/s corresponding to this key in the generated extra document,
            # to avoid duplication
            mapped = self._try_map_ecs(fields_mapping, d, source_key, source_value)
            for k, _ in mapped.items():
                extra[k] = None

            self._extra_docs.append(extra)

            # we also add this key to the main document
            return mapped

        m = self._try_map_ecs(fields_mapping, d, source_key, source_value)
        return m

    async def _update_ingestion_stats(self, ingested: int, skipped: int) -> None:
        """
        updates ingestion stats.

        Args:
            ingested (int): number of documents ingested
            skipped (int): number of documents skipped
        """
        if self._stats and self._sess:
            # update stats
            MutyLogger.get_instance().debug(
                "updating stats, processed=%d, ingested=%d, skipped=%d, tot_failed(in instance)=%d, tot_skipped(in instance)=%d"
                % (
                    self._records_processed_per_chunk,
                    ingested,
                    skipped,
                    self._tot_failed_in_source,
                    self._tot_skipped_in_source,
                )
            )
            d = {
                "source_id": self._source_id,
                "records_skipped": skipped,
                "records_ingested": ingested,
                "records_processed": self._records_processed_per_chunk,
                "records_failed": self._records_failed_per_chunk,
            }
            await self._stats.update(
                self._sess, d=d, ws_id=self._ws_id, user_id=self._user_id
            )

    async def _flush_and_check_thresholds(
        self,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
        **kwargs,
    ) -> None:
        """
        flushes buffer and checks failure thresholds.

        Args:
            flt (GulpIngestionFilter, optional): filter to apply during ingestion
            wait_for_refresh (bool, optional): whether to wait for refresh
            kwargs: additional keyword arguments
        """
        # flush buffer and get stats
        ingested, skipped = await self._flush_buffer(flt, wait_for_refresh, **kwargs)

        # check if request was canceled
        if self._req_canceled:
            raise RequestCanceledError(f"request {self._req_id} canceled!")

        # check failure thresholds
        failure_threshold = GulpConfig.get_instance().ingestion_evt_failure_threshold()
        if failure_threshold > 0 and (
            self._tot_skipped_in_source >= failure_threshold
            or self._tot_failed_in_source >= failure_threshold
        ):
            raise SourceCanceledError(
                f"ingestion per-source failure threshold reached (tot_skipped={self._tot_skipped_in_source}, "
                f"tot_failed={self._tot_failed_in_source}, threshold={failure_threshold}), "
                f"canceling source..."
            )

        # update stats if available
        if self._stats:
            await self._update_ingestion_stats(ingested, skipped)

        # reset buffers and counters
        self._docs_buffer = []
        self._records_processed_per_chunk = 0
        self._records_failed_per_chunk = 0

    async def process_record(
        self,
        record: Any,
        record_idx: int,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
        **kwargs,
    ) -> None:
        """
        Processes a single record by converting it to one or more documents.

        the document is then sent to the configured websocket and, if enabled, ingested in the configured opensearch index.

        Args:
            record (any): The record to process.
            record_idx (int): The index of the record.
            flt (GulpIngestionFilter, optional): The filter to apply during ingestion. Defaults to None.
            wait_for_refresh (bool, optional): Whether to wait for a refresh after ingestion. Defaults to False.
            kwargs: additional keyword arguments, they will be passed to plugin's `_record_to_gulp_documennt`
        """
        if self._external_query:
            # external query, documents have been already filtered by the query
            flt = None

        # get buffer size from config or override
        ingestion_buffer_size = (
            self._plugin_params.override_chunk_size
            or GulpConfig.get_instance().documents_chunk_size()
        )

        self._extra_docs = []
        self._tot_processed_in_source += 1

        # process this record and generate one or more gulpdocument dictionaries
        try:
            docs = await self._record_to_gulp_documents_wrapper(
                record, record_idx, **kwargs
            )
        except Exception as ex:
            # report failure
            self._record_failed(ex)
            return

        self._records_processed_per_chunk += 1

        if self._preview_mode:
            # preview, accumulate docs
            for d in docs:
                # remove highlight if present
                d.pop("highlight", None)

            self._preview_chunk.extend(docs)
            # MutyLogger.get_instance().debug("accumulated %d docs" % (len(self._preview_chunk)))
            if (
                len(self._preview_chunk)
                >= GulpConfig.get_instance().preview_mode_num_docs()
            ):
                # must stop
                raise PreviewDone("preview done")

            # and do nothing else
            return

        # add documents to buffer and check if we need to flush
        for d in docs:
            self._docs_buffer.append(d)
            if len(self._docs_buffer) >= ingestion_buffer_size:
                await self._flush_and_check_thresholds(flt, wait_for_refresh, **kwargs)

    async def _parse_custom_parameters(self) -> None:
        """
        parse custom plugin parameters against defined ones and set default values if needed

        Raises:
            ValueError: if a required parameter is missing
        """
        defined_custom_params = self.custom_parameters()
        for param in defined_custom_params:
            param_name = param.name

            # check if required parameter is present
            if (
                param.required
                and param_name not in self._plugin_params.custom_parameters
            ):
                raise ValueError(
                    f"required plugin parameter '{param_name}' not found in plugin_params.custom_parameters"
                )

            # get current value or set default
            param_value = self._plugin_params.custom_parameters.get(param_name)
            if param_value is None and param.default_value is not None:
                param_value = param.default_value
                self._plugin_params.custom_parameters[param_name] = param_value

            MutyLogger.get_instance().debug(
                f"---> found plugin custom parameter: {param_name}={param_value}"
            )

    @staticmethod
    async def mapping_parameters_to_mapping(
        mapping_parameters: GulpMappingParameters = None,
    ) -> tuple[dict[str, GulpMapping], str]:
        """
        convert plugin parameters to mapping: handle mappings and mapping files.
        this is used by the engine to load the plugin and set the mapping.

        Args:
            mapping_parameters (GulpMappingParameters, optional): the mapping parameters. if not set, the default (empty) mapping will be used.

        Returns:
            tuple[dict[str, GulpMapping], str]: a tuple with the mappings (if empty, this is set to an empty mapping with mapping_id="default") and the mapping id
        """
        if not mapping_parameters:
            mapping_parameters = GulpMappingParameters()

        mappings: dict[str, GulpMapping] = None
        mapping_id: str = None
        if (
            not mapping_parameters.mapping_file
            and not mapping_parameters.mappings
            and not mapping_parameters.additional_mapping_files
            and mapping_parameters.mapping_id
        ):
            raise ValueError(
                "mapping_id is set but mappings/mapping_file/additional_mapping_files are not!"
            )

        # check if mappings or mapping_file is set
        if mapping_parameters.mappings:
            # use provided mappings dictionary
            mappings = {
                k: GulpMapping.model_validate(v)
                for k, v in mapping_parameters.mappings.items()
            }
            MutyLogger.get_instance().debug(
                f'using plugin_params.mapping_parameters.mappings="{mapping_parameters.mappings}"'
            )
        elif mapping_parameters.mapping_file:
            # load from mapping file
            mapping_file = mapping_parameters.mapping_file
            MutyLogger.get_instance().debug(
                f"using plugin_params.mapping_parameters.mapping_file={mapping_file}"
            )

            mapping_file_path = GulpConfig.get_instance().build_mapping_file_path(
                mapping_file
            )
            file_content = await muty.file.read_file_async(mapping_file_path)
            mapping_data = json.loads(file_content)

            if not mapping_data:
                raise ValueError(f"mapping file {mapping_file_path} is empty!")

            mapping_file_obj = GulpMappingFile.model_validate(mapping_data)
            mappings = mapping_file_obj.mappings

        # validation checks
        if not mappings and not mapping_parameters.mapping_id:
            MutyLogger.get_instance().warning(
                "mappings/mapping_file and mapping_id are both None/empty!"
            )
            mappings = {"default": GulpMapping(fields={})}

        # ensure mapping_id is set to first key if not specified
        mapping_id = mapping_parameters.mapping_id or list(mappings.keys())[0]
        MutyLogger.get_instance().debug(f"mapping_id={mapping_id}")

        # check for additional mapping
        # skip if using direct mappings or no additional files
        if (
            mapping_parameters.mappings
            or not mapping_parameters.additional_mapping_files
        ):
            return mappings, mapping_id

        MutyLogger.get_instance().debug(
            f"loading additional mapping files/id: {mapping_parameters.additional_mapping_files} ..."
        )

        for file_info in mapping_parameters.additional_mapping_files:
            # load and merge additional mappings from files
            additional_file_path = GulpConfig.get_instance().build_mapping_file_path(
                file_info[0]
            )
            additional_mapping_id = file_info[1]

            file_content = await muty.file.read_file_async(additional_file_path)
            mapping_data = json.loads(file_content)

            if not mapping_data:
                raise ValueError(
                    f"additional mapping file {additional_file_path} is empty!"
                )

            additional_mapping_file = GulpMappingFile.model_validate(mapping_data)

            # merge mappings
            main_mapping = mappings.get(mapping_id, GulpMapping())
            add_mapping = additional_mapping_file.mappings[additional_mapping_id]

            MutyLogger.get_instance().debug(
                f"adding additional mappings from {additional_file_path}.{additional_mapping_id} to '{mapping_id}' ..."
            )

            for key, value in add_mapping.fields.items():
                main_mapping.fields[key] = value

            mappings[mapping_id] = main_mapping

        return mappings, mapping_id

    async def _handle_stacked_mappings(self) -> None:
        """
        pass mappings to the upper plugin in stacked configuration
        """
        # pylint: disable=W0212
        if not self._stacked:
            return

        # pass our mappings to the upper plugin
        if not self._upper_instance._mapping_id:
            self._upper_instance._mapping_id = self._mapping_id

        if not self._upper_instance._mappings:
            self._upper_instance._mappings = self._mappings

    async def _initialize_index_mappings(self) -> None:
        """
        initialize index type mapping from opensearch
        """
        if not self._index_type_mapping:
            self._index_type_mapping = (
                await GulpOpenSearch.get_instance().datastream_get_key_value_mapping(
                    self._ingest_index
                )
            )
            MutyLogger.get_instance().debug(
                f"got index type mappings with {len(self._index_type_mapping)} entries"
            )

    async def _initialize(self, plugin_params: GulpPluginParameters = None) -> None:
        """
        initialize mapping and plugin custom parameters

        this sets self._mappings, self._mapping_id, self._plugin_params (the plugin specific parameters), self._index_type_mapping

        Args:
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.
        Raises:
            ValueError: if mapping_id is set but mappings/mapping_file is not.
            ValueError: if a specific parameter is required but not found in plugin_params.

        """
        # ensure we have a plugin_params object
        self._plugin_params = plugin_params or GulpPluginParameters()

        MutyLogger.get_instance().debug(
            "---> _initialize: plugin=%s, plugin_params=%s"
            % (
                self.filename,
                json.dumps(self._plugin_params.model_dump(), indent=2),
            )
        )

        # parse the custom parameters
        await self._parse_custom_parameters()
        if (
            GulpPluginType.EXTENSION in self.type()
            or GulpPluginType.ENRICHMENT in self.type()
        ):
            MutyLogger.get_instance().debug(
                "extension/enrichment plugin, no mappings needed"
            )
            return

        # setup mappings if needed
        if not self._mappings:
            self._mappings, self._mapping_id = (
                await GulpPluginBase.mapping_parameters_to_mapping(
                    self._plugin_params.mapping_parameters
                )
            )

        # handle stacked plugin mappings
        await self._handle_stacked_mappings()

        # initialize index type mappings
        await self._initialize_index_mappings()

        # MutyLogger.get_instance().debug("---> finished _initialize: plugin=%s, mapping_id=%s, mappings=%s"% (self.filename, self._mapping_id, self._mappings))

    def _type_checks(
        self, k: str, v: Any, force_type_set: bool = False
    ) -> tuple[str, Any]:
        """
        check the type of a value and convert it if needed.

        Args:
            k (str): the key
            v (any): the value
            force_type_set (bool, optional): if not set, and the key is not found in the index_type_mapping, the value is converted to string. Defaults to False.
                this is set when "force_type" is set for a field mapping: the value is converted to the given type before reaching here, so we must not convert it again.

        Returns:
            tuple[str, any]: the key and the value
        """
        index_type = self._index_type_mapping.get(k)
        if not index_type:
            # MutyLogger.get_instance().warning("key %s not found in index_type_mapping" % (k))
            if force_type_set:
                # force_type has been enforced on this field, so we return as is
                return k, v

            # enforce to string
            return k, str(v)

        # check different types, we may add more ...
        index_type = self._index_type_mapping[k]
        if index_type == "long":
            # MutyLogger.get_instance().debug("converting %s:%s to long" % (k, v))
            if isinstance(v, str):
                if v.isnumeric():
                    return k, int(v)
                if v.lower().startswith("0x"):
                    return k, int(v, 16)
                try:
                    return k, int(v)
                except ValueError:
                    # MutyLogger.get_instance().exception("error converting %s:%s to long" % (k, v))
                    return k, None
            return k, v

        if index_type in ["float", "double"]:
            if isinstance(v, str):
                try:
                    return k, float(v)
                except ValueError:
                    # MutyLogger.get_instance().exception("error converting %s:%s to float" % (k, v))
                    return k, None

            return k, v

        if index_type == "date" and isinstance(v, str) and v.lower().startswith("0x"):
            # convert hex to int, then ensure it is a valid timestamp
            try:
                # MutyLogger.get_instance().debug("converting %s: %s to date" % (k, v))
                v = muty.time.ensure_iso8601(str(int(v, 16)))
                return k, v
            except ValueError:
                # MutyLogger.get_instance().exception("error converting %s:%s to date" % (k, v))
                return k, None

        if index_type == "keyword" or index_type == "text":
            # MutyLogger.get_instance().debug("converting %s:%s to keyword" % (k, v))
            return k, str(v)

        if index_type == "ip":
            # MutyLogger.get_instance().debug("converting %s:%s to ip" % (k, v))
            if "local" in v.lower():
                return k, "127.0.0.1"
            try:
                ipaddress.ip_address(v)
            except ValueError:
                # MutyLogger.get_instance().exception("error converting %s:%s to ip" % (k, v))
                return k, None

        # add more types here if needed ...
        # MutyLogger.get_instance().debug("returning %s:%s" % (k, v))
        return k, v

    def _stats_status(self) -> GulpRequestStatus:
        """
        Returns the status of the ingestion statistics.

        if not stats are set, returns GulpRequestStatus.DONE.

        Returns:
            GulpRequestStatus: The status of the ingestion statistics.
        """
        if not self._stats:
            return GulpRequestStatus.DONE
        return GulpRequestStatus(self._stats.status)

    async def _flush_buffer(
        self,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
        **kwargs,
    ) -> tuple[int, int]:
        """
        flushes the ingestion buffer to opensearch, updating the ingestion stats on the collab db.

        once updated, the ingestion stats are sent to the websocket.

        Args:
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            wait_for_refresh (bool, optional): Tell opensearch to wait for index refresh. Defaults to False (faster).
            **kwargs: additional keyword arguments.
        Returns:
            tuple[int,int]: ingested, skipped records
        """

        if self._enrich_during_ingestion:
            # first, enrich documents
            try:
                # call our enrich_documents
                self._docs_buffer = await self._enrich_documents_chunk(
                    self._docs_buffer, **kwargs
                )

                if self._upper_enrich_documents_chunk_fun:
                    # if an upper plugin is stacked, call its enrich_documents too to postprocess
                    # pylint: disable=E1102
                    self._docs_buffer = await self._upper_enrich_documents_chunk_fun(
                        self._docs_buffer, **kwargs
                    )
            except:  # Exception aa ex:
                # MutyLogger.get_instance().exception(ex)
                pass

        # then finally ingest the chunk, use all_fields_on_ws if external query or preview mode
        all_fields_on_ws = self._external_query
        ingested, skipped = await self._ingest_chunk_and_or_send_to_ws(
            self._docs_buffer,
            flt,
            wait_for_refresh,
            all_fields_on_ws=all_fields_on_ws,
        )

        self._tot_failed_in_source += self._records_failed_per_chunk
        self._tot_skipped_in_source += skipped
        self._tot_ingested_in_source += ingested

        # if wait_for_refresh:
        #     # update index type mapping too
        #     el = GulpOpenSearch.get_instance()
        #     self._index_type_mapping = await el.datastream_get_key_value_mapping(
        #         self._index
        #     )
        #     MutyLogger.get_instance().debug(
        #         "got index type mappings with %d entries"
        #         % (len(self._index_type_mapping))
        #     )
        return ingested, skipped

    def _record_failed(self, ex: Exception | str = None) -> None:
        """
        Handles a record failure during ingestion.
        """
        self._records_failed_per_chunk += 1
        if ex:
            MutyLogger.get_instance().exception(ex)

    async def _source_failed(
        self,
        err: str | Exception,
    ) -> None:
        """
        Handles the failure of a source during ingestion.

        Args:
            err (str | Exception): The error that caused the source to fail.
        """
        if isinstance(err, SourceCanceledError):
            # request has benn canceled
            self._req_canceled = True
        else:
            # it's an error
            self._is_source_failed = True
            # if self._stats:
            #     self._stats.status = GulpRequestStatus.FAILED

        if not isinstance(err, str):
            # exception to string
            e = muty.log.exception_to_string(err)  # , with_full_traceback=True)
        else:
            e = err

        MutyLogger.get_instance().error(
            "SOURCE FAILED: source=%s, ex=%s, processed in this source=%d, canceled=%r, failed=%r, ingestion=%r"
            % (
                self._file_path,
                e,
                self._records_processed_per_chunk,
                self._req_canceled,
                self._is_source_failed,
                self._ingestion_enabled,
            )
        )

        # add source info
        ee = "source=%s, %s" % (self._file_path, e)
        self._source_error = ee

    async def _update_source_mapping_parameters(self) -> None:
        """
        add mapping parameters to source, to keep track of which mappings has been used for this source
        """
        if self._plugin_params.mapping_parameters:
            d = {
                "plugin": self.bare_filename,
                "mapping_parameters": self._plugin_params.mapping_parameters.model_dump(exclude_none=True),
            }
            await GulpSource.update_by_id(
                None, self._source_id, d=d, ws_id=None, req_id=None
            )

    async def _source_done(self, flt: GulpIngestionFilter = None, **kwargs) -> None:
        """
        Finalizes the ingestion process for a source by flushing the buffer and updating the ingestion statistics.

        Args:
            flt (GulpIngestionFilter, optional): An optional filter to apply during ingestion. Defaults to None.
            **kwargs: Additional keyword arguments.
        """
        MutyLogger.get_instance().debug(
            "SOURCE DONE: %s, remaining docs to flush in docs_buffer: %d, status=%s, ingestion=%r"
            % (
                self._file_path or self._source_id,
                len(self._docs_buffer),
                self._stats.status if self._stats else GulpRequestStatus.DONE.value,
                self._ingestion_enabled,
            )
        )

        try:
            # flush the last chunk
            ingested, skipped = await self._flush_buffer(
                flt, wait_for_refresh=True, **kwargs
            )
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            self._is_source_failed = True
            ingested = 0
            skipped = 0

        if not self._stats and (not self._ingestion_enabled or self._preview_mode):
            # this also happens on query external
            self._sess = None
            return

        if self._ws_id and not self._raw_ingestion:
            # send ingest_source_done packet on ws
            if self._is_source_failed:
                status = GulpRequestStatus.FAILED
            elif self._req_canceled:
                status = GulpRequestStatus.CANCELED
            else:
                status = GulpRequestStatus.DONE

            GulpWsSharedQueue.get_instance().put(
                type=WSDATA_INGEST_SOURCE_DONE,
                ws_id=self._ws_id,
                user_id=self._user_id,
                operation_id=self._operation_id,
                data=GulpIngestSourceDonePacket(
                    source_id=self._source_id or "default",
                    context_id=self._context_id or "default",
                    req_id=self._req_id,
                    docs_ingested=self._tot_ingested_in_source,
                    docs_skipped=self._tot_skipped_in_source,
                    docs_failed=self._tot_failed_in_source,
                    status=status,
                ),
            )

        if self._stats:

            d = {
                "source_failed": (
                    1 if (self._is_source_failed and not self._raw_ingestion) else 0
                ),
                "source_processed": 1 if not self._raw_ingestion else 0,
                "source_id": self._source_id,
                "records_ingested": ingested,
                "records_skipped": skipped,
                "records_failed": self._records_failed_per_chunk,
                "records_processed": self._records_processed_per_chunk,
                "error": self._source_error,
            }
            if self._raw_ingestion and not self._req_canceled:
                # force status update, keep status as ongoing
                d["status"] = GulpRequestStatus.ONGOING

            try:
                await self._stats.update(
                    self._sess,
                    d,
                    ws_id=self._ws_id,
                    user_id=self._user_id,
                )
            except RequestCanceledError:
                MutyLogger.get_instance().warning("request canceled, source_done!")
            finally:
                self._sess = None
    
    def register_internal_events_callback(self, types: list[str]=None) -> None:
        """
        Register this plugin to be called by the engine for internal events.

        - the plugin must implement `internal_event_callback` method to handle the events.

        Args:
            types (list[str], optional): List of event types to register for. Defaults to None, which registers for all events.
        """
        GulpInternalEventsManager.get_instance().register(self, types)
    
    def deregister_internal_events_callback(self) -> None:
        """
        Stop listening to internal events.
        """
        GulpInternalEventsManager.get_instance().deregister(self.bare_filename)

    @staticmethod
    def load_sync(
        plugin: str,
        extension: bool = False,
        cache_mode: GulpPluginCacheMode = GulpPluginCacheMode.DEFAULT,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        same as calling load, but synchronous.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            cache_mode (GulpPluginCacheMode, optional): The cache mode. Defaults to GulpPluginCacheMode.DEFAULT.
            **kwargs: Additional keyword arguments.
        """
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # if there's already a running event loop, run the coroutine in a separate thread
            from gulp.process import GulpProcess

            executor = GulpProcess.get_instance().thread_pool
            future = executor.submit(
                asyncio.run,
                GulpPluginBase.load(plugin, extension, cache_mode, **kwargs),
            )
            return future.result()

        # either, create a new event loop to run the coroutine
        return loop.run_until_complete(
            GulpPluginBase.load(
                plugin, extension=extension, cache_mode=cache_mode, **kwargs
            )
        )

    @staticmethod
    async def load(
        plugin: str,
        extension: bool = False,
        cache_mode: GulpPluginCacheMode = GulpPluginCacheMode.DEFAULT,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        Load a plugin by name.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            cache_mode (GulpPluginCacheMode, optional): The cache mode. Defaults to GulpPluginCacheMode.DEFAULT.
            **kwargs: Additional keyword arguments.
        """
        # this is set in __reduce__(), which is called when the plugin is pickled(=loaded in another process)
        # pickled=True: running in worker
        # pickled=False: running in main process
        #pickled = kwargs.get("pickled", False)
        pickled = kwargs.pop("pickled", False)

         # get plugin full path by name
        path = GulpPluginBase.path_from_plugin(
            plugin, extension, raise_if_not_found=True
        )

        # try to get plugin from cache
        bare_name = os.path.splitext(os.path.basename(path))[0]
        force_load_from_disk: bool = False
        if cache_mode == GulpPluginCacheMode.IGNORE:
            # ignore cache
            MutyLogger.get_instance().debug(
                "ignoring cache for plugin %s" % (bare_name)
            )
            force_load_from_disk = True
        elif cache_mode == GulpPluginCacheMode.FORCE:
            # force load from disk
            MutyLogger.get_instance().warning("force cache for plugin %s" % (bare_name))
            force_load_from_disk = False
        else:
            # cache mode DEFAULT
            if cache_mode == GulpPluginCacheMode.DEFAULT:
                if GulpConfig.get_instance().plugin_cache_enabled():
                    # cache enabled
                    force_load_from_disk = False
                else:
                    # cache disabled
                    MutyLogger.get_instance().warning(
                        "plugin cache is disabled, loading plugin %s from disk" % (bare_name)
                    )
                    force_load_from_disk = True

        if not force_load_from_disk:
            # use cache
            m: ModuleType = GulpPluginCache.get_instance().get(bare_name)
            if m:
                # return from cache
                return m.Plugin(path, pickled=pickled, **kwargs)

        # load from file
        if extension:
            module_name = f"gulp.plugins.extension.{bare_name}"
        else:
            module_name = f"gulp.plugins.{bare_name}"

        # load from file
        m = muty.dynload.load_dynamic_module_from_file(module_name, path)
        p: GulpPluginBase = m.Plugin(path, pickled=pickled, **kwargs)
        MutyLogger.get_instance().debug(
            f"LOADED plugin m={m}, p={p}, name()={p.name}, pickled={pickled}, depends_on={p.depends_on()}"
        )

        # check dependencies
        if p.depends_on():
            # check each dependency (each is a file name)
            for dep in p.depends_on():
                # check if dependency is loaded
                dep_path_noext = GulpPluginBase.path_from_plugin(dep, is_extension=False, raise_if_not_found=False)
                dep_path_ext = GulpPluginBase.path_from_plugin(dep, is_extension=True, raise_if_not_found=False)
                dep_path = dep_path_noext or dep_path_ext
                if not dep_path:
                    await p.unload()
                    raise FileNotFoundError(
                        f"dependency {dep} not found, plugin={bare_name} cannot load, path={path}"
                    )

        # also call post-initialization routine if any
        await p.post_init(**kwargs)

        if cache_mode != GulpPluginCacheMode.IGNORE and GulpConfig.get_instance().plugin_cache_enabled():
            # add to cache
            GulpPluginCache.get_instance().add(m, bare_name)
        return p

    async def unload(self) -> None:
        """
        unload the plugin module, removing it from cache if needed

        NOTE: the plugin module is **no more valid** after this function returns.

        - if plugin cache is enabled, this method does nothing.
        - implementers must call super().unload() at the end of their unload method.

        Returns:
            None
        """

        # clear stuff
        self._sess = None
        self._stats = None
        self._mappings.clear()
        self._index_type_mapping.clear()
        self._upper_record_to_gulp_document_fun = None
        self._upper_enrich_documents_chunk_fun = None
        self._upper_instance = None
        self._docs_buffer.clear()
        self._extra_docs.clear()
        self._extra_docs: list[dict]
        self._plugin_params = None
        self._operation = None

        # empty internal events queue
        self.deregister_internal_events_callback()
        if GulpConfig.get_instance().plugin_cache_enabled():
            # do not unload if cache is enabled
            return

        MutyLogger.get_instance().debug("unloading plugin: %s" % (self.bare_filename))
        GulpPluginCache.get_instance().remove(self.bare_filename)

        # # finally delete the module
        # if self.type() == GulpPluginType.EXTENSION:
        #     module_name = f"gulp.plugins.extension.{self.bare_filename}"
        # else:
        #     module_name = f"gulp.plugins.{self.bare_filename}"
        # if module_name in sys.modules:
        #     del sys.modules[module_name]

    @staticmethod
    def path_from_plugin(
        plugin: str, is_extension: bool = False, raise_if_not_found: bool = False
    ) -> str:
        """
        Get the path of a plugin.

        if the extra path is set and the plugin exists there, such path is returned.
        either, path in the default plugins path is returned.
        
        Args:
            plugin (str): The name of the plugin, may include ".py/.pyc" extension.
            is_extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            raise_if_not_found (bool, optional): Whether to raise an exception if the plugin is not found. Defaults to False.
        Returns:
            str: The path of the plugin, or None
        Raises:
            FileNotFoundError: If the plugin is not found and raise_if_not_found is True.
        """

        def _check_path(path: str) -> str:
            """
            check if a file exists with .py or .pyc extension.

            Args:
                path (str): full path of the file, with or without extension
            Returns:
                str: the full path of the file if found, None otherwise
            """

            # check if extension is already there
            if path.endswith(".py") or path.endswith(".pyc"):
                if os.path.exists(path):
                    # return full path
                    return os.path.abspath(os.path.expanduser(path))

            # return py or pyc file if found
            for ext in [".py", ".pyc"]:
                with_ext_path = f"{path}{ext}"
                if os.path.exists(with_ext_path):
                    # return full path
                    p = os.path.abspath(os.path.expanduser(with_ext_path))
                    return p
            return None

        def _get_plugin_path(plugin: str, is_extension: bool) -> str:
            """
            Check if a plugin exists in the default or extra path.

            Args:
                plugin (str): The name of the plugin.
                is_extension (bool): Whether the plugin is an extension.
            Returns:
                str: The path of the plugin if found.
            """
            extra_path = GulpConfig.get_instance().path_plugins_extra()
            default_path = GulpConfig.get_instance().path_plugins_default()
            if is_extension:
                # add extension
                extra_path = muty.file.safe_path_join(extra_path, "extension")
                default_path = muty.file.safe_path_join(default_path, "extension")

            # first we check in extra_path
            if extra_path and os.path.exists(extra_path):
                p = _check_path(muty.file.safe_path_join(extra_path, plugin.lower()))
                if p:
                    return p

            # then in default path
            return _check_path(muty.file.safe_path_join(default_path, plugin.lower()))

        # check if its already an absolute path
        if os.path.abspath(plugin) == plugin and os.path.exists(plugin):
            return os.path.abspath(os.path.expanduser(plugin))

        # get plugin path
        p = _get_plugin_path(plugin, is_extension)
        if not p and raise_if_not_found:
            raise FileNotFoundError(
                f"plugin {plugin} not found, plugins_path={GulpConfig.get_instance().path_plugins_default()}, extra_path={GulpConfig.get_instance().path_plugins_extra()}"
            )
        return p

    @staticmethod
    async def list_plugins(
        name: str = None, extension_only: bool = False
    ) -> list[GulpPluginEntry]:
        """
        List available plugins.

        Args:
            name (str, optional): if set, only plugins with filename matching (case-insensitive) this will be returned. Defaults to None.
            extension_only (bool, optional): if set, only extensions will be returned. Defaults to False.
        Returns:
            list[GulpPluginEntry]: The list of available plugins.
        """

        def _exists(l: list[GulpPluginEntry], filename: str) -> bool:
            # check if filename exists in the list
            for p in l:
                if p.filename.lower() == filename.lower():
                    return True
            return False

        async def _list_internal(
            l: list[GulpPluginEntry],
            path: str,
            name: str = None,
            extension_only: bool = False,
        ) -> None:
            """
            append found plugins to l list.
            """
            MutyLogger.get_instance().debug("listing plugins in %s ..." % (path))
            path_extension = os.path.join(path, "extension")
            plugins = await muty.file.list_directory_async(path, "*.py*")
            extensions = await muty.file.list_directory_async(path_extension, "*.py*")
            files = plugins + extensions
            for f in files:
                if "__init__" in f or "__pycache__" in f or "/ui/" in f:
                    continue

                if "/extension/" in f:
                    extension = True
                else:
                    extension = False

                if extension_only and not extension:
                    continue

                try:
                    p = await GulpPluginBase.load(
                        f, extension=extension, cache_mode=GulpPluginCacheMode.IGNORE
                    )
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    MutyLogger.get_instance().error("could not load plugin %s" % (f))
                    continue
                if name is not None:
                    # filter by name
                    if name.lower() not in p.bare_filename.lower():
                        continue

                if _exists(l, p.filename):
                    MutyLogger.get_instance().warning(
                        "skipping plugin %s, already exists in list (in another path)"
                        % (f)
                    )
                    await p.unload()
                    continue

                # got entry
                try:
                    entry = GulpPluginEntry(
                        path=f,
                        display_name=p.display_name(),
                        type=p.type(),
                        desc=p.desc(),
                        regex=p.regex(),
                        ui=p.ui(),
                        filename=p.filename,
                        custom_parameters=p.custom_parameters(),
                        depends_on=p.depends_on(),
                        tags=p.tags(),
                        version=p.version(),
                        data=p.data(),
                    )

                    l.append(entry)
                except Exception as ex:
                    # something wrong in the value returned from plugin methods, cannot add ....
                    MutyLogger.get_instance().exception(ex)

                await p.unload()

        l: list[GulpPluginEntry] = []

        # list extra folder first, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            await _list_internal(l, p, name, extension_only)
            MutyLogger.get_instance().debug(
                "found %d plugins in extra path=%s" % (len(l), p)
            )

        # list default path
        default_path = GulpConfig.get_instance().path_plugins_default()
        await _list_internal(l, default_path, name, extension_only)
        return l

    @staticmethod
    async def list_ui_plugins() -> list[GulpUiPluginMetadata]:
        """
        List available UI plugins.

        Args:
        Returns:
            list[GulpUiPluginMetadata]: The list of available UI plugins.   
        """

        async def _list_ui_internal(
            l: list[GulpUiPluginMetadata],
            path: str,
        ) -> None:
            """
            append found plugins to l list.
            """
            MutyLogger.get_instance().debug("listing UI plugins in %s ..." % (path))
            files: list[str] = await muty.file.list_directory_async(path)
            for f in files:
                # skip non ts/js files
                f_lower: str = f.lower()
                if not f_lower.endswith(".ts") and not f_lower.endswith(".js") and not f_lower.endswith(".tsx"):
                    continue
                
                # we have the plugin path, now build the corresponding metadata file (json) path
                metadata_file = os.path.join(path, f + ".json")
                if not await muty.file.exists_async(metadata_file):
                    MutyLogger.get_instance().warning(
                        "plugin metadata file %s not found, skipping plugin %s" % (metadata_file, f)
                    )
                    continue

                metadata: bytes = await muty.file.read_file_async(metadata_file)
                js: dict = json5.loads(metadata)
                try:
                    mtd: GulpUiPluginMetadata = GulpUiPluginMetadata.model_validate(js)

                    # add path and filename
                    mtd.path = os.path.abspath(os.path.expanduser(f))
                    mtd.filename = os.path.basename(f)
                    l.append(mtd)
                except ValidationError as ve:
                    MutyLogger.get_instance().exception(ve)
                    MutyLogger.get_instance().warning(
                        "plugin metadata file %s is not valid, skipping plugin %s:\n%s" % (metadata_file, f, js)
                    )
                    continue

                # got entry

        l: list[GulpUiPluginMetadata] = []

        # list extra folder first, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            await _list_ui_internal(l, os.path.join(p,"ui"))
            MutyLogger.get_instance().debug(
                "found %d UI plugins in extra path=%s" % (len(l), p)
            )

        # list default path
        default_path = GulpConfig.get_instance().path_plugins_default()
        await _list_ui_internal(l, os.path.join(default_path, "ui"))
        return l
