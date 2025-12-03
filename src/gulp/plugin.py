"""Gulp plugin base class and plugin utilities."""

import asyncio
import inspect
import ipaddress
import os
import sys
from abc import ABC, abstractmethod
from copy import deepcopy
from enum import StrEnum
from types import ModuleType
from typing import Annotated, Any, Callable, Optional

import json5
import muty.dict
import muty.dynload
import muty.file
import muty.log
import muty.pydantic
import muty.string
import muty.time
import orjson
from fastapi import WebSocketDisconnect
from muty.log import MutyLogger
from opensearchpy import Field
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import (
    GulpRequestStats,
    GulpUpdateDocumentsStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping, GulpMappingField, GulpMappingFile
from gulp.api.opensearch.filters import (
    QUERY_DEFAULT_FIELDS,
    GulpDocumentFilterResult,
    GulpIngestionFilter,
    GulpQueryFilter,
)
from gulp.api.opensearch.structs import (
    GulpDocument,
    GulpQueryHelpers,
    GulpQueryParameters,
)
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import (
    WSDATA_DOCUMENTS_CHUNK,
    WSDATA_INGEST_SOURCE_DONE,
    WSDATA_USER_LOGIN,
    WSDATA_USER_LOGOUT,
    GulpDocumentsChunkPacket,
    GulpIngestSourceDonePacket,
    GulpQueryDonePacket,
    GulpRedisBroker,
)
from gulp.config import GulpConfig
from gulp.structs import (
    GulpDocumentsChunkCallback,
    GulpMappingParameters,
    GulpPluginCustomParameter,
    GulpPluginParameters,
    ObjectNotFound,
)


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


class GulpIngestInternalEvent(BaseModel):
    """
    this is sent  at the end of each source ingestion in GulpInternalEvent.data by the engine to plugins registered to the GulpInternalEventsManager.EVENT_INGEST event
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "user_id": "user123",
                    "operation_id": "op123",
                    "req_id": "req123",
                    "file_path": "/path/to/file.evtx",
                    "plugin_params": {"param1": "value1", "param2": 2},
                    "errors": [],
                    "records_ingested": 1000,
                    "records_failed": 10,
                    "records_skipped": 5,
                    "records_processed": 1015,
                }
            ]
        },
    )
    plugin: Annotated[
        str, Field(description="The plugin (internal) name performing the ingestion.")
    ]
    user_id: Annotated[str, Field(description="The user id performing the ingestion.")]
    operation_id: Annotated[
        str, Field(description="The operation id associated with this ingestion.")
    ]
    req_id: Annotated[str, Field(description="The request id.")]
    file_path: Annotated[str, Field(description="The file path being ingested.")]
    status: Annotated[
        str,
        Field(
            description='The ingestion status, "done", "failed", "canceled".',
        ),
    ]
    plugin_params: Annotated[
        dict, Field(description="The plugin parameters used for the ingestion.")
    ]
    errors: Annotated[
        list[str],
        Field(description="A list of errors encountered during ingestion."),
    ] = []
    records_ingested: Annotated[
        int, Field(description="The total number of records ingested.")
    ] = 0
    records_failed: Annotated[
        int,
        Field(
            description="The total number of records that failed to be ingested.",
        ),
    ] = 0
    records_skipped: Annotated[
        int,
        Field(
            description="The total number of records skipped during ingestion.",
        ),
    ] = 0
    records_processed: Annotated[
        int, Field(description="The total number of records processed.")
    ] = 0


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
                    "user_id": "user123",
                    "operation_id": "op123",
                    "data": {"ip": "192.168.2.10"},
                }
            ]
        },
    )
    type: Annotated[
        str,
        Field(
            description="The type of the event (e.g. login, logout, ingestion, etc.).",
        ),
    ]
    timestamp_msec: Annotated[
        int,
        Field(
            description="The timestamp of the event in milliseconds since epoch.",
        ),
    ]
    data: Annotated[
        dict,
        Field(
            description="Arbitrary data for the event.",
        ),
    ] = {}
    user_id: Annotated[
        Optional[str],
        Field(
            description="The user id associated with the event.",
        ),
    ] = None
    operation_id: Annotated[
        Optional[str],
        Field(
            description="The operation id associated with the event, if applicable.",
        ),
    ] = None
    req_id: Annotated[
        Optional[str],
        Field(
            description="The request id associated with the event, if applicable.",
        ),
    ] = None


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

    # plugin display name
    display_name: Annotated[
        str,
        Field(description="The plugin display name."),
    ]

    # supported plugin types
    type: Annotated[
        GulpPluginType,
        Field(description="One of the GulpPluginType values."),
    ]

    # file path associated with the plugin
    path: Annotated[
        str,
        Field(description="The file path associated with the plugin."),
    ]

    # bare filename without extension (internal plugin name)
    filename: Annotated[
        str,
        Field(
            description="This is the bare filename without extension (aka the `internal plugin name`, to be used as `plugin` throughout the whole gulp API)."
        ),
    ]

    # description of the plugin
    desc: Annotated[
        Optional[str],
        Field(description="A description of the plugin."),
    ] = None

    # arbitrary data for the UI
    data: Annotated[
        Optional[dict],
        Field(description="Arbitrary data for the UI."),
    ] = None

    # list of custom parameters this plugin supports
    custom_parameters: Annotated[
        Optional[list[GulpPluginCustomParameter]],
        Field(description="A list of custom parameters this plugin supports."),
    ] = []

    # tables created by the plugin
    tables: Annotated[
        Optional[list[str]],
        Field(description="A list of collab tables created by the plugin, if any."),
    ] = []

    # list of plugins this plugin depends on
    depends_on: Annotated[
        Optional[list[str]],
        Field(description="A list of plugins this plugin depends on."),
    ] = []

    # list of tags for the plugin
    tags: Annotated[
        Optional[list[str]],
        Field(description="A list of tags for the plugin."),
    ] = []

    # plugin version
    version: Annotated[
        Optional[str],
        Field(description="The plugin version."),
    ] = None

    # regex to identify the data type
    regex: Annotated[
        Optional[str],
        Field(
            description="A regex to identify the data type (i.e. to identify the file header), for ingestion plugin only."
        ),
    ] = None

    # HTML frame to be rendered in the UI
    ui: Annotated[
        Optional[str],
        Field(
            description="HTML frame to be rendered in the UI, i.e. for custom plugin panel."
        ),
    ] = None


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

    display_name: Annotated[
        str,
        Field(
            description="The plugin display name.",
        ),
    ]
    plugin: Annotated[
        str,
        Field(
            description="The related Gulp plugin.",
        ),
    ]
    extension: Annotated[
        bool,
        Field(
            description="True if the related Gulp plugin is an extension.",
        ),
    ] = True

    version: Annotated[
        Optional[str],
        Field(
            None,
            description="The plugin version.",
        ),
    ] = None

    desc: Annotated[
        Optional[str],
        Field(
            None,
            description="A description of the plugin.",
        ),
    ] = None


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
            name (str): The internal name of the plugin.
        """
        if name not in self._cache:
            MutyLogger.get_instance().debug("adding plugin %s to cache" % (name))
            self._cache[name] = plugin
        else:
            MutyLogger.get_instance().warning(
                "plugin %s already in cache, not adding" % (name)
            )

    def get(self, name: str) -> ModuleType:
        """
        Get a plugin from the cache.

        Args:
            name (str): The internal name of the plugin to get.
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
            name (str): The internal name of the plugin to remove from the cache.
        """
        if name in self._cache:
            MutyLogger.get_instance().debug("removing plugin %s from cache" % (name))
            del self._cache[name]


class GulpInternalEventsManager:
    """
    Singleton class to manage internal (local) events

    local events are broadcasted by the engine to registered plugins.

    a plugin registers to receive local events by calling GulpInternalEventsManager.register(plugin, types) where `types` is a list of event types the plugin is interested in.

    when an event is broadcasted (by core itself or by a plugin, calling GulpInternalEventsManager.broadcast_event), core calls the `internal_event_callback` method of each registered plugin that is interested in the event type.
    """

    _instance: "GulpInternalEventsManager" = None

    # these events are broadcasted by core itself to registered plugins
    EVENT_LOGIN: str = WSDATA_USER_LOGIN  # data=GulpUserAccessPacket
    EVENT_LOGOUT: str = WSDATA_USER_LOGOUT  # data=GulpUserAccessPacket
    EVENT_INGEST: str = "ingestion"
    EVENT_DELETE_OPERATION: str = "delete_operation"

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
            GulpInternalEventsManager: The local events manager instance.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def clear(self):
        """
        Clear the local events manager plugins list
        """
        self._plugins = {}

    def register(self, plugin: "GulpPluginBase", types: list[str] = None) -> None:
        """
        Register a plugin to receive local events.

        Args:
            plugin (GulpPluginBase): The plugin to register.
            types (list[str], optional): a list of events the plugin is interested in
        """
        name: str = plugin.name
        if name not in self._plugins.keys():
            MutyLogger.get_instance().debug(
                "registering plugin %s to receive local events: %s", name, types
            )
            self._plugins[name] = {
                "plugin_instance": plugin,  # the plugin instance
                "types": types if types else [],
            }
        # else:
        #     MutyLogger.get_instance().warning(
        #         "plugin %s already registered to receive local events" % (name)
        #     )

    def deregister(self, plugin: str) -> None:
        """
        Stop a plugin from receiving local events.

        Args:
            plugin (str): The name of the plugin to unregister.
        """
        if plugin in self._plugins.keys():
            MutyLogger.get_instance().debug(
                "deregistering plugin %s from receiving local events", plugin
            )
            del self._plugins[plugin]
        else:
            MutyLogger.get_instance().debug(
                "plugin %s not registered to receive local events", plugin
            )

    async def broadcast_event(
        self,
        t: str,
        data: dict = None,
        user_id: str = None,
        req_id: str = None,
        operation_id: str = None,
    ) -> None:
        """
        Broadcast an event to all plugins registered to receive it.

        NOTE: this can be used by the main process only.
        in workers, plugins should call GulpRedisBroker.put() to broadcast an event.

        Args:
            t: str: the event (must be previously registered with GulpInternalEventsManager.register)
            data (dict, optional): The data to send with the event (event-specific). Defaults to None.
            user_id (str, optional): the user id associated with this event. Defaults to None.
            req_id (str): the request id associated with this event.
            operation_id (str, optional): the operation id if applicable. Defaults to None.
        Returns:
            None
        """
        ev: GulpInternalEvent = GulpInternalEvent(
            type=t,
            timestamp_msec=muty.time.now_msec(),
            data=data,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
        )
        for _, entry in self._plugins.items():
            p: GulpPluginBase = entry["plugin_instance"]
            if t in entry["types"]:
                try:
                    MutyLogger.get_instance().debug(
                        "broadcasting event %s to plugin %s", t, p.name
                    )
                    await p.internal_event_callback(ev)
                except Exception as e:
                    MutyLogger.get_instance().exception(e)


class GulpPluginBase(ABC):
    """
    Base class for all Gulp plugins.
    """

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
        extension = self.type() == GulpPluginType.EXTENSION

        # return the reconstruction information as a tuple
        return (
            GulpPluginBase.load_pickled,  # callable to recreate the object
            (self.path, extension, GulpPluginCacheMode.IGNORE),  # args for callable
            self.__dict__,  # object state to restore
        )

    def __init__(
        self,
        path: str,
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        """
        Initialize a new instance of the class.

        Args:
            path (str): The file path associated with the plugin.
            module_name (str): The module name in sys.modules
            pickled (bool, optional, INTERNAL): Whether the plugin is pickled. Defaults to False.
                this should not be changed, as it is used by the pickle module to serialize the object when it is passed to the multiprocessing module.
        Returns:
            None

        """
        # print("********************************** INIT *************************************************")
        super().__init__()
        # enforce callback type
        self._enrich_documents_chunk_cb: GulpDocumentsChunkCallback = (
            self._enrich_documents_chunk
        )

        # plugin file path
        self.path: str = path
        self.module_name: str = module_name

        # print("*** GulpPluginBase.__init__ (%s, %s, %s) called!!!! ***" % (path, module_name, self.display_name()))

        # to have faster access to plugin filename and internal name
        self.filename: str = os.path.basename(self.path)
        self.name = os.path.splitext(self.filename)[0]

        # tell if the plugin has been pickled by the multiprocessing module (internal)
        self._pickled: bool = pickled

        # if set, this plugin have another plugin on top
        self._stacked: bool = False

        #
        # the followin  are stored in the plugin instance at the query/ingest entrypoint
        #

        # SQLAlchemy session
        self._sess: AsyncSession | None = None

        # request stats
        self._stats: GulpRequestStats | None = None

        # for ingestion, the mappings to apply
        self._mappings: dict[str, GulpMapping] = {}
        # for ingestion, the key in the mappings dict to be used
        self._mapping_id: str = None
        # calling user
        self._user_id: str = None
        # current gulp operation
        self._operation_id: str = None
        # current gulp context id
        self._context_id: str = None
        # current gulp source id
        self._source_id: str = None
        # used instead of _context_id and _source_id when they're generated at runtime by the mapping rules
        self._ctx_src_pairs: list[tuple[str, str]] = []

        # for custom value aliases mapping
        self._record_type: str = None

        # current file being ingested
        self._file_path: str = None
        self._adaptive_chunk_size: int = 0

        # original file path, if any
        self._original_file_path: str = None
        # opensearch index to operate to
        self._index: str = None
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
        self._upper_record_to_gulp_document_fun: Callable | None = None
        self._upper_enrich_documents_chunk_fun: Callable | None = None
        self._upper_instance: GulpPluginBase | None = None
        self._lower_instance: GulpPluginBase | None = None

        # to bufferize gulpdocuments
        self._docs_buffer: list[dict] = []

        # this is used by the engine to generate extra documents from a single gulp document
        self._extra_docs: list[dict] = []

        # to keep track of processed/ingested/failed/skipped records
        # total here means "for this source", not total for the whole ingestion
        self._records_processed_per_chunk: int = 0
        self._records_failed_per_chunk: int = 0
        self._records_processed_total: int = 0
        self._records_failed_total: int = 0
        self._records_skipped_total: int = 0
        self._records_ingested_total: int = 0

        # to keep track of ingested chunks
        self._chunks_ingested: int = 0

        # corresponding request is canceled and we should stop processing
        self._req_canceled: bool = False

        self._plugin_params: GulpPluginParameters = GulpPluginParameters()

        # to minimize db requests to postgres to get context and source at every record
        self._ctx_cache: dict = {}
        self._src_cache: dict = {}
        self._operation: GulpOperation | None = None

        # for preview mode
        self._preview_mode: bool = False
        self._preview_chunk: list[dict] = []

        # indicates the last chunk in a raw ingestion
        self._last_raw_chunk: bool = False
        self._raw_flush_count: int = 0

    def check_license(self, throw_on_invalid: bool = True) -> bool:
        """
        stub method for license checking, overridden by make_paid.py for paid plugins.

        if the plugin is not protected, this method does nothing and returns True.

        Args:
            throw_on_invalid (bool, optional): whether to throw an exception if the license is not valid. Defaults to True.
        Returns:
            bool: True if the license is valid, False otherwise
        Throws:
            ValueError: if the license is invalid and throw_on_invalid is True
        """
        return True

    @abstractmethod
    def display_name(self) -> str:
        """
        Returns the plugin display name.
        """

    @abstractmethod
    def type(self) -> GulpPluginType:
        """
        type of this plugin, must be one of GulpPluginType
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

    def tables(self) -> list[str]:
        """
        Returns a list of collab tables created by the plugin, if any.
        """
        return []

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

    def ui(self) -> str | None:
        """
        Returns HTML frame to be rendered in the UI, i.e. for custom plugin panel
        """
        return None

    def depends_on(self) -> list[str]:
        """
        Returns a list of plugins this plugin depends on (plugin internal names).
        """
        return []

    def regex(self) -> str | None:
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
        returns the accumulated preview chunk as a list of GulpDocument dictionaries.
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

    async def broadcast_ingest_internal_event(self) -> None:
        """
        broadcast internal ingest metrics event (in the end of each source ingestion) to plugins registered to the GulpInternalEventsManager.EVENT_INGEST event
        """
        assert self._stats
        ev: GulpIngestInternalEvent = GulpIngestInternalEvent(
            plugin=self.name,
            user_id=self._user_id,
            operation_id=self._operation_id,
            req_id=self._req_id,
            file_path=(
                self._original_file_path
                if self._original_file_path
                else self._file_path
            ),
            plugin_params=self._plugin_params.model_dump(exclude_none=True),
            errors=self._stats.errors,
            status=self._stats.status,
            records_ingested=self._records_ingested_total,
            records_failed=self._records_failed_total,
            records_skipped=self._records_skipped_total,
            records_processed=self._records_processed_total,
        )
        # MutyLogger.get_instance().debug(
        #     "***************************** broadcasting internal ingest event: %s", ev
        # )
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.put_internal_event(
            GulpInternalEventsManager.EVENT_INGEST,
            user_id=self._user_id,
            operation_id=self._operation_id,
            req_id=self._req_id,
            data=ev.model_dump(),
        )

    async def flush_buffer_and_send_to_ws(
        self,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
        all_fields_on_ws: bool = False,
    ) -> tuple[int, int, int]:
        """
        flush the internal doc buffer: send documents to opensearch and to the websocket.

        Args:
            flt (GulpIngestionFilter, optional): the ingestion filter to apply. Defaults to None.
            wait_for_refresh (bool, optional): whether to wait for refresh after ingestion. Defaults to False.
            all_fields_on_ws (bool, optional): whether to send all fields to the websocket. Defaults to False (only default fields)
        Returns:
            tuple[int, int, int]: the number of skipped, ingested, and failed documents
        """
        if not self._docs_buffer:
            return 0, 0, 0

        if self._external_query:
            # for external queries, always send all fields to the websocket
            all_fields_on_ws = True

        MutyLogger.get_instance().debug(
            "processing chunk of %d documents with plugin=%s",
            len(self._docs_buffer),
            self.name,
        )

        el = GulpOpenSearch.get_instance()
        skipped: int = 0
        ingested_docs: list[dict] = []
        if self._raw_ingestion:
            # disable refresh for raw ingestion, unless it is the last chunk
            if self._last_raw_chunk:
                wait_for_refresh = True
            else:
                wait_for_refresh = False

        # MutyLogger.get_instance().debug(orjson.dumps(self._docs_buffer, option=orjson.OPT_INDENT_2).decode())
        # MutyLogger.get_instance().debug('flushing ingestion buffer, len=%d',len(self.buffer))

        # perform ingestion, ingested_docs may be different from self._docs_buffer in the end due to skipped documents
        skipped, ingested_docs, failed = await el.bulk_ingest(
            self._index,
            self._docs_buffer,
            flt=flt,
            wait_for_refresh=wait_for_refresh,
        )
        # print(orjson.dumps(ingested_docs, option=orjson.OPT_INDENT_2).decode())
        if failed > 0:
            # NOTE: errors here means something wrong with the format of the documents, and must be fixed ASAP.
            # ideally, function should NEVER append errors and the errors total should be the same before and
            # after this function returns (this function may only change the skipped total, which means some duplicates were found).
            if GulpConfig.get_instance().debug_abort_on_opensearch_ingestion_error():
                raise Exception(
                    "failed=%d, opensearch ingestion errors means GulpDocument contains invalid data, review errors on collab db!" % (failed)
                )
    
        def __select_ws_doc_fields(doc: dict) -> dict:
            """
            patch document to send to ws, either all fields or only default fields
            """
            return (
                doc
                if all_fields_on_ws
                else {
                    field: doc[field] for field in QUERY_DEFAULT_FIELDS if field in doc
                }
            )

        # send ingested docs to websocket
        if flt:
            # copy filter to avoid changing the original, if any,
            flt = deepcopy(flt)

            # ensure data on ws is filtered
            flt.storage_ignore_filter = False

            ws_docs = [
                __select_ws_doc_fields(doc)
                for doc in ingested_docs
                if GulpIngestionFilter.filter_doc_for_ingestion(doc, flt)
                == GulpDocumentFilterResult.ACCEPT
            ]
        else:
            # no filter, send all ingested docs to ws
            ws_docs = [__select_ws_doc_fields(doc) for doc in ingested_docs]

        if ws_docs:
            MutyLogger.get_instance().debug("adding %d docs to ws", len(ws_docs))
            # send documents to the websocket
            chunk: GulpDocumentsChunkPacket = GulpDocumentsChunkPacket(
                docs=ws_docs,
                chunk_size=len(ws_docs),
                # wait for refresh is set only on the last chunk
                last=wait_for_refresh,
                chunk_number=self._chunks_ingested,
            )
            # MutyLogger.get_instance().debug(
            #     "sending chunk of %d documents to ws_id=%s", len(ws_docs), self._ws_id
            # )
            redis_broker = GulpRedisBroker.get_instance()
            await redis_broker.put(
                t=WSDATA_DOCUMENTS_CHUNK,
                ws_id=self._ws_id,
                operation_id=self._operation_id,
                user_id=self._user_id,
                req_id=self._req_id,
                d=chunk.model_dump(exclude_none=True),
            )
            self._chunks_ingested += 1

        # check if the request is canceled
        canceled: bool = await GulpRequestStats.is_canceled(self._sess, self._req_id)
        if canceled:
            self._req_canceled = True
            MutyLogger.get_instance().warning(
                "_process_docs_chunk: request %s canceled!", self._req_id
            )

        # MutyLogger.get_instance().debug("returning %d ingested, %d skipped, %d failed",l, skipped, failed)
        ingested: int = len(ingested_docs)
        self._records_skipped_total += skipped
        self._records_ingested_total += ingested
        self._records_failed_total += failed
        return skipped, ingested, failed

    async def _context_id_from_doc_value(
        self, k: str, v: str, force_v_as_context_id: bool = False
    ) -> str:
        """
        get "gulp.context_id" from cache or create new GulpContext based on the key and value

        Args:
            k (str): name of the field (i.e. "gulp.context_id")
            v (str): field's value
            force_v_as_context_id (bool): if True, forces the use of `v` as the context ID when creating a new context. Defaults to False.
        Returns:
            str: gulp.context_id

        """
        # check cache first
        cache_key: str = f"{k}-{v}"
        if cache_key in self._ctx_cache:
            # MutyLogger.get_instance().debug("found context %s in cache, returning id %s", v, self._ctx_cache[cache_key])
            # return cached context id
            return self._ctx_cache[cache_key]

        # cache miss - create new context (or get existing)
        if not self._operation:
            # we need the operation object, lazy load
            self._operation = await GulpOperation.get_by_id(
                self._sess, self._operation_id
            )

        context: GulpContext
        context, created = await self._operation.add_context(
            self._sess,
            self._user_id,
            v,
            self._ws_id,
            self._req_id,
            ctx_id=v if force_v_as_context_id else None,
        )

        # update cache
        self._ctx_cache[cache_key] = context.id
        MutyLogger.get_instance().debug(
            "context name=%s, id=%s added to cache, created=%r", v, context.id, created
        )
        return context.id

    async def _source_id_from_doc_value(
        self, context_id: str, k: str, v: str, force_v_as_source_id: bool = False
    ) -> str:
        """
        get "gulp.source_id" from cache or create new GulpSource based on the key and value

        Args:
            context_id (str): parent context id
            k (str): name of the field (i.e. "gulp.source_id")
            v (str): field's value
            force_v_as_source_id (bool): if True, forces the use of `v` as the source ID when creating a new source. Defaults to False.
        Returns:
            str: gulp.source_id or None if context_id is None

        """
        if not context_id:
            MutyLogger.get_instance().error(
                "context_id is None, cannot create source for key %s, value %s", k, v
            )

            return None

        # check cache first
        cache_key: str = f"{context_id}-{k}-{v}"
        if cache_key in self._src_cache:
            # MutyLogger.get_instance().debug("found source %s in cache for context %s, returning id %s",v,context_id, self._src_cache[cache_key])
            # return cached source id
            return self._src_cache[cache_key]

        # cache miss - create new source (or get existing)

        # fetch context object
        context: GulpContext = await GulpContext.get_by_id(self._sess, context_id)

        # create source
        mapping_parameters = self._plugin_params.mapping_parameters
        source, created = await context.add_source(
            self._sess,
            self._user_id,
            v,
            ws_id=self._ws_id,
            req_id=self._req_id,
            src_id=v if force_v_as_source_id else None,
            plugin=self.name,
            mapping_parameters=mapping_parameters,
        )

        # update cache
        self._src_cache[cache_key] = source.id
        MutyLogger.get_instance().debug(
            "source name=%s, context_id=%s, id=%s added to cache, created=%r",
            v,
            context_id,
            source.id,
            created,
        )

        return source.id

    async def query_external(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        q: Any,
        index: str,
        plugin_params: GulpPluginParameters,
        q_options: GulpQueryParameters = None,
        **kwargs,
    ) -> tuple[int, int]:
        """
        query an external source, convert results to gulpdocument dictionaries, ingest them and stream them to the websocket.

        NOTE: this is guaranteed to be called by the gulp API in a worker process, unless q_options.prweview_mode is set

        Args:
            sess (AsyncSession): The database session.
            stats (GulpRequestStats): the request stats, ignored if q_options.preview_mode is set
            user_id (str): the user performing the query, ignored if q_options.preview_mode is set
            req_id (str): the request id, ignored if q_options.preview_mode is set
            ws_id (str): the websocket id to receive WSDATA_DOCUMENTS_CHUNK, WSDATA_QUERY_DONE, WSDATA_QUERY_GROUP_MATCH packets, ignored if q_options.preview_mode is set
            operation_id (str): the operation id
            q(Any): the query to perform, format is plugin specific
            plugin_params (GulpPluginParameters): the plugin parameters, they are mandatory here (custom_parameters should usually be set with specific external source parameters, i.e. how to connect)
            q_options (GulpQueryParameters): additional query options, defaults to None (use defaults)
            index (str, optional): the gulp's operation index to ingest into during query, ignored if q_options.preview_mode is set
            kwargs: additional keyword arguments
        Notes:
            - implementers must call super().query_external first

        Returns:
            tuple:
            - total_processed (int): The number of documents processed (unless limit, preview mode or errors this will be equal to total_hits).
            - total_hits (int): The total number of hits found.
        Raises:
            any exception encountered during the query
        """
        MutyLogger.get_instance().debug(
            "GulpPluginBase.query_external: q=%s, sess=%s, index=%s, operation_id=%s, q_options=%s, plugin_params=%s, kwargs=%s",
            q,
            sess,
            index,
            operation_id,
            q_options,
            plugin_params,
            kwargs,
        )

        # setup the engine in external query mode:
        # the flow is similar to ingest_file, with the difference the documents are queried from an external source instead of being read from a file
        # once read (in chunks), they are processed the same way as in ingestion, i.e. converted to GulpDocuments, ingested and sent to the websocket
        self._sess = sess
        self._stats = stats
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._external_query = True
        self._operation_id = operation_id
        self._index = index

        # initialize
        await self._initialize(plugin_params=plugin_params)
        if q_options.preview_mode:
            self._preview_mode = True
            MutyLogger.get_instance().warning(
                "***PREVIEW MODE** enabled for external query!"
            )

        return (0, 0)

    async def ingest_raw(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        chunk: bytes,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        last: bool = False,
        **kwargs,
    ) -> GulpRequestStatus:
        """
        ingest a chunk of arbitrary data

        NOTE: implementers must call super().ingest_raw first
        NOTE: this is guaranteed to be called by the engine inside a try/except which reraises the generated exception.
        NOTE: it is the responsibility of the plugin to create a GulpContext and a GulpSource from each document's data, if they're not already there (as when ingesting raw GulpDocuments with the default `raw` plugin).

        Args:
            sess (AsyncSession): The database session.
            stats (GulpRequestStats): The ingestion stats, to be updated by the plugin during ingestion.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation_id (str): id of the operation on collab database.
            chunk: bytes: a raw bytes buffer containing the raw data to be converted to GulpDocuments.
            stats (GulpRequestStats): The ingestion stats.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            plugin_params (GulpPluginParameters, optional): The plugin parameters. Defaults to None.
            last: bool, whether this is the last chunk to ingest, defaults to False
            **kwargs: additional keyword arguments

        Returns:
            GulpRequestStatus: The status of the ingestion.

        Raises:
            any exception encountered during ingestion

        """
        self._sess = sess
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._operation_id = operation_id
        self._index = index
        self._raw_ingestion = True
        self._stats = stats
        self._last_raw_chunk = last

        MutyLogger.get_instance().debug(
            "ingesting raw, chunk size=%d, plugin %s, last=%r, user_id=%s, operation_id=%s, index=%s, ws_id=%s, req_id=%s",
            len(chunk),
            self.name,
            last,
            user_id,
            operation_id,
            index,
            ws_id,
            req_id,
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)
        return GulpRequestStatus.ONGOING

    async def _enrich_documents_chunk(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        index: str = None,
        last: bool = False,
        req_id: str = None,
        q_name: str = None,
        q_group: str = None,
        **kwargs,
    ) -> list[dict]:
        """
        a GulpDocumentChunkCallback to be implemented by a plugin to enrich a chunk of documents, called by _enrich_documents_chunk_wrapper

        NOTE: implementors should process the chunk then call super()._enrich_documents_chunk to allow for stacked plugins.
        """
        if self._upper_enrich_documents_chunk_fun:
            # call upper plugin's enrich_documents_chunk
            chunk = await self._upper_enrich_documents_chunk_fun(
                sess,
                chunk,
                chunk_num=chunk_num,
                total_hits=total_hits,
                index=index,
                last=last,
                req_id=req_id,
                q_name=q_name,
                q_group=q_group,
                **kwargs,
            )
        return chunk

    async def _enrich_documents_chunk_wrapper(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        index: str = None,
        last: bool = False,
        req_id: str = None,
        q_name: str = None,
        q_group: str = None,
        **kwargs,
    ) -> list[dict]:
        """a GulpDocumentsChunk callback wrapper to enrich each chunk of documents during query"""
        cb_context = kwargs["cb_context"]
        stats: GulpRequestStats = cb_context["stats"]
        ws_id: str = cb_context["ws_id"]
        flt: GulpQueryFilter = cb_context["flt"]

        # call plugin's _enrich_documents_chunk
        chunk = await self._enrich_documents_chunk_cb(
            sess,
            chunk,
            chunk_num=chunk_num,
            total_hits=total_hits,
            index=index,
            last=last,
            req_id=req_id,
            q_name=q_name,
            q_group=q_group,
            **kwargs,
        )

        MutyLogger.get_instance().debug(
            "%s: enriched %d documents, last=%r", self.name, len(chunk), last
        )

        # update the documents on opensearch
        # also ensure no highlight field is left from the query
        for d in chunk:
            d.pop("highlight", None)

        updated, _, errors = await GulpOpenSearch.get_instance().update_documents(
            index, chunk, wait_for_refresh=last
        )
        cb_context["total_updated"] += updated
        cb_context["errors"].extend(errors)
        cb_context["total_hits"] = total_hits

        # update running stats
        await stats.update_updatedocuments_stats(
            sess,
            total_hits=total_hits,
            updated=updated,
            flt=flt,
            errors=errors,
            user_id=stats.user_id,
            ws_id=ws_id,
            last=last,
        )

        return chunk

    async def enrich_documents(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> tuple[int, int, list[str], bool]:
        """
        enriches a chunk of GulpDocuments dictionaries on-demand.

        progress is sent to the websocket, and documents are updated in the opensearch index.

        NOTE: this is guaranteed to be called by the gulp API in a worker process

        Args:
            sess (AsyncSession): The database session.
            stats (GulpRequestStats): the request stats, to be updated by the plugin during query/ingestion
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            operation_id (str): id of the operation on collab database.
            index (str): the index to query and enrich
            flt(GulpQueryFilter, optional): a filter to restrict the documents to enrich. Defaults to None.
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.
            kwargs: additional keyword arguments:
                - rq (dict): the raw query used by the engine to select the documents to enrich (will be merged with flt, if any)

        Returns:
            tuple[int, int, list[str], bool]: total hits for the query, total enriched documents (may be less than total hits if errors), list of unique errors encountered (if any), and whether the request was canceled

        NOTE: implementers of enrich_documents must:

        1. in enrich_documents, call self._initialize() and then super().enrich_documents, possibly providing "rq" in kwargs to further filter the documents to enrich.
        2. implement _enrich_documents_chunk to process each chunk of documents and enrich them.
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment" % (self.name)
            )

        # initialize these in plugin
        self._user_id = user_id
        self._stats = stats
        self._req_id = req_id
        self._ws_id = ws_id
        self._sess = sess
        self._operation_id = operation_id
        self._index = index

        # check if the caller provided a raw query to be used
        rq = kwargs.get("rq", None)
        if not rq:
            raise ValueError(
                "enrich_documents: raw query missing, 'rq' must be provided by plugin to core via kwargs"
            )

        q: dict = {}
        if not flt:
            flt = GulpQueryFilter()

        if rq:
            if flt.is_empty():
                # raw query provided by the caller
                q = rq
            else:
                # merge raw query and filter
                qq = flt.to_opensearch_dsl()
                q = GulpQueryHelpers.merge_queries(qq, rq)
        else:
            # no raw query, just filter
            if flt.is_empty():
                # match all query
                q = {"query": {"match_all": {}}}
            else:
                # convert filter
                q = flt.to_opensearch_dsl()

        # force return all fields
        q_options = GulpQueryParameters(fields="*")
        errors: list[str] = []
        cb_context = {
            "total_hits": 0,
            "total_updated": 0,
            "flt": flt,
            "errors": errors,
            "stats": stats,
            "ws_id": ws_id,
        }
        canceled: bool = False
        try:
            await GulpOpenSearch.get_instance().search_dsl(
                sess,
                index,
                q,
                req_id=self._req_id,
                q_options=q_options,
                callback=self._enrich_documents_chunk_wrapper,
                cb_context=cb_context,
            )
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            errors.append(str(ex))
            if isinstance(ex, RequestCanceledError):
                # flag canceled
                canceled = True

        return cb_context["total_hits"], cb_context["total_updated"], errors, canceled

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

        NOTE: implementers of enrich_single_document must:

        1. implement _enrich_documents_chunk
        2. in enrich_documents, call self._initialize() and then super().enrich_single_document
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment", self.name
            )

        self._operation_id = operation_id
        self._index = index
        # await self._initialize(plugin_params=plugin_params)

        # get the document and call the plugin to enrich
        doc = await GulpOpenSearch.get_instance().query_single_document(index, doc_id)
        docs = await self._enrich_documents_chunk(None, [doc])  # sess not used here

        # MutyLogger.get_instance().debug("docs=%s" % (docs))
        if not docs:
            raise ValueError(
                "document %s found but not enriched (check plugin %s)!",
                doc_id,
                self.name,
            )

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

        NOTE: implementers must call super().ingest_file first
        NOTE: this is guaranteed to be called by the engine inside a try/except which reraises the generated exception.
        NOTE: this is guaranteed to be called by the engine in a worker process (unless preview_mode is set in plugin_params)

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
            **kwargs: additional keyword arguments
        Returns:
            GulpRequestStatus: The status of the ingestion.

        Raises:
            any exception encountered during ingestion

        """
        self._sess = sess
        self._stats = stats
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._operation_id = operation_id
        self._context_id = context_id
        self._index = index
        self._file_path = file_path
        self._original_file_path = original_file_path
        self._source_id = source_id

        if GulpConfig.get_instance().ingestion_documents_adaptive_chunk_size():
            # calculate adaptive chunk size based on file size
            file_size: int = await muty.file.get_size(file_path)
            file_size_mb: int = file_size / (1024 * 1024)
            if file_size_mb < 10:
                self._adaptive_chunk_size = 500
            elif file_size_mb < 100:
                self._adaptive_chunk_size = 1000
            elif file_size_mb < 500:
                self._adaptive_chunk_size = 1500
            else:
                self._adaptive_chunk_size = 2000

        # initialize
        await self._initialize(plugin_params=plugin_params)
        if self._plugin_params.preview_mode:
            self._preview_mode = True

        MutyLogger.get_instance().debug(
            f"ingesting file source_id={source_id}, file_path={file_path}, original_file_path={original_file_path}, plugin {self.name}, user_id={user_id}, operation_id={operation_id}, \
                plugin_params={self._plugin_params}, flt={flt}, context_id={context_id}, index={index}, ws_id={ws_id}, req_id={req_id}, preview_mode={self._preview_mode}"
        )

        # add mapping parameters to source
        await self._update_source_mapping_parameters()
        return GulpRequestStatus.ONGOING

    async def load_plugin_direct(
        self,
        plugin: str,
        sess: AsyncSession,
        stats: GulpRequestStats,
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
            sess (AsyncSession, optional): The database session.
            stats (GulpRequestStats, optional): The ingestion stats.
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
        lower._index = index
        lower._file_path = file_path
        lower._original_file_path = original_file_path
        lower._source_id = source_id
        await lower._initialize(plugin_params)
        lower._preview_mode = self._preview_mode
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
        Raise:
            ValueError: if the plugin types do not match or are not INGESTION or ENRICHMENT
        Returns:
            PluginBase: the loaded plugin
        """
        lower = await GulpPluginBase.load(
            plugin, extension=False, cache_mode=cache_mode, *args, **kwargs
        )
        if (
            self.type() != lower.type()
            or self.type()
            not in [
                GulpPluginType.INGESTION,
                GulpPluginType.ENRICHMENT,
                GulpPluginType.EXTERNAL,
            ]
            or lower.type()
            not in [
                GulpPluginType.INGESTION,
                GulpPluginType.ENRICHMENT,
                GulpPluginType.EXTERNAL,
            ]
        ):
            await lower.unload()
            raise ValueError(
                "cannot stack plugin %s (type %s) over %s (type %s), types must match and be INGESTION, ENRICHMENT or EXTERNAL"
                % (self.name, self.type(), lower.name, lower.type())
            )
        # set upper plugin functions in lower, so it can call them after processing data itself
        lower._upper_record_to_gulp_document_fun = self._record_to_gulp_document
        lower._upper_enrich_documents_chunk_fun = self._enrich_documents_chunk
        lower._upper_instance = self
        self._lower_instance = lower

        # set this plugin as stacked
        self._stacked = True

        # set mapping in lower plugin
        lower._mapping_id = self._mapping_id
        lower._mappings = self._mappings
        MutyLogger.get_instance().debug(
            "---> plugin %s stacked over %s", self.name, lower.name
        )
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
        # MutyLogger.get_instance().debug(orjson.dumps("base doc:\n%s" % (base_doc_dump), option=orjson.OPT_INDENT_2).decode())
        extra_docs = []
        for extra_fields in self._extra_docs:
            # MutyLogger.get_instance().debug("creating new doc with %s\nand\n%s" % (orjson.dumps(extra_fields, option=orjson.OPT_INDENT_2), orjson.dumps(base_doc_dump, option=orjson.OPT_INDENT_2).decode()))

            new_doc_data = {**base_doc_dump, **extra_fields}

            # also add link to the base document
            new_doc_data["gulp.base_document_id"] = doc.id

            # default event code must be ignored for the extra document (since the extra document, by design, has a different event code)
            new_doc = GulpDocument(
                self,
                **new_doc_data,
                __ignore_default_event_code__=True,
            )
            # MutyLogger.get_instance().debug(
            #     "creating new doc with base=\n%s\ndata=\n%s\nnew_doc=%s"
            #     % (
            #         orjson.dumps(base_doc_dump, option=orjson.OPT_INDENT_2).decode(),
            #         orjson.dumps(new_doc_data, option=orjson.OPT_INDENT_2).decode(),
            #         orjson.dumps(new_doc.model_dump(), option=orjson.OPT_INDENT_2).decode(),
            #     )
            # )
            extra_docs.append(new_doc.model_dump(by_alias=True))

        return [doc.model_dump(by_alias=True)] + extra_docs

    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument | dict:
        """
        to be implemented in a plugin to convert a record to a GulpDocument.

        parses a record and calls _process_key for each key/value pair, generating
        a dictionary to be merged in the final gulp document.

        NOTE: called by the engine, do not call this function directly.
            THIS FUNCTION MUST NOT GENERATE EXCEPTIONS, on failure it should return None to indicate a malformed record.

        Args:
            record (any): the record to convert.
                NOTE: in a stacked plugin, this method always receives `record` as a `dict` (GulpDocument.model_dump()) and MUST return a new or modified `dict` after processing.
            record_idx (int): the index of the record in the source
            kwargs: additional keyword arguments
        Returns:
            GulpDocument|dict: the GulpDocument or dict (in case of a stacked plugin), or None to skip processing (i.e. plugin detected malformed record)

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

        # walk generated documents and ensure they have context_id and source_id set
        mapping: GulpMapping = self.selected_mapping()
        # MutyLogger.get_instance().debug("using mapping: %s", mapping)
        default_src = mapping.default_source
        default_ctx = mapping.default_context
        dd: list[dict] = []
        for d in docs:
            gulp_context_id: str = d.get("gulp.context_id", None)
            gulp_source_id: str = d.get("gulp.source_id", None)

            if not gulp_context_id and default_ctx:
                # create context_id from default context
                if self._preview_mode:
                    d["gulp.context_id"] = "preview"
                else:
                    d["gulp.context_id"] = await self._context_id_from_doc_value(
                        "gulp.context_id", default_ctx
                    )
            if not gulp_source_id and default_src:
                # create source_id from default source
                context_id = d.get("gulp.context_id", None)
                if self._preview_mode:
                    d["gulp.source_id"] = "preview"
                else:
                    d["gulp.source_id"] = await self._source_id_from_doc_value(
                        context_id, "gulp.source_id", default_src
                    )

            # check if we have both context_id and source_id set
            gulp_context_id: str = d.get("gulp.context_id", None)
            gulp_source_id: str = d.get("gulp.source_id", None)
            if gulp_context_id and gulp_source_id:
                # we have both context_id and source_id, we can proceed
                dd.append(d)
            else:
                # we cannot process this document, skip it
                MutyLogger.get_instance().warning(
                    f"document {d} does not have context_id or source_id set, skipping"
                )
        return dd

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
        field_mapping: GulpMappingField,
        d: dict,
        source_key: str,
        source_value: Any,
        skip_unmapped: bool = False,
        **kwargs,
    ) -> tuple[dict, list[str]]:
        """
        tries to map a source key to an ECS key.

        Args:
            field_mapping (GulpMappingField): mapping for the key (fields.mappings.mapping_id[source_key])
            d (dict): The dict to be updated with the mapped key/value/s
            source_key (str): The source key to map.
            source_value (any): The value to set in the mapped key
            skip_unmapped (bool): whether to skip unmapped keys, defaults to False(=if mapping not found for source_key, set it as unmapped in the resulting dict).
            **kwargs: additional keyword arguments
                - force_type (str, optional): force the type of the mapped key, defaults to None.
        Returns:
            dict, list[str]: a tuple containing the updated dict and a list of the new keys set in the dict (all derived from source_key).
        """

        # print(field_mapping)
        mapping = field_mapping.ecs
        if isinstance(mapping, str):
            # single mapping
            mapping = [mapping]

        set_keys: list[str] = []
        force_type: str = kwargs.get("force_type", None)
        if mapping:
            # source key is mapped, add the mapped key to the document
            for k in mapping:
                kk, vv = self._type_checks(k, source_value, force_type_set=force_type is not None)
                if vv:
                    d[kk] = vv
                    set_keys.append(kk)
        else:
            # unmapped key
            if not skip_unmapped:
                kk = GulpPluginBase.build_unmapped_key(source_key)
                d[kk] = source_value
                set_keys.append(kk)

        return d, set_keys

    async def _check_doc_for_ctx_id(
        self, doc: dict, mapping: GulpMapping, **kwargs
    ) -> str:
        """
        check if the context id has been already set in the document during parsing.
        if not, it will try to find it by walking the mapping fields and calling _process_key on the key set as context (is_gulp_type=context_id or context_name).

        Args:
            doc (dict): The document to check.
            mapping (GulpMapping): The mapping to use to find the context id in the document
            **kwargs: additional keyword arguments
        Returns:
            str: The context id if found, otherwise None.
        """
        context_key: str = "gulp.context_id"
        ctx_id: str = doc.get(context_key, None)
        if not ctx_id:
            # not set yet, try to find it in the document
            for k, v in mapping.fields.items():
                # walk mapping and get the field set as context, as we need the context_id
                if v.is_gulp_type and v.is_gulp_type in ["context_name", "context_id"]:
                    d: dict = await self._process_key(
                        k, doc.get(k, None), doc, **kwargs
                    )
                    ctx_id: str = d.get(context_key)
                    if not ctx_id:
                        # not found
                        return None
        return ctx_id

    def _apply_value_aliases(self, mapped_key: str, d: dict, value_aliases: dict[str, dict[str, dict]]) -> dict:
        """
        apply value aliases after mapping        
        """
        
        """
        "value_aliases": {
            # example value_aliases dictionary
            {
                // source_key
                "network.direction": {
                    // this is the default
                    "default": {
                        "2": "outbound",
                        "1": "inbound"
                    },
                    // some special cases
                    "special_case": {
                        "2": "abcd",
                        "1": "efgh"
                    }
                }
            }
        """
        # MutyLogger.get_instance().debug("applying value aliases for key %s on dict %s", mapped_key, d)
        if not value_aliases or not mapped_key in value_aliases:
            return d

        r_type: str = 'default'
        if self._record_type:
            r_type = self._record_type
        aliases: dict[str, dict] = value_aliases[mapped_key]
        if r_type in aliases:
            alias_map: dict = aliases[r_type]
        else:
            alias_map: dict = aliases.get('default', {})

        # MutyLogger.get_instance().debug("r_type=%s, selected alias map: %s", r_type, alias_map)
        if not alias_map:
            return d
        
        # apply
        for k, v in d.items():
            if str(v) in alias_map:
                d[k] = alias_map[str(v)]
        # MutyLogger.get_instance().debug("r_type=%s, successfully mapped dict=%s", r_type, d)
        return d
        
    async def _process_key(
        self, source_key: str, source_value: Any, doc: dict, **kwargs
    ) -> dict:
        """
        Maps the source key, generating a dictionary to be merged in the final gulp document.

        also updates self._extra_docs with dictionaries indicating extra documents to be generated,
        to be post-processed in _finalize_process_record().

        Args:
            source_key (str): The source key, in doc,  to map.
            source_value (any): value of doc[source_key] in doc
            doc (dict): The whole document **BEFORE** any mapping applied: this is the unmodified record coming from the source, as a dict
            **kwargs: additional keyword arguments
        Returns:
            a dictionary to be merged in the final gulp document
        """

        if not source_value:
            return {}

        # check if we have a mapping for source_key
        mapping = self.selected_mapping()
        if mapping.exclude and source_key in mapping.exclude:
            # ignore this key
            return {}
        if mapping.include and source_key not in mapping.include:
            # ignore this key
            return {}

        # to replace with aliases AFTER mapping        
        value_aliases: dict = mapping.value_aliases or {}

        # get field mapping from mappings.mapping_id.fields[source_key] (i.e. { "mappings": { "windows": { "fields": { "AccountDomain": { ... } } } } })
        field_mapping: GulpMappingField = mapping.fields.get(source_key)
        if not field_mapping:
            # missing mapping at all (no ecs and no timestamp field)
            d = {GulpPluginBase.build_unmapped_key(source_key): source_value}
            if value_aliases:
                # apply aliases
                self._apply_value_aliases(GulpPluginBase.build_unmapped_key(source_key), d, value_aliases)
            return d

        # determine if this is a timestamp for an extra doc and determine timestamp type, if needed
        is_extra_doc_timestamp: bool = (
            field_mapping.extra_doc_with_event_code and not field_mapping.is_timestamp
        )
        is_timestamp: str = field_mapping.is_timestamp
        if is_extra_doc_timestamp:
            is_timestamp = "generic"

        # print(
        #     "processing key:",
        #     source_key,
        #     "value:",
        #     source_value,
        #     "field_mapping:",
        #     field_mapping,
        #     "\n",
        # )

        gulp_type: str = field_mapping.is_gulp_type
        if gulp_type:
            d: dict={}
            force_type: str = "str" # always string
            if gulp_type in ["context_name", "context_id"]:
                # this is a gulp context field
                if self._preview_mode:
                    ctx_id = "preview"
                else:
                    if gulp_type == "context_name":
                        # get or create the context
                        ctx_id: str = await self._context_id_from_doc_value(
                            source_key, source_value
                        )
                    else:
                        # directly use the value as context_id, and ensure it exists
                        ctx_id: str = await self._context_id_from_doc_value(
                            source_key, source_value, force_v_as_context_id=True
                        )

                # also map the value if there's ecs set
                m, _ = self._try_map_ecs(
                    field_mapping,
                    d,
                    source_key,
                    str(source_value),
                    skip_unmapped=True,
                    force_type=force_type,
                )
                m["gulp.context_id"] = ctx_id

                # mapping value aliases not needed here
                return m
            elif gulp_type in ["source_id", "source_name"]:
                # this is a gulp source field
                if self._preview_mode:
                    ctx_id = "preview"
                    src_id = "preview"
                else:
                    # find the context
                    ctx_id: str = await self._check_doc_for_ctx_id(
                        doc, mapping, **kwargs
                    )
                    if not ctx_id:
                        # no context_id, cannot process source
                        MutyLogger.get_instance().error(
                            f"cannot set source {source_key} without context"
                        )
                        return {}

                    if gulp_type == "source_name":
                        # get/create the source
                        src_id: str = await self._source_id_from_doc_value(
                            ctx_id, source_key, source_value
                        )
                    else:
                        # directly use the value as source_id
                        src_id: str = await self._source_id_from_doc_value(
                            ctx_id, source_key, source_value, force_v_as_source_id=True
                        )

                if not src_id:
                    # cannot proceed without source_id
                    return {}

                # also map the value if there's ecs set
                m, _ = self._try_map_ecs(
                    field_mapping,
                    d,
                    source_key,
                    str(source_value),
                    skip_unmapped=True,
                    force_type=force_type,
                )
                m["gulp.source_id"] = src_id
                m["gulp.context_id"] = ctx_id
                if [ctx_id, src_id] not in self._ctx_src_pairs:
                    # add to the unique pairs, will be used when computing source_field_types
                    self._ctx_src_pairs.append([ctx_id, src_id])

                # mapping value aliases not needed here
                return m

            # mapping value aliases not needed here
            return m

        # d will accumulate values
        d: dict = {}
        if field_mapping.flatten_json:
            # this key value is a json that must be flattened, i.e. {"a": {"b": 1}} -> {"a.b": 1}
            if isinstance(source_value, str):
                js: dict = orjson.loads(source_value)
            else:
                js: dict = source_value
            flattened: dict = muty.dict.flatten(js, prefix="%s." % (source_key))
            for k, v in flattened.items():
                processed: dict = await self._process_key(k, v, doc, **kwargs)
                if value_aliases:
                    # apply aliases
                    for kk, _ in processed.items():
                        self._apply_value_aliases(kk, processed, value_aliases)    
                d.update(processed)
            return d
        
        force_type: str = field_mapping.force_type
        if force_type:
            # force value to the given type
            t = force_type
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
            field_mapping.multiplier
            and isinstance(source_value, int)
            and field_mapping.multiplier > 1
        ):
            # apply multiplier
            source_value = int(source_value * field_mapping.multiplier)

        if is_timestamp:
            # this field represents a timestamp to be handled
            # NOTE: a field mapped as "@timestamp" (AND ONLY THAT) needs `is_timestamp` set ONLY IF its type is different than `generic` (i.e. if it is a `chrome` or `windows_filetime` timestamp)
            # this is because `@timestamp` is a special field handled directly by the engine.
            if is_timestamp == "chrome":
                # timestamp chrome, turn to nanoseconds from epoch. do not apply offset here, yet
                source_value = muty.time.chrome_epoch_to_nanos_from_unix_epoch(
                    int(source_value)
                )

                force_type = "int"
            elif is_timestamp == "windows_filetime":
                # timestamp windows filetime, turn to nanoseconds from epoch. do not apply offset here, yet
                source_value = muty.time.windows_filetime_to_nanos_from_unix_epoch(
                    int(source_value)
                )
                force_type = "int"
            elif is_timestamp == "generic":
                # this is a generic timestamp, turn it into a string and nanoseconds. no offset applied here, yet
                _, ns, _ = GulpDocument.ensure_timestamp(str(source_value))
                source_value = ns
                force_type = "int"
            else:
                # not supported
                MutyLogger.get_instance().warning(
                    f"timestamp type {field_mapping.is_timestamp} not supported for key {source_key}, keeping as is..."
                )
                return {}

        if field_mapping.extra_doc_with_event_code:
            # this will trigger the creation of an extra document
            # with the given event code in _finalize_process_record()
            extra: dict = {
                "event_code": str(field_mapping.extra_doc_with_event_code),
                "timestamp": source_value,
            }

            # remove field/s corresponding to this key in the generated extra document
            d, _ = self._try_map_ecs(field_mapping, d, source_key, source_value)
            for k, _ in d.items():
                extra[k] = None
            self._extra_docs.append(extra)

            if value_aliases:
                # apply aliases
                for kk, _ in processed.items():
                    self._apply_value_aliases(kk, processed, value_aliases)    

            # we also add this key to the main document
            return d

        # map to the "ecs" value
        m, _ = self._try_map_ecs(
            field_mapping, d, source_key, source_value, force_type=force_type
        )
        if value_aliases:
            # apply value aliases if any
            for kk, _ in m.items():
                self._apply_value_aliases(kk, m, value_aliases)    
        return m

    async def _flush_and_check_thresholds(
        self,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> None:
        """
        flushes buffer and checks failure thresholds.

        Args:
            flt (GulpIngestionFilter, optional): filter to apply during ingestion
            wait_for_refresh (bool, optional): whether to wait for refresh
            kwargs: additional keyword arguments
        """
        MutyLogger.get_instance().debug(
            "_flush_and_check_thresholds called, plugin=%s", self.name
        )
        # flush buffer
        skipped, ingested, failed = await self.flush_buffer_and_send_to_ws(
            flt, wait_for_refresh
        )

        # check if request was canceled
        if self._req_canceled:
            raise RequestCanceledError(f"request {self._req_id} canceled!")

        # check failure thresholds
        failure_threshold = GulpConfig.get_instance().ingestion_evt_failure_threshold()
        if not self._raw_ingestion and failure_threshold > 0 and (
            self._records_skipped_total >= failure_threshold
            or self._records_failed_total >= failure_threshold
        ):
            raise SourceCanceledError(
                f"ingestion per-source failure threshold reached (tot_skipped={self._records_skipped_total}, "
                f"tot_failed={self._records_failed_total}, threshold={failure_threshold}), "
                f"canceling source..."
            )

        # update stats
        await self._stats.update_ingestion_stats(
            self._sess,
            user_id=self._user_id,
            ws_id=self._ws_id,
            records_ingested=ingested,
            records_skipped=skipped,
            records_processed=self._records_processed_per_chunk,
            records_failed=self._records_failed_per_chunk,
        )
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
    ) -> bool:
        """
        Processes a single record by converting it to one or more documents.

        the document is then sent to the configured websocket and, if enabled, ingested in the configured opensearch index.

        if the record fails processing, it is counted as a failure, the failure counter is updated, and this function
        returns without further processing and without throwing exceptions (returns True to indicate processing should continue).

        in preview mode, documents are accumulated in self._preview_chunk until the configured number of documents is reached,
        at which point process_record returns False to indicate that no further records should be processed.

        NOTE: this function shouldn't generate exception unless the request is canceled or the source must be canceled due to failure threshold.

                Args:
            record (any): The record to process.
            record_idx (int): The index of the record.
            flt (GulpIngestionFilter, optional): The filter to apply during ingestion. Defaults to None.
            wait_for_refresh (bool, optional): Whether to wait for a refresh after ingestion. Defaults to False.
            kwargs: additional keyword arguments, they will be passed to plugin's `_record_to_gulp_documennt`

        Returns:
            bool: True if processing should continue, False if it should stop (i.e. in preview mode and enough documents have been accumulated).
        Raises:
            SourceCanceledError: if the per-source failure threshold is reached.
            RequestCanceledError: if the request is canceled.
        """
        if self._external_query:
            # external query, documents have been already filtered by the query
            flt = None

        # get ingestion chunk size, either adaptive or fixed
        ingestion_buffer_size: int = (
            self._adaptive_chunk_size
            if self._adaptive_chunk_size
            else GulpConfig.get_instance().ingestion_documents_chunk_size()
        )
        if self._plugin_params.override_chunk_size:
            # override
            ingestion_buffer_size = self._plugin_params.override_chunk_size

        self._extra_docs = []

        # process this record and generate one or more gulpdocument dictionaries
        try:
            docs = await self._record_to_gulp_documents_wrapper(
                record, record_idx, **kwargs
            )
        except Exception as ex:
            # increment records failed counters
            self._records_failed_per_chunk += 1
            self._records_failed_total += 1
            MutyLogger.get_instance().exception(ex)

            # continue processing anyway
            return True

        self._records_processed_per_chunk += 1
        self._records_processed_total += 1

        if self._preview_mode:
            # MutyLogger.get_instance().debug("***PREVIEW MODE*** got %d docs", len(docs))
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
                MutyLogger.get_instance().warning(
                    "***PREVIEW MODE*** reached %d documents, stopping!",
                    len(self._preview_chunk),
                )
                return False
            # continue accumulating
            return True

        # add documents to buffer and check if we need to flush
        # MutyLogger.get_instance().debug("adding %d docs to buffer", len(docs))
        for d in docs:
            self._docs_buffer.append(d)
            if len(self._docs_buffer) >= ingestion_buffer_size:
                # flush
                await self._flush_and_check_thresholds(flt, wait_for_refresh)
        return True

    async def _parse_custom_parameters(self) -> None:
        """
        parse the defined GulpPluginCustomParameters and store the key/value pairs in self._plugin_params.custom_parameters.

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

    def _ensure_plugin_params(
        self,
        plugin_params: GulpPluginParameters = None,
        mapping_file: str = None,
        mappings: dict[str, GulpMapping] = None,
        mapping_id: str = None,
        additional_mapping_files: list[tuple[str, str]] = None,
        additional_mappings: dict[str, GulpMapping] = None,
    ) -> GulpPluginParameters:
        """
        ensure plugin_params is not None and optionally set mapping parameters if not already set.

        Args:
            plugin_params (GulpPluginParameters, optional): the plugin parameters. If None, a default one will be created.
            mapping_file (str, optional): the mapping file to use if plugin_params.mapping_parameters is empty.
            mappings (dict[str, GulpMapping], optional): the mappings to use if plugin_params.mapping_parameters is empty.
            mapping_id (str, optional): the mapping id to use if plugin_params.mapping_parameters is empty.
            additional_mapping_files (list[tuple[str,str]], optional): additional mapping files to use if plugin_params.mapping_parameters is empty.
            additional_mappings (dict[str, GulpMapping], optional): additional mappings to use if plugin_params.mapping_parameters is empty.
        Returns:
            GulpPluginParameters: the ensured plugin parameters.
        """
        if not plugin_params:
            plugin_params = GulpPluginParameters()

        if plugin_params.mapping_parameters.is_empty():
            plugin_params.mapping_parameters = GulpMappingParameters(
                mapping_file=mapping_file,
                mapping_id=mapping_id,
                mappings=mappings,
                additional_mapping_files=additional_mapping_files,
                additional_mappings=additional_mappings,
            )
        else:
            # apply overrides
            if mapping_file:
                plugin_params.mapping_parameters.mapping_file = mapping_file
            if mapping_id:
                plugin_params.mapping_parameters.mapping_id = mapping_id
            if mappings:
                plugin_params.mapping_parameters.mappings = mappings
            if additional_mapping_files:
                plugin_params.mapping_parameters.additional_mapping_files = (
                    additional_mapping_files
                )
            if additional_mappings:
                plugin_params.mapping_parameters.additional_mappings = (
                    additional_mappings
                )

        return plugin_params

    @staticmethod
    async def mapping_parameters_to_mapping(
        mapping_parameters: GulpMappingParameters = None,
    ) -> tuple[dict[str, GulpMapping], str]:
        """
        get each defined mapping, handling loading from file if needed, and merging additional mappings if specified.

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
            and not mapping_parameters.additional_mappings
            and mapping_parameters.mapping_id
        ):
            raise ValueError(
                "mapping_id is set but mappings/mapping_file/additional_mapping_files/additional_mappings are not!"
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
            # MutyLogger.get_instance().debug(
            #     f"using plugin_params.mapping_parameters.mapping_file={mapping_file}"
            # )

            mapping_file_path = GulpConfig.get_instance().build_mapping_file_path(
                mapping_file
            )
            file_content = await muty.file.read_file_async(mapping_file_path)
            mapping_data = orjson.loads(file_content)

            if not mapping_data:
                raise ValueError(f"mapping file {mapping_file_path} is empty!")

            mapping_file_obj = GulpMappingFile.model_validate(mapping_data)
            mappings = mapping_file_obj.mappings

        # validation checks
        if not mappings and not mapping_parameters.mapping_id:
            # empty mapping will be used
            MutyLogger.get_instance().warning(
                "mappings/mapping_file and mapping_id are both None/empty!"
            )
            mappings = {"default": GulpMapping(fields={})}

        # ensure mapping_id is set to first key if not specified
        mapping_id = mapping_parameters.mapping_id or list(mappings.keys())[0]
        # MutyLogger.get_instance().debug(f"mapping_id={mapping_id}")

        # if we have specified direct mapping alone, just stop here and use it
        if mapping_parameters.mappings or (
            not mapping_parameters.additional_mapping_files
            and not mapping_parameters.additional_mappings
        ):
            return mappings, mapping_id

        # we may have additional mapping specified in mapping_parameters.additional_mapping_files and/or
        # mapping_parameters.additional_mappings. so, merge them
        if mapping_parameters.additional_mapping_files:
            MutyLogger.get_instance().debug(
                f"loading additional mapping files/id: {mapping_parameters.additional_mapping_files} ..."
            )

            for file_info in mapping_parameters.additional_mapping_files:
                # load and merge additional mappings from files
                additional_file_path = (
                    GulpConfig.get_instance().build_mapping_file_path(file_info[0])
                )
                additional_mapping_id = file_info[1]

                file_content = await muty.file.read_file_async(additional_file_path)
                mapping_data = orjson.loads(file_content)

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

        if mapping_parameters.additional_mappings:
            MutyLogger.get_instance().debug(
                f"loading additional mappings: {mapping_parameters.additional_mappings} ..."
            )

            for (
                additional_mapping_id,
                additional_mapping,
            ) in mapping_parameters.additional_mappings.items():
                # merge additional mappings
                main_mapping = mappings.get(mapping_id, GulpMapping())
                add_mapping = GulpMapping.model_validate(additional_mapping)

                MutyLogger.get_instance().debug(
                    f"adding additional mappings from {additional_mapping_id} to '{mapping_id}' ..."
                )
                for key, value in add_mapping.fields.items():
                    main_mapping.fields[key] = value

                mappings[mapping_id] = main_mapping

        return mappings, mapping_id

    async def _fetch_index_type_mappings(self) -> None:
        """
        get the type mappings for the current index on OpenSearch
        """
        if not self._index_type_mapping:
            self._index_type_mapping = (
                await GulpOpenSearch.get_instance().datastream_get_field_types(
                    self._index
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
            "---> _initialize: plugin=%s, stacked=%r, plugin_params=%s",
            self.filename,
            self._stacked,
            orjson.dumps(
                self._plugin_params.model_dump(), option=orjson.OPT_INDENT_2
            ).decode(),
        )

        # parse the custom parameters
        await self._parse_custom_parameters()
        pt: GulpPluginType = self.type()
        if pt in [GulpPluginType.EXTENSION, GulpPluginType.ENRICHMENT]:
            MutyLogger.get_instance().debug(
                "plugin %s type=%s, no mappings needed", self.name, pt
            )
            return

        # setup mappings
        # in a lower stacked plugin this is already set by the upper with setup_stacked_plugin()
        if not self._mappings:
            self._mappings, self._mapping_id = (
                await GulpPluginBase.mapping_parameters_to_mapping(
                    self._plugin_params.mapping_parameters
                )
            )

        # initialize index type mappings
        await self._fetch_index_type_mappings()

        # MutyLogger.get_instance().debug(
        #     "---> finished _initialize: plugin=%s, mapping_id=%s, mappings=%s"
        #     % (
        #         self.filename,
        #         self._mapping_id,
        #         self._mappings,
        #     )
        # )

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

    async def _update_source_mapping_parameters(self) -> None:
        """
        add mapping parameters to source, to keep track of which mappings has been used for this source
        """
        if self._plugin_params.mapping_parameters and not self._preview_mode:
            from gulp.api.opensearch.sigma import get_sigma_mappings

            sm = await get_sigma_mappings(self._plugin_params.mapping_parameters)
            self._plugin_params.mapping_parameters.sigma_mappings = sm

            n: GulpSource = await GulpSource.get_by_id(
                self._sess, self._source_id, throw_if_not_found=False
            )
            if not n:
                MutyLogger.get_instance().error(
                    f"cannot find source {self._source_id} to update mapping_parameters"
                )
                return
            await n.update(
                self._sess,
                plugin=self.name,
                mapping_parameters=self._plugin_params.mapping_parameters.model_dump(
                    exclude_none=True
                ),
            )

    async def update_final_stats_and_flush(
        self, flt: GulpIngestionFilter = None, ex: Exception = None
    ) -> GulpRequestStatus:
        """
        to be called to finish ingestion on a source to finalize stats and flush the internal buffer

        for raw ingestion, the source is never set to DONE unless it's the last chunk or the request is canceled.
        for standard ingestion or query external, a WSDATA_INGEST_SOURCE_DONE packet is sent on the websocket.

        Args:
            flt (GulpIngestionFilter, optional): An optional filter to apply during ingestion. Defaults to None.
            ex (Exception, optional): An optional exception that may have caused the source to fail. Defaults to None.

        Returns:
            GulpRequestStatus: The final status of the ingestion process.
        """
        MutyLogger.get_instance().debug(
            "update_final_stats_and_flush called for plugin=%s", self.name
        )
        if not self._stats:
            MutyLogger.get_instance().error(
                "cannot update final stats and flush: stats object is None!"
            )
            return GulpRequestStatus.FAILED
        
        if self._preview_mode:
            MutyLogger.get_instance().debug("*** preview mode, no ingestion! ***")
            return GulpRequestStatus.DONE

        MutyLogger.get_instance().debug(
            "*** plugin=%s, file_path/source_id=%s, remaining docs to flush in docs_buffer: %d, status=%s ***",
            self.name,
            self._file_path or self._source_id,
            len(self._docs_buffer),
            self._stats.status,
        )
        ingested: int = 0
        skipped: int = 0
        failed: int = 0
        errors: list[str] = []
        status: GulpRequestStatus = None
        if ex:
            if isinstance(ex, SourceCanceledError):
                # also SourceCanceled counts as request canceled
                self._req_canceled = True
            else:
                errors.append(muty.log.exception_to_string(ex))

        if self._stacked:
            # we're a stacked plugin and buffering was done in the lower instance: so, use its buffer
            self._docs_buffer = self._lower_instance._docs_buffer

        try:
            # flush the last chunk
            skipped, ingested, failed = await self.flush_buffer_and_send_to_ws(
                flt,
                wait_for_refresh=True,
            )
        except Exception as e:
            MutyLogger.get_instance().exception(e)
            s: str = muty.log.exception_to_string(e)
            if s not in errors:
                errors.append(s)

        self._records_failed_per_chunk += failed
        source_finished: bool = True
        disconnected: bool = False
        if self._raw_ingestion:
            # on raw ingestion, source is never done unless last chunk or canceled
            if self._last_raw_chunk:
                status = GulpRequestStatus.DONE
            else:
                # keep the raw ingestion ongoing if its not the last chunk
                status = GulpRequestStatus.ONGOING
                source_finished = False
            if self._req_canceled:
                status = GulpRequestStatus.CANCELED
        else:
            # standard ingestion or query external, send WSDATA_INGEST_SOURCE_DONE on the ws
            if errors:
                status = GulpRequestStatus.FAILED
            else:
                status = GulpRequestStatus.DONE
            if self._req_canceled:
                status = GulpRequestStatus.CANCELED

            p: GulpIngestSourceDonePacket = GulpIngestSourceDonePacket(
                source_id=self._source_id,
                context_id=self._context_id,
                records_ingested=self._records_ingested_total,
                records_skipped=self._records_skipped_total,
                records_failed=self._records_failed_total,
                status=status.value,
            )

            disconnected: bool = False
            try:
                await GulpRedisBroker.get_instance().put(
                    WSDATA_INGEST_SOURCE_DONE,
                    self._user_id,
                    ws_id=self._ws_id,
                    operation_id=self._operation_id,
                    req_id=self._req_id,
                    d=p.model_dump(exclude_none=True),
                )
            except WebSocketDisconnect as ex:
                # may fail in case socket disconnected
                disconnected = True
                errors.append(str(ex))
                source_finished = True
                status = GulpRequestStatus.FAILED

        self._raw_flush_count += 1
        # "and not disconnected" since if the websocket is disconnected, we want to update stats
        if (
            self._raw_ingestion
            and status == GulpRequestStatus.ONGOING
            and not disconnected
        ):
            if self._raw_flush_count % 10 != 0:
                # do not update stats and source_fields too frequently on raw
                return status

        # update stats
        try:
            MutyLogger.get_instance().debug(
                "updating ingestion stats with: ingested=%d, skipped=%d, processed=%d, failed=%d, errors=%s, status=%s, source_finished=%r, ws_disconnected=%r",
                ingested,
                skipped,
                self._records_processed_per_chunk,
                self._records_failed_per_chunk,
                errors,
                status,
                source_finished,
                disconnected,
            )
            d: dict = await self._stats.update_ingestion_stats(
                self._sess,
                user_id=self._user_id,
                ws_id=self._ws_id,
                records_ingested=ingested,
                records_skipped=skipped,
                records_processed=self._records_processed_per_chunk,
                records_failed=self._records_failed_per_chunk,
                errors=errors,
                status=status,
                source_finished=source_finished,
                ws_disconnected=disconnected,
            )

            # update source field types (in background)
            from gulp.api.server_api import GulpServer

            if self._ctx_src_pairs:
                # multiple context and sources generated
                coro = GulpOpenSearch.get_instance().datastream_update_source_field_types_by_ctx_src_pairs(
                    None,  # sess=None since we're spawning a background task and cannot use the same session
                    self._index,
                    self._user_id,
                    operation_id=self._operation_id,
                    ctx_src_pairs=self._ctx_src_pairs,
                )
            else:
                # use default context_id, source_id
                coro = GulpOpenSearch.get_instance().datastream_update_source_field_types_by_src(
                    None,  # sess=None since we're spawning a background task and cannot use the same session
                    self._index,
                    self._user_id,
                    operation_id=self._operation_id,
                    context_id=self._context_id,
                    source_id=self._source_id,
                )
            bg_task_name = f"update_source_field_types_{self._operation_id}_{self._context_id}_{self._source_id}"
            GulpServer.spawn_bg_task(coro, bg_task_name)

            return d["status"]
        except:
            MutyLogger.get_instance().exception("error updating ingestion stats")
            return GulpRequestStatus.FAILED

    def register_internal_events_callback(self, types: list[str] = None) -> None:
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
        GulpInternalEventsManager.get_instance().deregister(self.name)

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
            **kwargs: Additional keyword arguments ("pickled", only when running in worker process).
        """
        # this is set in __reduce__(), which is called when the plugin is pickled(=loaded in another process)
        # pickled=True: running in worker
        # pickled=False: running in main process
        # pickled = kwargs.get("pickled", False)
        pickled = kwargs.pop("pickled", False)

        # get plugin full path by name
        path = GulpPluginBase.path_from_plugin(
            plugin, extension, raise_if_not_found=True
        )

        # try to get plugin from cache
        internal_plugin_name = os.path.splitext(os.path.basename(path))[0]
        force_load_from_disk: bool = False
        # if path.endswith(".pyc" or path.endswith(".pyc")):
        #     # compiled file, always load from disk
        #     MutyLogger.get_instance().debug(
        #         "loading compiled plugin %s from disk" % (bare_name)
        #     )
        #     cache_mode = GulpPluginCacheMode.IGNORE

        if cache_mode == GulpPluginCacheMode.IGNORE:
            # ignore cache
            MutyLogger.get_instance().debug(
                "ignoring cache for plugin %s" % (internal_plugin_name)
            )
            force_load_from_disk = True
        elif cache_mode == GulpPluginCacheMode.FORCE:
            # force cache use
            MutyLogger.get_instance().warning(
                "force cache for plugin %s" % (internal_plugin_name)
            )
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
                        "plugin cache is disabled, loading plugin %s from disk"
                        % (internal_plugin_name)
                    )
                    force_load_from_disk = True

        seed: str = muty.string.generate_unique()
        if extension:
            module_name = f"gulp.plugins.extension.{internal_plugin_name}-{seed}"
        else:
            module_name = f"gulp.plugins.{internal_plugin_name}-{seed}"

        p: GulpPluginBase = None
        if not force_load_from_disk:
            m: ModuleType = GulpPluginCache.get_instance().get(internal_plugin_name)
            if m:
                # use already loaded object from cache
                p = m.Plugin(path, module_name, pickled=pickled, **kwargs)
                sys.modules[module_name] = m

        if not p:
            # load from file and add to sys.modules
            m: ModuleType = muty.dynload.load_dynamic_module_from_file(
                module_name, path
            )
            p = m.Plugin(path, module_name, pickled=pickled, **kwargs)
        MutyLogger.get_instance().debug(
            f"LOADED plugin m={m}, p={p}, name()={p.name}, pickled={pickled}, depends_on={p.depends_on()}"
        )

        # check dependencies
        if p.depends_on():
            # check each dependency (each is a file name)
            for dep in p.depends_on():
                # check if dependency is loaded
                dep_path_noext = GulpPluginBase.path_from_plugin(
                    dep, is_extension=False, raise_if_not_found=False
                )
                dep_path_ext = GulpPluginBase.path_from_plugin(
                    dep, is_extension=True, raise_if_not_found=False
                )
                dep_path = dep_path_noext or dep_path_ext
                if not dep_path:
                    await p.unload()
                    raise FileNotFoundError(
                        f"dependency {dep} not found, plugin={internal_plugin_name} cannot load, path={path}"
                    )

        # also call post-initialization routine if any
        await p.post_init(**kwargs)

        if (
            cache_mode != GulpPluginCacheMode.IGNORE
            and GulpConfig.get_instance().plugin_cache_enabled()
        ):
            # add loaded object to the cache so next Plugin() instantiation is faster (no file load)
            GulpPluginCache.get_instance().add(m, internal_plugin_name)
        return p

    async def unload(self) -> None:
        """
        unload the plugin and remove it from sys.modules.

        - if plugin cache is enabled, this method does nothing.
        - implementers must call super().unload() at the end of their unload method.

        Returns:
            None
        """
        # clear stuff
        MutyLogger.get_instance().debug("unload() called for plugin: %s", self.name)

        # empty internal events queue
        self.deregister_internal_events_callback()
        del sys.modules[self.module_name]

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
                    # prefer path in extra path if exists
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
                    if name.lower() not in p.name.lower():
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
                if (
                    not f_lower.endswith(".ts")
                    and not f_lower.endswith(".js")
                    and not f_lower.endswith(".tsx")
                ):
                    continue

                # we have the plugin path, now build the corresponding metadata file (json) path
                metadata_file = os.path.join(path, f + ".json")
                if not await muty.file.exists_async(metadata_file):
                    MutyLogger.get_instance().warning(
                        "plugin metadata file %s not found, skipping plugin %s"
                        % (metadata_file, f)
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
                        "plugin metadata file %s is not valid, skipping plugin %s:\n%s"
                        % (metadata_file, f, js)
                    )
                    continue

                # got entry

        l: list[GulpUiPluginMetadata] = []

        # list extra folder first, if any
        p = GulpConfig.get_instance().path_plugins_extra()
        if p:
            await _list_ui_internal(l, os.path.join(p, "ui"))
            MutyLogger.get_instance().debug(
                "found %d UI plugins in extra path=%s" % (len(l), p)
            )

        # list default path
        default_path = GulpConfig.get_instance().path_plugins_default()
        await _list_ui_internal(l, os.path.join(default_path, "ui"))
        return l
