"""Gulp plugin base class and plugin utilities."""

import asyncio
import inspect
import ipaddress
import os
import sys
import time
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from types import ModuleType
from typing import Annotated, Any, Callable, Literal, Optional

import json5
from gulp.process import GulpProcess
import muty.dynload
import muty.file
import muty.log
import muty.pydantic
import muty.string
import orjson
from fastapi import WebSocketDisconnect
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, ValidationError, Field
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.context import GulpContext
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import (
    GulpRequestStats,
    GulpUpdateDocumentsStats,
    RequestCanceledError,
    RequestStatsType,
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
    GulpNormalizedTimestamp,
    GulpQueryHelpers,
    GulpQueryParameters,
)
from gulp.api.opensearch_api import GulpOpenSearch, GulpPreparedBulk
from gulp.api.prometheus_api import GulpMetrics
from gulp.api.s3_api import GulpS3
from gulp.api.ws_api import (
    WSDATA_DOCUMENTS_CHUNK,
    WSDATA_INGEST_RAW_PROGRESS,
    WSDATA_INGEST_SOURCE_DONE,
    GulpDocumentsChunkPacket,
    GulpIngestRawProgress,
    GulpIngestSourceDonePacket,
    GulpQueryDonePacket,
    GulpRedisBroker,
)
from gulp.config import GulpConfig
from gulp.events import (
    GulpChunkPrePostIngestEventData,
    GulpCoreEvent,
    GulpIngestSourceDoneEventData,
    events,
)
from gulp.api.mapping.mapping_utils import (
    apply_value_aliases,
    build_extra_doc_fields,
    convert_special_timestamp,
    expand_extra_docs,
    flatten_json_value,
    mapping_parameters_to_mapping,
    transform_scalar,
)
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


@dataclass(slots=True)
class _GulpIngestionChunk:
    """Detached logical chunk owned by the ingestion coordinator."""

    documents: list[dict]
    records_processed: int
    records_failed: int
    flt: GulpIngestionFilter | None
    sequence: int = 0
    all_fields_on_ws: bool = False
    prepared: GulpPreparedBulk | None = None
    bulk_task: asyncio.Task[tuple[int, list[dict], int]] | None = None
    totals_committed: bool = False


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


class InternalDocValueCache:
    """
    a cache for specific document values, local to the plugin (more specifically, to the process running the plugin).
    this is to be used to avoid recalculating same value every time for the same input (i.e. hashes, derived values, ...)
    """

    def __init__(self, cache_size: int = 1000):
        self._cache: dict[str, Any] = {}
        self._max_size: int = cache_size

    def set_max_size(self, max_size: int) -> None:
        """
        set the maximum cache size (after which the cache is reset)

        Args:
            max_size (int): the maximum cache size
        """
        self._max_size = max_size

    def get_value(self, k: str) -> Any | None:
        """
        get a value from the cache
        Args:
            k (str): the key
        Returns:
            Any|None: the value if found, None otherwise
        """
        return self._cache.get(k, None)

    def set_value(self, k: str, v: Any) -> None:
        """
        set a value in the cache

        Args:
            k (str): the key
            v (Any): the value
        """
        if len(self._cache) > self._max_size:
            # reset cache
            self._cache = {}
        self._cache[k] = v


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

        NOTE: a class extending GulpPluginBase should calls super().__init__() in its __init__ before anything else.

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
        # fallback reused when the selected mapping is unavailable
        self._empty_mapping: GulpMapping = GulpMapping()
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
        # S3 object ID for the current stored-file ingestion.
        self._stored_file_id: str | None = None
        self._adaptive_chunk_size: int = 0
        self._adaptive_chunk_enabled: bool = False
        self._chunk_build_started: float | None = None
        # One last normalized value per stable timestamp stream. Most plugins
        # use only the implicit "primary" stream; named streams keep parsers
        # with multiple timestamp fields from evicting one another.
        self._normalized_timestamp_cache: dict[
            str, tuple[tuple[Any, ...], GulpNormalizedTimestamp]
        ] = {}

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
        # Stable reference used after a nested lower plugin has been unloaded
        # and its lifecycle link has been detached.
        self._ingestion_owner: GulpPluginBase | None = None

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

        # Chunk results awaiting the next collaboration-statistics update.
        self._stats_pending_processed: int = 0
        self._stats_pending_failed: int = 0
        self._stats_pending_skipped: int = 0
        self._stats_pending_ingested: int = 0

        # A file ingestion may own one OpenSearch-only task while parsing the
        # following chunk. No SQLAlchemy session is used inside this task.
        self._pipeline_enabled: bool = False
        self._pipeline_active_chunk: _GulpIngestionChunk | None = None
        self._chunk_sequence_next: int = 0

        # to keep track of ingested chunks
        self._chunks_ingested: int = 0

        # corresponding request is canceled and we should stop processing
        self._req_canceled: bool = False

        self._plugin_params: GulpPluginParameters = GulpPluginParameters()

        self._operation: GulpOperation | None = None

        # for preview mode
        self._preview_mode: bool = False
        self._preview_chunk: list[dict] = []

        # indicates the last chunk in a raw ingestion
        self._last_raw_chunk: bool = False
        self._raw_flush_count: int = 0

        # Chunk listener state is fixed for one source.
        self._event_listener_mask: int = 0
        self._source_finished_event_emitted: bool = False
        self._chunk_pre_ingest_failed: bool = False

        # mantain a plugin local cache to avoid precomputing the same values every time.
        # core uses it for context_id, source_id, gulp.event_code, plugin can use it while processing records.
        self.doc_value_cache: InternalDocValueCache = InternalDocValueCache()

        # to keep track of upload and delete file later in _ingest_file_internal
        self.upload_task: asyncio.Task | None = None

        # just a shortcut for selected_mapping().unmapped_as_is
        self._unmapped_as_is: bool = False

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
        return GulpProcess.get_instance().is_main_process()

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

    async def post_init(self, **kwargs):
        """
        this is called after the plugin has been initialized, to allow for any post-initialization tasks.

        Args:
            kwargs: additional arguments to pass
        """
        return

    async def _emit_source_finished_event(
        self, status: GulpRequestStatus, errors: list[str]
    ) -> None:
        """Emit one best-effort terminal event for the current source."""
        if self._source_finished_event_emitted:
            return
        self._source_finished_event_emitted = True
        ev = GulpIngestSourceDoneEventData(
            plugin=self.name,
            user_id=self._user_id,
            operation_id=self._operation_id,
            req_id=self._req_id,
            file_path=(
                self._original_file_path
                if self._original_file_path
                else self._file_path or self._source_id or ""
            ),
            plugin_params=self._plugin_params.model_dump(exclude_none=True),
            errors=errors,
            status=status.value,
            records_ingested=self._records_ingested_total,
            records_failed=self._records_failed_total,
            records_skipped=self._records_skipped_total,
            records_processed=self._records_processed_total,
        )
        await events.emit(
            GulpCoreEvent.SOURCE_FINISHED,
            user_id=self._user_id,
            operation_id=self._operation_id,
            req_id=self._req_id,
            data=ev.model_dump(),
        )

    async def _send_documents_chunk_to_ws(self, docs: list[dict], last: bool) -> None:
        """Publish an ingestion chunk, including an empty terminal raw chunk."""
        send_empty_terminal = (
            not docs
            and self._raw_ingestion
            and self._last_raw_chunk
            and last
            and not self._preview_mode
        )
        if not docs and not send_empty_terminal:
            return

        MutyLogger.get_instance().debug("adding %d docs to ws", len(docs))
        chunk = GulpDocumentsChunkPacket(
            docs=docs,
            chunk_size=len(docs),
            last=last,
            chunk_number=self._chunks_ingested,
        )
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

        if (
            self._chunks_ingested % 50 == 0 or self._last_raw_chunk
        ) and self._raw_ingestion:
            payload = GulpIngestRawProgress(last=self._last_raw_chunk)
            await redis_broker.put(
                t=WSDATA_INGEST_RAW_PROGRESS,
                ws_id=self._ws_id,
                operation_id=self._operation_id,
                user_id=self._user_id,
                req_id=self._req_id,
                d=payload.model_dump(exclude_none=True),
                private=False,
            )

    async def _flush_buffer_and_send_to_ws(
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
        chunk = self._detach_ingestion_chunk(flt=flt, all_fields_on_ws=all_fields_on_ws)
        return await self._process_ingestion_chunk(
            chunk, wait_for_refresh=wait_for_refresh, update_stats=False
        )

    def _detach_ingestion_chunk(
        self,
        flt: GulpIngestionFilter = None,
        all_fields_on_ws: bool = False,
    ) -> _GulpIngestionChunk:
        """Move the current mutable buffer and counters into an owned envelope."""
        chunk = _GulpIngestionChunk(
            documents=self._docs_buffer,
            records_processed=self._records_processed_per_chunk,
            records_failed=self._records_failed_per_chunk,
            flt=flt,
            sequence=self._chunk_sequence_next,
            all_fields_on_ws=all_fields_on_ws or self._external_query,
        )
        self._chunk_sequence_next += 1
        self._docs_buffer = []
        self._records_processed_per_chunk = 0
        self._records_failed_per_chunk = 0

        if (
            self._chunk_build_started is not None
            and GulpConfig.get_instance().prometheus_enabled()
        ):
            try:
                GulpMetrics.ingestion_chunk_build_duration_seconds.observe(
                    time.perf_counter() - self._chunk_build_started
                )
            except Exception:
                pass
        self._chunk_build_started = None
        return chunk

    async def _prepare_ingestion_chunk(self, chunk: _GulpIngestionChunk) -> None:
        """Run synchronous pre-ingestion work and serialize the exact bulk body."""
        if not chunk.documents:
            return

        MutyLogger.get_instance().debug(
            "processing chunk of %d documents with plugin=%s",
            len(chunk.documents),
            self.name,
        )
        if (
            not self._preview_mode
            and self._event_listener_mask & events.CHUNK_PRE_INGEST
        ):
            pre_ingest_ev = GulpChunkPrePostIngestEventData(
                chunk=chunk.documents,
                index=self._index,
                req_id=self._req_id,
                ws_id=self._ws_id,
                user_id=self._user_id,
                operation_id=self._operation_id,
                plugin=self.name,
                flt=chunk.flt,
            )
            try:
                result = await events.emit(
                    GulpCoreEvent.CHUNK_PRE_INGEST,
                    wait=True,
                    timeout=30.0,
                    user_id=self._user_id,
                    operation_id=self._operation_id,
                    req_id=self._req_id,
                    data=pre_ingest_ev.model_dump(exclude_none=True),
                )
                transformed = result.result.get("chunk")
                if not isinstance(transformed, list) or not all(
                    isinstance(document, dict) for document in transformed
                ):
                    raise ValueError(
                        "chunk_pre_ingest listener returned an invalid chunk"
                    )
                chunk.documents = transformed
            except Exception:
                self._chunk_pre_ingest_failed = True
                raise

        chunk.prepared = await GulpOpenSearch.get_instance().prepare_bulk_ingest(
            chunk.documents, flt=chunk.flt
        )

    async def _run_ingestion_bulk(
        self,
        chunk: _GulpIngestionChunk,
        wait_for_refresh: bool,
    ) -> tuple[int, list[dict], int]:
        """Run only OpenSearch work; safe to execute beside record parsing."""
        if self._raw_ingestion:
            wait_for_refresh = self._last_raw_chunk
        if chunk.prepared is None or not chunk.prepared.body:
            if wait_for_refresh:
                await GulpOpenSearch.get_instance().index_refresh(self._index)
            return 0, [], 0
        return await GulpOpenSearch.get_instance().bulk_ingest_prepared(
            self._index,
            chunk.prepared,
            wait_for_refresh=wait_for_refresh,
        )

    def _commit_ingestion_chunk(
        self,
        chunk: _GulpIngestionChunk,
        skipped: int,
        ingested: int,
        failed: int,
    ) -> None:
        """Commit one consumed chunk exactly once to source and pending stats."""
        if chunk.totals_committed:
            return
        chunk.totals_committed = True
        self._records_processed_total += chunk.records_processed
        self._records_failed_total += chunk.records_failed + failed
        self._records_skipped_total += skipped
        self._records_ingested_total += ingested
        self._stats_pending_processed += chunk.records_processed
        self._stats_pending_failed += chunk.records_failed + failed
        self._stats_pending_skipped += skipped
        self._stats_pending_ingested += ingested

    async def _flush_pending_ingestion_stats(self) -> None:
        """Persist all acknowledged results not yet reflected in request stats."""
        if not any(
            (
                self._stats_pending_processed,
                self._stats_pending_failed,
                self._stats_pending_skipped,
                self._stats_pending_ingested,
            )
        ):
            return
        await self._stats.update_ingestion_stats(
            self._sess,
            user_id=self._user_id,
            ws_id=self._ws_id,
            records_ingested=self._stats_pending_ingested,
            records_skipped=self._stats_pending_skipped,
            records_processed=self._stats_pending_processed,
            records_failed=self._stats_pending_failed,
        )
        self._stats_pending_processed = 0
        self._stats_pending_failed = 0
        self._stats_pending_skipped = 0
        self._stats_pending_ingested = 0

    def _update_adaptive_chunk_size(self, chunk: _GulpIngestionChunk) -> None:
        """Retarget the sole document-count boundary from observed NDJSON bytes."""
        if (
            not self._adaptive_chunk_enabled
            or chunk.prepared is None
            or not chunk.prepared.body
            or not chunk.prepared.documents
        ):
            return

        target_bytes = min(
            5 * 1024 * 1024,
            int(chunk.prepared.max_content_length_bytes * 0.8),
        )
        proposed = round(
            len(chunk.prepared.documents) * target_bytes / chunk.prepared.size_bytes
        )
        current = self._adaptive_chunk_size
        lower = max(1, (current + 1) // 2)
        upper = min(8000, current * 2)
        self._adaptive_chunk_size = max(1, min(8000, proposed, upper))
        self._adaptive_chunk_size = max(lower, self._adaptive_chunk_size)
        MutyLogger.get_instance().debug(
            "adaptive ingestion chunk size changed from %d to %d "
            "(documents=%d, ndjson_bytes=%d, target_bytes=%d)",
            current,
            self._adaptive_chunk_size,
            len(chunk.prepared.documents),
            chunk.prepared.size_bytes,
            target_bytes,
        )

    async def _postprocess_ingestion_chunk(
        self,
        chunk: _GulpIngestionChunk,
        ingested_docs: list[dict],
        wait_for_refresh: bool,
    ) -> None:
        """Run ordered websocket, cancellation, callback, and event work."""
        if self._raw_ingestion:
            wait_for_refresh = self._last_raw_chunk
        if not chunk.documents:
            await self._send_documents_chunk_to_ws([], last=wait_for_refresh)
            return

        def _select_ws_doc_fields(doc: dict) -> dict:
            return (
                doc
                if chunk.all_fields_on_ws
                else {
                    field: doc[field] for field in QUERY_DEFAULT_FIELDS if field in doc
                }
            )

        if chunk.flt:
            ws_filter = deepcopy(chunk.flt)
            ws_filter.storage_ignore_filter = False
            ws_docs = [
                _select_ws_doc_fields(doc)
                for doc in ingested_docs
                if GulpIngestionFilter.filter_doc_for_ingestion(doc, ws_filter)
                == GulpDocumentFilterResult.ACCEPT
            ]
        else:
            ws_docs = [_select_ws_doc_fields(doc) for doc in ingested_docs]
        await self._send_documents_chunk_to_ws(ws_docs, last=wait_for_refresh)

        if ingested_docs and not self._preview_mode:
            current_plugin: GulpPluginBase | None = self
            while current_plugin is not None:
                callback = current_plugin._plugin_params._chunk_ingestion_callback
                if callback:
                    await callback(
                        self._sess,
                        ingested_docs,
                        index=self._index,
                        req_id=self._req_id,
                        ws_id=self._ws_id,
                        user_id=self._user_id,
                        operation_id=self._operation_id,
                    )
                current_plugin = current_plugin._upper_instance

            if self._event_listener_mask & events.CHUNK_POST_INGEST:
                try:
                    post_ingest_ev = GulpChunkPrePostIngestEventData(
                        chunk=ingested_docs,
                        index=self._index,
                        req_id=self._req_id,
                        ws_id=self._ws_id,
                        user_id=self._user_id,
                        operation_id=self._operation_id,
                        plugin=self.name,
                        flt=chunk.flt,
                    )
                    await events.emit(
                        GulpCoreEvent.CHUNK_POST_INGEST,
                        user_id=self._user_id,
                        operation_id=self._operation_id,
                        req_id=self._req_id,
                        data=post_ingest_ev.model_dump(exclude_none=True),
                    )
                except Exception:
                    MutyLogger.get_instance().exception(
                        "failed to emit chunk-post-ingest event"
                    )

    async def _acknowledge_ingestion_chunk(
        self,
        chunk: _GulpIngestionChunk,
        wait_for_refresh: bool,
        update_stats: bool,
    ) -> tuple[int, int, int]:
        """Acknowledge one prepared chunk and perform ordered coordinator work."""
        try:
            result = (
                await chunk.bulk_task
                if chunk.bulk_task is not None
                else await self._run_ingestion_bulk(chunk, wait_for_refresh)
            )
        except Exception:
            self._commit_ingestion_chunk(chunk, 0, 0, 0)
            if update_stats:
                await self._flush_pending_ingestion_stats()
            raise

        skipped, ingested_docs, failed = result
        ingested = len(ingested_docs)
        self._commit_ingestion_chunk(chunk, skipped, ingested, failed)
        self._update_adaptive_chunk_size(chunk)

        post_started = time.perf_counter()
        post_error: Exception | None = None
        if (
            failed > 0
            and GulpConfig.get_instance().debug_abort_on_opensearch_ingestion_error()
        ):
            post_error = RuntimeError(
                "failed=%d, opensearch ingestion errors means GulpDocument contains "
                "invalid data, review errors on collab db!" % failed
            )
        else:
            try:
                await self._postprocess_ingestion_chunk(
                    chunk, ingested_docs, wait_for_refresh
                )
            except Exception as ex:
                post_error = ex

        if GulpConfig.get_instance().prometheus_enabled():
            try:
                GulpMetrics.ingestion_chunk_postprocess_duration_seconds.observe(
                    time.perf_counter() - post_started
                )
            except Exception:
                pass
        if update_stats:
            await self._flush_pending_ingestion_stats()
        if chunk.documents:
            canceled = await GulpRequestStats.is_canceled(self._sess, self._req_id)
            # The cancellation SELECT starts a transaction. Close it before a
            # plugin spends an unbounded amount of time building the next chunk.
            await self._sess.commit()
            if canceled:
                self._req_canceled = True
                MutyLogger.get_instance().warning(
                    "_process_docs_chunk: request %s canceled!", self._req_id
                )
        if post_error is not None:
            raise post_error
        return skipped, ingested, failed

    async def _process_ingestion_chunk(
        self,
        chunk: _GulpIngestionChunk,
        wait_for_refresh: bool,
        update_stats: bool,
    ) -> tuple[int, int, int]:
        """Prepare and synchronously acknowledge one detached logical chunk."""
        try:
            await self._prepare_ingestion_chunk(chunk)
        except Exception:
            self._commit_ingestion_chunk(chunk, 0, 0, 0)
            if update_stats:
                await self._flush_pending_ingestion_stats()
            raise
        return await self._acknowledge_ingestion_chunk(
            chunk,
            wait_for_refresh=wait_for_refresh,
            update_stats=update_stats,
        )

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
        cached_ctx_id = self.doc_value_cache.get_value(cache_key)
        if cached_ctx_id:
            # MutyLogger.get_instance().debug("found context %s in cache, returning id %s", v, cached_ctx_id)
            # return cached context id
            return cached_ctx_id

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
        self.doc_value_cache.set_value(cache_key, context.id)

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
        cached_source_id = self.doc_value_cache.get_value(cache_key)
        if cached_source_id:
            # MutyLogger.get_instance().debug("found source %s in cache for context %s, returning id %s, cache_key=%s",v,context_id, cached_source_id, cache_key)
            return cached_source_id

        # cache miss - create new source (or get existing)
        # MutyLogger.get_instance().warning("NOT found source %s in cache for cache_key=%s",context_id, cache_key)

        # fetch context object
        context: GulpContext = await GulpContext.get_by_id(
            self._sess, context_id, recursive=True
        )
        """if context and force_v_as_source_id:
            for s in context.sources:
                if s.id == v:
                    # MutyLogger.get_instance().warning("*** not found in cache but present in context.sources, cache it now, src_id=%s", s.id)
                    self.doc_value_cache.set_value(cache_key, s.id)
                    return s.id"""

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
        self.doc_value_cache.set_value(cache_key, source.id)
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
            ws_id (str): the websocket id to receive WSDATA_DOCUMENTS_CHUNK and WSDATA_QUERY_DONE packets, ignored if q_options.preview_mode is set
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
        self._normalized_timestamp_cache.clear()

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
        **kwargs,
    ) -> list[dict]:
        """
        a GulpDocumentChunkCallback to be implemented by a plugin to enrich a chunk of documents, called by _enrich_documents_chunk_wrapper

        > kwargs["fields"] is set, propagated from the `fields` parameter in the initial plugin call to `enrich_documents` or `enrich_single_document`

        NOTE: implementors should process the chunk then call super()._enrich_documents_chunk to allow for stacked plugins.
        """
        u: GulpPluginBase = self
        while u._upper_instance != None:
            # walk the chain of upper stacked plugins and call each _enrich_documents_chunk (from bottom to top)
            try:
                chunk = await self._upper_enrich_documents_chunk_fun(
                    sess,
                    chunk,
                    chunk_num=chunk_num,
                    total_hits=total_hits,
                    index=index,
                    last=last,
                    req_id=req_id,
                    q_name=q_name,
                    **kwargs,
                )
                u = u._upper_instance
            except Exception as e:
                raise e
        if chunk is None:
            raise ObjectNotFound("cannot enrich!")
        if len(chunk) == 0:
            raise ObjectNotFound("no enrich data found")
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
        **kwargs,
    ) -> list[dict]:
        """a GulpDocumentsChunk callback wrapper to enrich each chunk of documents during query"""
        cb_context = kwargs["cb_context"]
        stats: GulpRequestStats = cb_context["stats"]
        ws_id: str = cb_context["ws_id"]
        flt: GulpQueryFilter = cb_context["flt"]
        stats_last = last and not cb_context.get("defer_final_stats", False)

        enriched_chunk = await self._enrich_documents_chunk_cb(
            sess,
            chunk,
            chunk_num=chunk_num,
            total_hits=total_hits,
            index=index,
            last=last,
            req_id=req_id,
            q_name=q_name,
            **kwargs,
        )

        MutyLogger.get_instance().debug(
            "%s: enriched %d documents, last=%r",
            self.name,
            len(enriched_chunk),
            last,
        )

        # update the documents on opensearch
        # also ensure no highlight field is left from the query
        for d in enriched_chunk:
            d.pop("highlight", None)

        dry_run: bool = GulpConfig.get_instance().debug_enrich_dry_run()
        if not dry_run and enriched_chunk:
            updated, _, errors = await GulpOpenSearch.get_instance().update_documents(
                index, enriched_chunk, wait_for_refresh=last
            )
        elif not enriched_chunk:
            updated = 0
            errors = []
        else:
            # dry run, simulate ...
            updated = len(enriched_chunk)
            errors = []

        num_updated = updated
        cb_context["total_updated"] += num_updated
        cb_context["errors"].extend(errors)
        cb_context["total_hits"] = total_hits

        # update running stats
        await stats.update_updatedocuments_stats(
            sess,
            total_hits=total_hits,
            updated=num_updated,
            flt=flt,
            errors=errors,
            user_id=stats.user_id,
            ws_id=ws_id,
            last=stats_last,
        )
        return enriched_chunk

    async def enrich_documents(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        fields: dict,
        q: dict = None,
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
            fields (dict): the fields to enrich as { "field_name": field_value (will use "field_value" as input for enrichment), "other_field": None (will get value from document["other_field"] as input for enrichment) }
            q (dict, optional): if any, the plugin itself may supply a raw query to further refine the documents to be selected for the enrichment. if not set, the engine will select the documents based on fields (all documents having the specified fields set, see `fields`). Defaults to None, ignored when calling the base method.
            flt(GulpQueryFilter, optional): a filter to restrict the documents to enrich. Defaults to None.
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.

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
        if fields is None:
            raise ValueError(
                "enrich_documents: a non-empty fields parameter is mandatory"
            )

        # initialize these in plugin
        self._user_id = user_id
        self._stats = stats
        self._req_id = req_id
        self._ws_id = ws_id
        self._sess = sess
        self._operation_id = operation_id
        self._index = index

        # Build the base enrichment-selection query from "fields".
        # - fields with value None: input is read from the document, so field must exist.
        # this basically allows to select only documents having the specified fields, without caring about their value, which is a common use case for enrichment plugins (i.e. enrich all documents having "file.hash" field, regardless of the hash value)
        # - fields with an explicit value
        # this allows for the enrich plugin to search (on its enrich provider) for a specific value which, is found, will be added as enrichment to (all) the documents.
        # NOTE: the latter (fields with an explicit value) is meant to be used only with single document, not a range
        should_filters: list[dict] = []
        for f_name, f_value in fields.items():
            if f_value is None:
                should_filters.append({"exists": {"field": f_name}})

        if should_filters:
            # only these documents
            qq: dict = {
                "query": {
                    "bool": {
                        "should": should_filters,
                        "minimum_should_match": 1,
                    }
                }
            }
        else:
            # all documents
            qq = {"query": {"match_all": {}}}

        # check if the caller provided a raw query to be used, if so merge with q
        if q:
            MutyLogger.get_instance().debug(
                "enrich_documents: raw query provided by caller: %s", q
            )
            # merge
            qq = GulpQueryHelpers.merge_queries(qq, q)

        # check if we must further filter with GulpQUeryFilter
        if flt and not flt.is_empty():
            MutyLogger.get_instance().debug(
                "enrich_documents: applying GulpQueryFilter: %s", flt
            )
            qqq = flt.to_opensearch_dsl()
            qq = GulpQueryHelpers.merge_queries(qq, qqq)

        # force return all fields
        q_options = GulpQueryParameters(fields="*", highlight_results=False)
        errors: list[str] = []
        cb_context = {
            "total_hits": 0,
            "total_updated": 0,
            "flt": flt,
            "errors": errors,
            "stats": stats,
            "ws_id": ws_id,
            "defer_final_stats": kwargs.get("defer_final_stats", False),
        }
        canceled: bool = False
        MutyLogger.get_instance().debug(
            "enrich query:\n%s", orjson.dumps(qq, option=orjson.OPT_INDENT_2).decode()
        )
        # try:
        await GulpOpenSearch.get_instance().search_dsl(
            sess,
            index,
            qq,
            req_id=self._req_id,
            q_options=q_options,
            callback=self._enrich_documents_chunk_wrapper,
            cb_context=cb_context,
            fields=fields,
        )
        # except Exception as ex:
        #     MutyLogger.get_instance().exception(ex)
        #     errors.append(str(ex))
        #     if isinstance(ex, RequestCanceledError):
        #         # flag canceled
        #         canceled = True

        return cb_context["total_hits"], cb_context["total_updated"], errors, canceled

    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        operation_id: str,
        index: str,
        fields: dict,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        """
        to be implemented in a plugin to enrich a single document on-demand.

        Args:
            sess (AsyncSession): The database session.
            doc (dict): the document to enrich
            operation_id (str): id of the operation on collab database.
            index (str): the index to query
            fields (dict): the fields to enrich as { "field_name": field_value (will use "field_value" as input for enrichment), "other_field": None (will get value from document["other_field"] as input for enrichment) }
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

        if not fields:
            raise ValueError(
                "enrich_single_document: a non-empty fields parameter is mandatory"
            )

        self._operation_id = operation_id
        self._index = index
        # await self._initialize(plugin_params=plugin_params)

        # get the document and call the plugin to enrich
        doc = await GulpOpenSearch.get_instance().query_single_document(index, doc_id)
        try:
            docs = await self._enrich_documents_chunk(
                None, [doc], fields=fields
            )  # sess not used here
        except Exception as e:
            raise Exception(
                "failed to enrich document %s with plugin %s: %s"
                % (doc_id, self.name, str(e))
            ) from e

        # update the document
        dry_run: bool = GulpConfig.get_instance().debug_enrich_dry_run()
        if dry_run:
            # no update happens, dry run set
            return docs[0]
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
        self._normalized_timestamp_cache.clear()
        self._stored_file_id = (
            self._upper_instance._stored_file_id if self._upper_instance else None
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)
        if self._plugin_params.preview_mode:
            self._preview_mode = True

        config = GulpConfig.get_instance()
        initial_chunk_size = config.ingestion_documents_chunk_size()
        if self._plugin_params.override_chunk_size is not None:
            self._adaptive_chunk_enabled = False
            self._adaptive_chunk_size = self._plugin_params.override_chunk_size
        else:
            self._adaptive_chunk_enabled = (
                config.ingestion_documents_adaptive_chunk_size()
            )
            self._adaptive_chunk_size = (
                min(8000, initial_chunk_size)
                if self._adaptive_chunk_enabled
                else initial_chunk_size
            )
        self._pipeline_enabled = bool(
            config.ingestion_pipeline_enabled()
            and not self._preview_mode
            and not self._raw_ingestion
            and not self._external_query
        )
        self._pipeline_active_chunk = None
        self._chunk_sequence_next = 0
        self._chunk_build_started = time.perf_counter()

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

        try:
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
            lower._stored_file_id = None
            await lower._initialize(plugin_params)
            lower._preview_mode = self._preview_mode
            return lower
        except BaseException:
            await lower.unload()
            raise

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

        the outer plugin may override its default lower plugin with `GulpPluginParameters.stacked_plugin`.

        the engine takes care of postprocessing data:

        - for any record, calls the upper plugin _record_to_gulp_document function after the lower
        - when flushing a chunk of documents to opensearch, calls the upper plugin _enrich_documents function after the lower

        Args:
            plugin (str): the default plugin to load as lower
            *args: additional arguments to pass to the plugin constructor.
            cache_mode (GulpPluginCacheMode, optional): the cache mode for the plugin. Defaults to GulpPluginCacheMode.DEFAULT.
            **kwargs: additional keyword arguments to pass to the plugin constructor.
        Raise:
            ValueError: if the plugin types do not match or are not INGESTION or ENRICHMENT
        Returns:
            PluginBase: the loaded plugin
        """
        if self._upper_instance is None:
            plugin = self._plugin_params.stacked_plugin or plugin
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
        self._ingestion_owner = lower

        # set this plugin as stacked
        self._stacked = True

        # set mapping in lower plugin
        lower._mapping_id = self._mapping_id
        lower._mappings = self._mappings
        MutyLogger.get_instance().debug(
            "---> plugin %s stacked over %s", self.name, lower.name
        )
        return lower

    def _finalize_process_record(self, doc: GulpDocument | dict) -> list[dict]:
        """
        finalize processing a record generating a list with one (default) or more documents (mapping can generate extra document/s from a single record)

        Args:
            doc (GulpDocument | dict): the gulp document to be used as base for extra documents.

        Returns:
            list[dict]: the final list of documents to be ingested.

        NOTE: called by the engine, do not call this function directly.
        """

        if not self._extra_docs:
            if isinstance(doc, dict):
                return [doc]
            else:
                return [doc.model_dump(by_alias=True)]

        base_doc_dump = doc.model_dump() if isinstance(doc, GulpDocument) else doc
        # MutyLogger.get_instance().debug(orjson.dumps("base doc:\n%s" % (base_doc_dump), option=orjson.OPT_INDENT_2).decode())

        extra_docs = []
        expanded_docs = expand_extra_docs(
            base_doc_dump,
            self._extra_docs,
            base_document_id=(
                doc.id if isinstance(doc, GulpDocument) else doc.get("id")
            ),
        )
        for new_doc_data in expanded_docs[1:]:
            # MutyLogger.get_instance().debug("creating new doc with %s\nand\n%s" % (orjson.dumps(extra_fields, option=orjson.OPT_INDENT_2), orjson.dumps(base_doc_dump, option=orjson.OPT_INDENT_2).decode()))
            # default event code must be ignored for the extra document (since the extra document, by design, has a different event code)
            new_doc = GulpDocument(
                self,
                **new_doc_data,
                __ignore_default_event_code__=True,
                __timestamp_cache_slot__=f"extra:{new_doc_data['event_code']}",
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
    ) -> GulpDocument | dict | list[GulpDocument] | list[dict] | None:
        """
        to be implemented in a plugin to convert a record to a GulpDocument.

        parses a record and calls _process_key for each key/value pair, generating one or more GulpDocuments to be ingested.

        NOTE: called by the engine, do not call this function directly.
            THIS FUNCTION MUST NOT GENERATE EXCEPTIONS, on failure it should return None to indicate a malformed record.

        Args:
            record (any): the record to convert.
                NOTE: in a stacked plugin, this method always receives `record` as a `dict` (GulpDocument.model_dump()) and MUST return a new or modified `dict` after processing.
            record_idx (int): the index of the record in the source
            kwargs: additional keyword arguments
        Returns:
            GulpDocument|dict|list[GulpDocument]|list[dict]|None: the GulpDocument, dict, list of GulpDocuments, list of dicts, or None to skip processing (i.e. plugin detected malformed record)

        """
        raise NotImplementedError("not implemented!")

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

        docs: list[dict] = []
        if isinstance(doc, list):
            # record_to_gulp_document generated more than one doc
            for d in doc:
                docs.extend(self._finalize_process_record(d))
        else:
            docs = self._finalize_process_record(doc)

        # apply upper plugin processing if stacked
        if self._upper_record_to_gulp_document_fun:
            for i, doc in enumerate(docs):
                u: GulpPluginBase = self
                # walk the chain of upper stacked plugins and call each _record_to_gulp_document (from bottom to top)
                while u._upper_instance != None:
                    docs[i] = await u._upper_record_to_gulp_document_fun(
                        doc, record_idx, **kwargs
                    )
                    u = u._upper_instance

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
                    # MutyLogger.get_instance().debug("**** calling source_id_from_doc_value, context_id=%s, k=gulp_source_id, v=%s", context_id, default_src)
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
        Returns the selected (self._mapping_id) mapping or the instance fallback.

        The mapping lookup remains dynamic so plugins may change ``_mapping_id``
        while processing a source. The empty fallback is allocated once per plugin
        instance.

        Returns:
            GulpMapping: The selected mapping.
        """
        return self._mappings.get(self._mapping_id, self._empty_mapping)

    def _normalize_timestamp_cached(
        self,
        timestamp: Any,
        plugin_params: GulpPluginParameters | None = None,
        timestamp_format: str | None = None,
        timestamp_naive_timezone: Literal["local", "utc"] = "local",
        fallback_on_format_error: bool = False,
        cache_slot: str = "primary",
    ) -> GulpNormalizedTimestamp:
        """Normalize one ingestion timestamp, reusing a consecutive string."""
        if (
            self.type() != GulpPluginType.INGESTION
            or not isinstance(timestamp, str)
        ):
            return GulpDocument.normalize_timestamp(
                timestamp,
                plugin_params=plugin_params,
                timestamp_format=timestamp_format,
                timestamp_naive_timezone=timestamp_naive_timezone,
                fallback_on_format_error=fallback_on_format_error,
            )

        timestamp_offset_msec = (
            getattr(plugin_params, "timestamp_offset_msec", 0)
            if plugin_params is not None
            else None
        )
        local_timezone = (
            time.tzname if timestamp_naive_timezone == "local" else None
        )
        cache_key = (
            timestamp,
            timestamp_format,
            timestamp_naive_timezone,
            fallback_on_format_error,
            timestamp_offset_msec,
            local_timezone,
        )
        cache = getattr(self, "_normalized_timestamp_cache", None)
        if cache is None:
            cache = {}
            self._normalized_timestamp_cache = cache
        cached = cache.get(cache_slot)
        if cached is not None and cached[0] == cache_key:
            return cached[1]

        normalized = GulpDocument.normalize_timestamp(
            timestamp,
            plugin_params=plugin_params,
            timestamp_format=timestamp_format,
            timestamp_naive_timezone=timestamp_naive_timezone,
            fallback_on_format_error=fallback_on_format_error,
        )
        cache[cache_slot] = (cache_key, normalized)
        return normalized

    def _build_or_update_enriched_obj(
        self, doc: dict, k: str, v: Any, skip_none: bool = True
    ) -> dict:
        """
        builds or updates an enriched objject to be set in doc["gulp.enriched"]

        Args:
            doc (dict): The document to update.
            k (str): The key to add.
            v (any): The value to add.
            skip_none (bool): whether to skip adding None or empty values, defaults to True.
        """
        if skip_none and v is None or v == "":
            # skip empties
            return {}

        # check if we have an gulp.enriched node yet in the document
        gulp_enriched_key: str = GulpOpenSearch.ENRICHED_PREFIX
        enriched_dict: dict = doc.get(gulp_enriched_key, None)
        if not enriched_dict:
            enriched_dict = {}

        # add a subkey with the plugin name
        enriched_subkey = self.name
        if enriched_dict.get(enriched_subkey, None) is None:
            enriched_dict[enriched_subkey] = {}

        # return the whole { "gulp.enriched": { ... } } node back
        enriched_dict[enriched_subkey][k] = v
        return {gulp_enriched_key: enriched_dict}

    @staticmethod
    def build_or_update_unmapped_obj(
        doc: dict, k: str, v: Any, skip_none: bool = True, as_is: bool = False
    ) -> dict:
        """
        builds or updates an unmapped object to be set in doc["gunmapped"]

        Args:
            doc (dict): The document to update.
            k (str): The key to add.
            v (any): The value to add.
            skip_none (bool): whether to skip adding None or empty values, defaults to True.
            as_is (bool): whether to return the value as is, without adding it to an "unmapped" object, defaults to False.
        """
        if skip_none and v is None or v == "":
            # skip empties
            return {}
        if as_is:
            # return the value as is, without adding it to an "unmapped" object
            return {k: v}

        # check if we have an unmapped node yet in the document
        unmapped_key: str = GulpOpenSearch.UNMAPPED_PREFIX
        unmapped: dict = doc.get(unmapped_key, None)
        if not unmapped:
            unmapped = {}

        # return the whole { "gulp.unmapped": { ... } } node back
        unmapped[k] = v
        return {unmapped_key: unmapped}

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
            skip_unmapped (bool): whether to skip unmapped keys, defaults to False(i.e. use it to call _try_map_ecs for context/source). either, source_key is added in the `unmapped` object.
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
                kk, vv = self._type_checks(
                    k, source_value, force_type_set=force_type is not None
                )
                if vv:
                    d[kk] = vv
                    set_keys.append(kk)
        else:
            # fallback, unmapped key (should never happen, means "ecs" is set to "" ...)
            if not skip_unmapped:
                d = GulpPluginBase.build_or_update_unmapped_obj(
                    d, source_key, source_value, as_is=self._unmapped_as_is
                )
                set_keys.append(source_key)

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

    def _apply_value_aliases(
        self, mapped_key: str, d: dict, value_aliases: dict[str, dict[str, dict]]
    ) -> dict:
        """Apply value aliases after mapping via the shared mapping helper."""

        return apply_value_aliases(
            mapped_key=mapped_key,
            d=d,
            value_aliases=value_aliases,
            record_type=self._record_type,
        )

    def _get_timestamp_format_for_default_timestamp(self):
        """gets the timestamp format for the default "@timestamp" field in the currently used mapping, if any, by looking in the mapping for the field with ecs="@timestamp" """
        timestamp_format: str = None
        mapping = self.selected_mapping()
        for _, d in mapping.fields.items():
            d: GulpMappingField
            if isinstance(d.ecs, str) and d.ecs == "@timestamp":
                timestamp_format = d.timestamp_format
                break
            elif isinstance(d.ecs, list) and "@timestamp" in d.ecs:
                timestamp_format = d.timestamp_format
                break
        return timestamp_format

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

        if source_value is None:
            return {}

        # check if we have a mapping for source_key
        mapping = self.selected_mapping()
        self._unmapped_as_is = mapping.unmapped_as_is

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
        allow_unmapped_fields: bool = True
        if (
            not GulpConfig.get_instance().ingestion_allow_unmapped_fields()
            or not self._plugin_params.override_allow_unmapped_fields
        ):
            allow_unmapped_fields = False

        if not field_mapping:
            # missing mapping at all (no ecs and no timestamp field)
            if not allow_unmapped_fields:
                # no umapped fields in the output!
                return {}

            # update the "unmapped" node
            d = GulpPluginBase.build_or_update_unmapped_obj(
                doc, source_key, source_value, as_is=self._unmapped_as_is
            )
            if value_aliases:
                # apply aliases
                self._apply_value_aliases(k, d, value_aliases)
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
            d: dict = {}
            force_type: str = "str"  # always string
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
                        # MutyLogger.get_instance().debug("**** calling source_id_from_doc_value, context_id=%s, k=%s, v=%s", ctx_id, source_key, source_value)
                        src_id: str = await self._source_id_from_doc_value(
                            ctx_id, source_key, source_value
                        )
                    else:
                        # directly use the value as source_id
                        # MutyLogger.get_instance().debug("**** calling source_id_from_doc_value, context_id=%s, k=%s, v=%s, force_v_as_source_id=True", ctx_id, source_key, source_value)
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
            flattened = flatten_json_value(source_value, prefix=source_key)
            for k, v in flattened.items():
                processed: dict = await self._process_key(k, v, doc, **kwargs)
                if value_aliases:
                    # apply aliases
                    for kk, _ in processed.items():
                        self._apply_value_aliases(kk, processed, value_aliases)
                d.update(processed)
            return d

        force_type: str = field_mapping.force_type
        source_value = transform_scalar(
            source_value,
            force_type=force_type,
            invalid_int=0,
            invalid_float=0.0,
        )

        if (
            field_mapping.multiplier
            and isinstance(source_value, int)
            and field_mapping.multiplier > 1
        ):
            # apply multiplier
            source_value = int(
                transform_scalar(
                    source_value,
                    multiplier=field_mapping.multiplier,
                    multiplier_first=False,
                )
            )

        if is_timestamp:
            # this field represents a timestamp to be handled and turned into nanoseconds-from-unix-epoch
            # NOTE: a field mapped as "@timestamp" (AND ONLY THAT) needs `is_timestamp` set ONLY IF its type is different than `generic` (i.e. if it is a `chrome` or `windows_filetime` timestamp)
            # this is because `@timestamp` is a special field handled directly by the engine.
            try:
                source_value = convert_special_timestamp(
                    source_value,
                    timestamp_kind=is_timestamp,
                    timestamp_format=field_mapping.timestamp_format,
                    output="unix_nanos",
                )
                force_type = "int"
            except ValueError:
                # not supported
                MutyLogger.get_instance().warning(
                    f"timestamp type {field_mapping.is_timestamp} not supported for key {source_key}, value=%s, set to 0...",
                    str(source_value),
                )
                source_value = 0

        if field_mapping.extra_doc_with_event_code:
            # this will trigger the creation of an extra document
            # with the given event code in _finalize_process_record()
            # remove field/s corresponding to this key in the generated extra document
            d, _ = self._try_map_ecs(field_mapping, d, source_key, source_value)
            self._extra_docs.append(
                build_extra_doc_fields(
                    field_mapping,
                    timestamp_value=source_value,
                    mapped_fields=d.keys(),
                )
            )

            if value_aliases:
                # apply aliases
                for kk, _ in d.items():
                    self._apply_value_aliases(kk, d, value_aliases)

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
        chunk = self._detach_ingestion_chunk(flt=flt)
        if self._pipeline_enabled:
            # A full next chunk is retained locally until the preceding bulk is
            # acknowledged. It is never submitted after a preceding failure.
            await self._acknowledge_pipeline_chunk(update_stats=True)
            try:
                await self._prepare_ingestion_chunk(chunk)
            except Exception:
                self._commit_ingestion_chunk(chunk, 0, 0, 0)
                await self._flush_pending_ingestion_stats()
                raise
            chunk.bulk_task = asyncio.create_task(
                self._run_ingestion_bulk(chunk, wait_for_refresh=False),
                name=f"gulp_bulk_{self._req_id}_{chunk.sequence}",
            )
            self._pipeline_active_chunk = chunk
            # Buffered readers and CPU-bound plugin coroutines may process many
            # records without suspending. Give the newly scheduled bulk one
            # turn now so it reaches OpenSearch before parsing resumes.
            await asyncio.sleep(0)
            self._chunk_build_started = time.perf_counter()
            return

        await self._process_ingestion_chunk(
            chunk,
            wait_for_refresh=wait_for_refresh,
            update_stats=True,
        )
        self._check_ingestion_thresholds()
        self._chunk_build_started = time.perf_counter()

    async def _acknowledge_pipeline_chunk(self, update_stats: bool) -> None:
        """Consume the sole active OpenSearch task, if present."""
        chunk = self._pipeline_active_chunk
        if chunk is None:
            return
        self._pipeline_active_chunk = None
        await self._acknowledge_ingestion_chunk(
            chunk,
            wait_for_refresh=False,
            update_stats=update_stats,
        )
        self._check_ingestion_thresholds()

    def _check_ingestion_thresholds(self) -> None:
        """Raise only after the acknowledged chunk was consumed and reported."""
        if self._req_canceled:
            raise RequestCanceledError(f"request {self._req_id} canceled!")

        failure_threshold = GulpConfig.get_instance().ingestion_evt_failure_threshold()
        if (
            not self._raw_ingestion
            and failure_threshold > 0
            and (
                self._records_skipped_total >= failure_threshold
                or self._records_failed_total >= failure_threshold
            )
        ):
            raise SourceCanceledError(
                f"ingestion per-source failure threshold reached (tot_skipped={self._records_skipped_total}, "
                f"tot_failed={self._records_failed_total}, threshold={failure_threshold}), "
                f"canceling source..."
            )

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
            kwargs: additional keyword arguments, they will be passed to plugin's `_record_to_gulp_document`

        Returns:
            bool: True if processing should continue, False if it should stop (i.e. in preview mode and enough documents have been accumulated).
        Raises:
            SourceCanceledError: if the per-source failure threshold is reached.
            RequestCanceledError: if the request is canceled.
        """
        if self._external_query:
            # external query, documents have been already filtered by the query
            flt = None

        # This is the only active flush boundary. Adaptive mode changes the
        # count only after observing a completed chunk's exact NDJSON body.
        ingestion_buffer_size = (
            self._adaptive_chunk_size
            or GulpConfig.get_instance().ingestion_documents_chunk_size()
        )

        self._extra_docs = []

        # process this record and generate one or more gulpdocument dictionaries
        try:
            docs = await self._record_to_gulp_documents_wrapper(
                record, record_idx, **kwargs
            )
        except Exception as ex:
            # increment records failed counters
            self._records_failed_per_chunk += 1
            MutyLogger.get_instance().exception(ex)

            # continue processing anyway
            return True

        self._records_processed_per_chunk += 1

        if self._preview_mode:
            # MutyLogger.get_instance().debug("***PREVIEW MODE*** got %d docs", len(docs))
            # preview, accumulate docs
            for d in docs:
                # remove highlight if present
                d.pop("highlight", None)

            self._preview_chunk.extend(docs)
            # MutyLogger.get_instance().debug("accumulated %d docs" % (len(self._preview_chunk)))
            if len(self._preview_chunk) >= self._plugin_params.preview_mode_max_records:
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
        ensure plugin_params is not None and has mapping_parameters set, either from plugin_params.mapping_parameters or from the provided mapping_file/mappings/mapping_id/additional_mapping_files/additional_mappings.
        each parameter is set in plugin_params.mapping_parameters only if plugin_params.mapping_parameters is empty, otherwise it is used to override the corresponding parameter in plugin_params.mapping_parameters.

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
        MutyLogger.get_instance().debug(
            "---> ensuring plugin_params: plugin_params=%s, mapping_file=%s, mappings=%s, mapping_id=%s, additional_mapping_files=%s, additional_mappings=%s",
            plugin_params,
            mapping_file,
            mappings,
            mapping_id,
            additional_mapping_files,
            additional_mappings,
        )
        if not plugin_params:
            # initialize as empty
            MutyLogger.get_instance().debug(
                "---> plugin_params is None, initializing as empty"
            )
            plugin_params = GulpPluginParameters()

        if plugin_params.mapping_parameters.is_empty():
            MutyLogger.get_instance().debug(
                "---> plugin_params.mapping_parameters is empty, setting from provided values"
            )
            # set whole mapping parameters from provided values
            plugin_params.mapping_parameters = GulpMappingParameters(
                mapping_file=mapping_file,
                mapping_id=mapping_id,
                mappings=mappings or {},
                additional_mapping_files=additional_mapping_files or [],
                additional_mappings=additional_mappings or {},
            )
        else:
            # apply overrides for each mapping parameter if provided
            if mapping_file and not plugin_params.mapping_parameters.mapping_file:
                plugin_params.mapping_parameters.mapping_file = mapping_file

            if mapping_id and not plugin_params.mapping_parameters.mapping_id:
                plugin_params.mapping_parameters.mapping_id = mapping_id

            if mappings and not plugin_params.mapping_parameters.mappings:
                """MutyLogger.get_instance().debug(
                    "---> setting plugin_params.mapping_parameters.mappings with provided mappings"
                )"""
                plugin_params.mapping_parameters.mappings = mappings
            else:
                """MutyLogger.get_instance().debug(
                    "---> updating plugin_params.mapping_parameters.mappings with provided mappings"
                )"""
                plugin_params.mapping_parameters.mappings.update(mappings or {})

            if (
                additional_mapping_files
                and not plugin_params.mapping_parameters.additional_mapping_files
            ):
                plugin_params.mapping_parameters.additional_mapping_files = (
                    additional_mapping_files
                )
            if (
                additional_mappings
                and not plugin_params.mapping_parameters.additional_mappings
            ):
                """MutyLogger.get_instance().debug(
                    "---> setting plugin_params.mapping_parameters.additional_mappings with provided additional_mappings"
                )"""
                plugin_params.mapping_parameters.additional_mappings = (
                    additional_mappings
                )
            else:
                """MutyLogger.get_instance().debug(
                    "---> updating plugin_params.mapping_parameters.additional_mappings with provided additional_mappings"
                )"""
                plugin_params.mapping_parameters.additional_mappings.update(
                    additional_mappings or {}
                )

        # MutyLogger.get_instance().debug("---> ensured plugin_params: %s", plugin_params)
        return plugin_params

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

    async def _upload_file_task(
        self,
        file_path: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        user_id: str,
        req_id: str,
    ):
        """
        run during ingestion if "store_file" parameter is set: stores the ingested file in the s3-compatible storage
        """
        obj_name = self.stored_file_id()
        t: dict = {
            "operation_id": operation_id,
            "user_id": user_id,
            "req_id": req_id,
            "context_id": context_id,
            "source_id": source_id,
        }
        await GulpS3.get_instance().upload_file(file_path, object_name=obj_name, tags=t)

    def stored_file_id(self) -> str:
        """Return the cached S3 object ID for the current file ingestion."""
        if self._stored_file_id is None:
            self._stored_file_id = GulpS3.build_object_name(
                self._original_file_path or self._file_path,
                self._operation_id,
                self._context_id,
                self._source_id,
                self._user_id,
                self._req_id,
            )
        return self._stored_file_id

    async def _initialize(self, plugin_params: GulpPluginParameters = None) -> None:
        """
        initialize plugin's copy of mapping and plugin custom parameters with the provided plugin_params or with the default one if not provided.

        this sets self._mappings, self._mapping_id, self._plugin_params (the plugin specific parameters), self._index_type_mapping

        Args:
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.
        Raises:
            ValueError: if mapping_id is set but mappings/mapping_file is not.
            ValueError: if a specific parameter is required but not found in plugin_params.

        """
        # ensure we have a plugin_params object
        self._plugin_params = plugin_params or GulpPluginParameters()
        self._source_finished_event_emitted = False
        self._chunk_pre_ingest_failed = False
        self._event_listener_mask = (
            0 if self._plugin_params.preview_mode else await events.listener_mask()
        )

        MutyLogger.get_instance().debug(
            "---> _initialize: plugin=%s, stacked=%r, plugin_params=%s",
            self.filename,
            self._stacked,
            orjson.dumps(
                self._plugin_params.model_dump(), option=orjson.OPT_INDENT_2
            ).decode(),
        )

        if (
            self.type() == GulpPluginType.INGESTION
            and not self._raw_ingestion
            and not self._plugin_params.preview_mode
            and self._plugin_params.store_file
        ):
            if self._upper_instance:
                MutyLogger.get_instance().warning(
                    "plugin %s is stacked, skipping upload handling (done in the upper)!",
                    self.filename,
                )
            else:
                # start the upload concurrently so we can ingest the file while it's being uploaded
                # keep a handle so we can wait for it before deleting the file later.
                async def _upload_wrapper():
                    try:
                        await self._upload_file_task(
                            self._file_path,
                            self._operation_id,
                            self._context_id,
                            self._source_id,
                            self._user_id,
                            self._req_id,
                        )
                    except Exception as ex:
                        MutyLogger.get_instance().exception(
                            "***ERROR*** in background upload task: %s", ex
                        )

                self.upload_task = asyncio.create_task(_upload_wrapper())

        # parse the custom parameters
        await self._parse_custom_parameters()
        pt: GulpPluginType = self.type()
        if pt == GulpPluginType.ENRICHMENT:
            MutyLogger.get_instance().debug(
                "plugin %s type=%s, no mappings needed", self.name, pt
            )
            return

        # setup mappings
        # in a lower stacked plugin this is already set by the upper with setup_stacked_plugin()
        if not self._mappings:
            self._mappings, self._mapping_id = await mapping_parameters_to_mapping(
                self._plugin_params.mapping_parameters
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

            # enforce to string (element-wise for arrays, so keyword arrays like
            # process.args stay arrays instead of being repr'd into a single string)
            if isinstance(v, list):
                return k, [str(x) for x in v]
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
            # keyword/text fields may legitimately hold arrays (e.g. process.args);
            # stringify element-wise so the array is preserved.
            if isinstance(v, list):
                return k, [str(x) for x in v]
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
            # create or reuse mapping entry and set the mapping id on the source
            from gulp.api.collab.mapping_parameters import GulpMappingParametersEntry

            mp, _ = await GulpMappingParametersEntry.create_if_not_exists(
                self._sess,
                self._plugin_params.mapping_parameters.model_dump(exclude_none=True),
                self._user_id,
                self._operation_id,
            )
            await n.update(self._sess, plugin=self.name, mapping_parameters_id=mp.id)

    def _ingestion_buffer_owner(self) -> "GulpPluginBase":
        """Return the lowest stacked plugin that owns buffering and bulk tasks."""
        owner = self
        visited: set[int] = set()
        while id(owner) not in visited:
            visited.add(id(owner))
            lower_owner = owner._ingestion_owner or owner._lower_instance
            if lower_owner is None:
                break
            owner = lower_owner
        return owner

    def _copy_ingestion_owner_state(self, owner: "GulpPluginBase") -> None:
        """Expose owner totals on the outer plugin used for terminal events."""
        if owner is self:
            return
        self._records_processed_total = owner._records_processed_total
        self._records_failed_total = owner._records_failed_total
        self._records_skipped_total = owner._records_skipped_total
        self._records_ingested_total = owner._records_ingested_total
        self._req_canceled = owner._req_canceled
        self._ctx_src_pairs = owner._ctx_src_pairs

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

        owner = self._ingestion_buffer_owner()
        MutyLogger.get_instance().debug(
            "*** plugin=%s, file_path/source_id=%s, remaining docs to flush in docs_buffer: %d, status=%s ***",
            self.name,
            self._file_path or self._source_id,
            len(owner._docs_buffer),
            self._stats.status,
        )
        errors: list[str] = []
        status: GulpRequestStatus = None
        if ex:
            if isinstance(ex, SourceCanceledError):
                # also SourceCanceled counts as request canceled
                self._req_canceled = True
                owner._req_canceled = True
            else:
                errors.append(muty.log.exception_to_string(ex))

        # An already submitted chunk is always acknowledged. Work constructed
        # after it is sent only at graceful EOF; on abort it is discarded.
        try:
            await owner._acknowledge_pipeline_chunk(update_stats=True)
        except Exception as pipeline_ex:
            MutyLogger.get_instance().exception(pipeline_ex)
            serialized = muty.log.exception_to_string(pipeline_ex)
            if serialized not in errors:
                errors.append(serialized)
            ex = ex or pipeline_ex

        if ex is None and not owner._chunk_pre_ingest_failed:
            try:
                final_chunk = owner._detach_ingestion_chunk(flt=flt)
                await owner._process_ingestion_chunk(
                    final_chunk,
                    wait_for_refresh=True,
                    update_stats=False,
                )
            except Exception as final_ex:
                MutyLogger.get_instance().exception(final_ex)
                serialized = muty.log.exception_to_string(final_ex)
                if serialized not in errors:
                    errors.append(serialized)
        else:
            owner._docs_buffer = []
            owner._records_processed_per_chunk = 0
            owner._records_failed_per_chunk = 0

        self._copy_ingestion_owner_state(owner)
        source_finished: bool = True
        disconnected: bool = False
        if self._raw_ingestion:
            # on raw ingestion, source is never done unless last chunk or canceled
            if self._last_raw_chunk:
                status = GulpRequestStatus.FAILED if errors else GulpRequestStatus.DONE
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
            # (configurable) update stats frequency on raw ingestion
            freq: int = (
                GulpConfig.get_instance().ingestion_raw_update_stats_chunk_frequency()
            )
            if self._raw_flush_count % freq != 0:
                return status

        final_status = status
        try:
            MutyLogger.get_instance().debug(
                "updating ingestion stats with: ingested=%d, skipped=%d, processed=%d, failed=%d, errors=%s, status=%s, source_finished=%r, ws_disconnected=%r",
                owner._stats_pending_ingested,
                owner._stats_pending_skipped,
                owner._stats_pending_processed,
                owner._stats_pending_failed,
                errors,
                status,
                source_finished,
                disconnected,
            )
            d: dict = await self._stats.update_ingestion_stats(
                self._sess,
                user_id=self._user_id,
                ws_id=self._ws_id,
                records_ingested=owner._stats_pending_ingested,
                records_skipped=owner._stats_pending_skipped,
                records_processed=owner._stats_pending_processed,
                records_failed=owner._stats_pending_failed,
                errors=errors,
                status=status,
                source_finished=source_finished,
                ws_disconnected=disconnected,
            )
            owner._stats_pending_ingested = 0
            owner._stats_pending_skipped = 0
            owner._stats_pending_processed = 0
            owner._stats_pending_failed = 0

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
            final_status = GulpRequestStatus(d["status"])
        except Exception:
            MutyLogger.get_instance().exception("error updating ingestion stats")
            final_status = GulpRequestStatus.FAILED

        if source_finished:
            try:
                event_errors = list(
                    dict.fromkeys([*getattr(self._stats, "errors", []), *errors])
                )
                await self._emit_source_finished_event(final_status, event_errors)
            except Exception:
                MutyLogger.get_instance().exception(
                    "failed to emit source-finished event"
                )

        return final_status

    async def update_final_stats_and_unload(
        self, flt: GulpIngestionFilter = None, ex: Exception = None
    ) -> GulpRequestStatus:
        """Finalize plugin stats and always release the plugin instance."""
        try:
            return await self.update_final_stats_and_flush(flt=flt, ex=ex)
        finally:
            await self.unload()

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
        run_post_init: bool = True,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        Load a plugin by name.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            cache_mode (GulpPluginCacheMode, optional): The cache mode. Defaults to GulpPluginCacheMode.DEFAULT.
            run_post_init (bool, optional): Whether to initialize runtime side effects. Defaults to True.
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
        internal_plugin_name = os.path.splitext(os.path.basename(path))[0]

        # check if we're allowed to load this plugin: if internal_plugin_name matches one of the patterns in the
        # disabled list, loading is disabled
        disabled_list: list[str] = GulpConfig.get_instance().plugin_disabled()
        if disabled_list:
            for pattern in disabled_list:
                if pattern.lower() in internal_plugin_name.lower():
                    raise FileNotFoundError(
                        f"loading plugin '{plugin}' is DISABLED by config!"
                    )

        # try to get plugin from cache
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

        route_count: int | None = None
        broadcast_types: set[str] | None = None
        app = None
        if run_post_init and extension and GulpProcess.get_instance().is_main_process():
            from gulp.api.server_api import GulpServer

            app = GulpServer.get_instance()._app
            if app is not None:
                route_count = len(app.router.routes)
            broadcast_types = set(GulpRedisBroker.get_instance().broadcast_types)

        try:
            if run_post_init:
                await p.post_init(**kwargs)
                await events.drain()
        except BaseException:
            if route_count is not None:
                del app.router.routes[route_count:]
            if broadcast_types is not None:
                broker_types = GulpRedisBroker.get_instance().broadcast_types
                broker_types.clear()
                broker_types.update(broadcast_types)
            try:
                await p.unload()
            except BaseException as unload_ex:
                MutyLogger.get_instance().exception(
                    "failed to unload plugin after post_init error: %s", unload_ex
                )
            finally:
                sys.modules.pop(module_name, None)
                if GulpProcess.get_instance().is_main_process():
                    try:
                        events.unsubscribe(p)
                        await events.drain()
                    except Exception as cleanup_ex:
                        MutyLogger.get_instance().exception(
                            "failed to remove plugin event subscriptions: %s",
                            cleanup_ex,
                        )
            raise

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

        from gulp.process import GulpProcess

        lower = self._lower_instance
        self._lower_instance = None
        try:
            await self._acknowledge_pipeline_chunk(update_stats=True)
            if lower and lower.module_name in sys.modules:
                await lower.unload()
        finally:
            try:
                self._normalized_timestamp_cache.clear()
                if GulpProcess.get_instance().is_main_process():
                    events.unsubscribe(self)
                    await events.drain()
            finally:
                sys.modules.pop(self.module_name, None)

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

            # return pyc or py file if found, preferring compiled bytecode.
            for ext in [".pyc", ".py"]:
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

        def _plugin_key(filename: str) -> str:
            return os.path.splitext(os.path.basename(filename))[0].lower()

        def _plugin_sort_key(path: str) -> tuple[str, int, str]:
            ext_rank = 0 if path.endswith(".pyc") else 1
            return (_plugin_key(path), ext_rank, path.lower())

        def _exists(l: list[GulpPluginEntry], filename: str) -> bool:
            # check if the plugin basename exists in the list
            key = _plugin_key(filename)
            for p in l:
                if _plugin_key(p.filename) == key:
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
            files = sorted(plugins + extensions, key=_plugin_sort_key)
            for f in files:
                if "__init__" in f or "__pycache__" in f or "/ui/" in f:
                    continue
                if not (f.endswith(".py") or f.endswith(".pyc")):
                    continue

                if "/extension/" in f:
                    extension = True
                else:
                    extension = False

                if extension_only and not extension:
                    continue

                try:
                    p = await GulpPluginBase.load(
                        f,
                        extension=extension,
                        cache_mode=GulpPluginCacheMode.IGNORE,
                        run_post_init=False,
                    )
                except Exception as ex:
                    MutyLogger.get_instance().exception(ex)
                    MutyLogger.get_instance().error("could not load plugin %s" % (f))
                    continue
                try:
                    if name is not None and name.lower() not in p.name.lower():
                        # filter by name
                        continue

                    if _exists(l, p.filename):
                        MutyLogger.get_instance().warning(
                            "skipping plugin %s, already exists in list (in another path)"
                            % (f)
                        )
                        continue

                    # got entry
                    try:
                        entry = GulpPluginEntry(
                            path=f,
                            display_name=p.display_name(),
                            type=p.type(),
                            desc=p.desc(),
                            regex=p.regex(),
                            filename=p.filename,
                            custom_parameters=p.custom_parameters(),
                            depends_on=p.depends_on(),
                            tags=p.tags(),
                            version=p.version(),
                            data=p.data(),
                            protected=hasattr(p, "_protected"),
                        )

                        l.append(entry)
                    except Exception as ex:
                        # something wrong in the value returned from plugin methods, cannot add ....
                        MutyLogger.get_instance().exception(ex)
                finally:
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
                if not await muty.file.exists(metadata_file):
                    MutyLogger.get_instance().warning(
                        "plugin metadata file %s not found, skipping plugin %s"
                        % (metadata_file, f)
                    )
                    continue

                metadata: bytes = await muty.file.read_file(metadata_file)
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
