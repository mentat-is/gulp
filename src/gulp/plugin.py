"""Gulp plugin base class and plugin utilities.
"""

import asyncio
import ipaddress
import json
import os
from abc import ABC, abstractmethod
from copy import deepcopy
from enum import StrEnum
from types import ModuleType
from typing import Any, Callable, Optional
import inspect
import muty.crypto
import muty.dynload
import muty.file
import muty.jsend
import muty.log
import muty.pydantic
import muty.string
import muty.time
from muty.log import MutyLogger
from opensearchpy import Field
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.context import GulpContext
from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpRequestStats,
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
from gulp.api.opensearch.query import (
    GulpQuery,
    GulpQueryHelpers,
    GulpQueryParameters,
    GulpQueryNoteParameters,
)
from gulp.api.opensearch.sigma import GulpPluginSigmaSupport, GulpQuerySigmaParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.ws_api import (
    GulpDocumentsChunkPacket,
    GulpIngestSourceDonePacket,
    GulpQueryDonePacket,
    GulpSharedWsQueue,
    GulpWsQueueDataType,
)
from gulp.config import GulpConfig
from gulp.structs import (
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
    """

    INGESTION = "ingestion"
    EXTENSION = "extension"
    EXTERNAL = "external"
    ENRICHMENT = "enrichment"


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
                    "sigma_support": [
                        muty.pydantic.autogenerate_model_example_by_class(
                            GulpPluginSigmaSupport
                        ),
                    ],
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
    sigma_support: Optional[list[GulpPluginSigmaSupport]] = Field(
        [],
        description="The supported backends/pipelines/output_formats.",
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


class GulpPluginCache:
    """
    Plugin cache singleton.
    """

    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpPluginCache":
        """
        returns the plugin cache instance.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._cache = {}

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
        if not GulpConfig.get_instance().plugin_cache_enabled():
            return
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
        if not GulpConfig.get_instance().plugin_cache_enabled():
            return None

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
        if not GulpConfig.get_instance().plugin_cache_enabled():
            return
        if name in self._cache:
            MutyLogger.get_instance().debug("removing plugin %s from cache" % (name))
            del self._cache[name]


class GulpPluginBase(ABC):
    """
    Base class for all Gulp plugins.
    """

    def __reduce__(self):
        """
        This method is automatically used by the pickle module to serialize the object when it is passed to the multiprocessing module.

        Returns:
            tuple: A tuple containing the callable, its arguments, and the object's state.
        """
        # load with ignore_cache=True, pickled=True
        extension = GulpPluginType.EXTENSION in self.type()
        return (
            GulpPluginBase.load_sync,
            (self.path, extension, True, True),
            self.__dict__,
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
        super().__init__()

        # tell if the plugin has been pickled by the multiprocessing module (internal)
        self._pickled = pickled

        # if set, this plugin have another plugin on top
        self._stacked = False

        # plugin file path
        self.path = path

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
        # plugin specific parameters (k: v, where k is one of the GulpPluginSpecificParams "name")
        self._custom_params: dict = {}
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

        # to have faster access to the plugin filename
        self.filename = os.path.basename(self.path)
        self.bare_filename = os.path.splitext(self.filename)[0]

        # to have faster access to the plugin name
        self.name = self.display_name()

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
        self._tot_ingested_in_source: int = 0

        # to keep track of ingested chunks
        self._chunks_ingested: int = 0

        # additional options
        self._ingestion_enabled: bool = True

        # enrichment during ingestion may be disabled also if _enrich_documents_chunk is implemented
        self._enrich_during_ingestion: bool = True
        self._enrich_index: str = None
        self._tot_enriched: int = 0

        self._note_parameters: GulpQueryNoteParameters = GulpQueryNoteParameters()
        self._external_plugin_params: GulpPluginParameters = GulpPluginParameters()
        self._plugin_params: GulpPluginParameters = GulpPluginParameters()

        # to minimize db requests to postgres to get context and source at every record
        self._ctx_cache = {}
        self._src_cache = {}
        self._operation: GulpOperation = None

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

        NOTE: after `_initialize` has been called these are accessible by the plugin through the `self._custom_params` dictionary using GulpPluginCustomParameter.name as the key.

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
        Returns a list of plugins this plugin depends on.
        """
        return []

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
        # MutyLogger.get_instance().debug(f"processing chunk of {len(data)} documents with plugin {self.name}")
        if not data:
            return 0, 0

        el = GulpOpenSearch.get_instance()
        skipped: int = 0
        ingested_docs: list[dict] = []

        # MutyLogger.get_instance().debug('flushing ingestion buffer, len=%d' % (len(self.buffer)))
        if self._ingestion_enabled:
            # perform ingestion, ingested_docs may be different from data in the end due to skipped documents
            skipped, ingestion_errors, ingested_docs = await el.bulk_ingest(
                self._ingest_index,
                data,
                flt=flt,
                wait_for_refresh=wait_for_refresh,
            )
            # print(json.dumps(ingested_docs, indent=2))
            if ingestion_errors > 0:
                """
                NOTE: errors here means something wrong with the format of the documents, and must be fixed ASAP.
                ideally, function should NEVER append errors and the errors total should be the same before and after this function returns (this function may only change the skipped total, which means some duplicates were found).
                """
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
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.DOCUMENTS_CHUNK,
                ws_id=self._ws_id,
                user_id=self._user_id,
                req_id=self._ws_id,
                data=data,
            )
            self._chunks_ingested += 1

            if self._note_parameters.create_notes and self._ingestion_enabled:
                # auto-create notes during external query with ingestion (this is a sigma query)
                await GulpNote.bulk_create_from_documents(
                    sess=self._sess,
                    user_id=self._user_id,
                    ws_id=self._ws_id,
                    req_id=self._req_id,
                    docs=data["docs"],
                    name=self._note_parameters.note_name,
                    tags=self._note_parameters.note_tags,
                    color=self._note_parameters.note_color,
                    glyph_id=self._note_parameters.note_glyph_id,
                )

        # check if the request is cancelled ()
        stats: GulpRequestStats = await GulpRequestStats.get_by_id(
            self._sess, self._req_id, throw_if_not_found=False
        )
        if stats and stats.status == GulpRequestStatus.CANCELED:
            self._req_canceled = True
            MutyLogger.get_instance().warning("_process_docs_chunk: request cancelled")

        return len(ingested_docs), skipped

    def _check_sigma_support(
        self, s_options: GulpQuerySigmaParameters
    ) -> tuple[str, str, str]:
        """
        check if the plugin supports the given sigma backend/pipeline/output_format

        if s_options.backend, pipeline or output_format are not set, the first supported is selected.

        Args:
            s_options (GulpQuerySigmaParameters): the sigma query parameters.

        Returns:
            tuple[str,str,str]: the selected backend, pipeline, output_format
        """
        if not self.sigma_support():
            raise ValueError(
                f"plugin {
                    self.name} does not support any sigma backends/pipelines/output_formats"
            )
        if not s_options:
            s_options = GulpQuerySigmaParameters()

        # if set, get the specified backend/pipeline/output_format.
        # either, get the first available for each
        if not s_options.backend:
            backend = self.sigma_support()[0].backends[0].name
            MutyLogger.get_instance().debug(
                f"no backend specified, using first supported: {backend}"
            )
        else:
            backend = s_options.backend

        if not s_options.pipeline:
            pipeline = self.sigma_support()[0].pipelines[0].name
            MutyLogger.get_instance().debug(
                f"no pipeline specified, using first supported: {pipeline}"
            )
        else:
            pipeline = s_options

        if not s_options.output_format:
            output_format = self.sigma_support()[0].output_formats[0].name
            MutyLogger.get_instance().debug(
                f"no output_format specified, using first supported: {
                    output_format}"
            )
        else:
            output_format = s_options.output_format

        # check support
        support_list = self.sigma_support()
        for s in support_list:
            backend_supported = False
            pipeline_supported = False
            output_format_supported = False
            for b in s.backends:
                if b.name == backend:
                    backend_supported = True
                    break
            for p in s.pipelines:
                if p.name == pipeline:
                    pipeline_supported = True
                    break
            for o in s.output_formats:
                if o.name == output_format:
                    output_format_supported = True
                    break

            if backend_supported and pipeline_supported and output_format_supported:
                return (backend, pipeline, output_format)

        raise ValueError(
            f"plugin {self.name} does not support the combination: backend={
                backend}, pipeline={pipeline}, output_format={output_format}"
        )

    def sigma_support(self) -> list[GulpPluginSigmaSupport]:
        """
        return backends/pipelines supported by the plugin (check pysigma on github for details)

        NOTE: "opensearch" backend and "dsl_lucene" output format are MANDATORY to query Gulp

        Returns:
            list[GulpPluginSigmaSupport]: the supported backends/pipelines/output_formats.
        """
        return []

    def sigma_convert(
        self,
        sigma: str,
        s_options: GulpQuerySigmaParameters,
    ) -> list[GulpQuery]:
        """
        convert a sigma rule specifically targeted to this plugin into a query for the target specified by backend/pipeline/output_format.

        Args:
            sigma (str): the sigma rule YAML
            s_options (GulpQuerySigmaParameters): the `backend`, `pipeline`, `output_format` to be used (`plugin` is ignored), the current plugin must implement them.
        Returns:
            list[GulpConvertedSigma]: one or more queries in the format specified by backend/pipeline/output_format.
        """
        raise NotImplementedError("not implemented!")

    async def _extract_context_and_source_from_doc(self, doc: dict) -> tuple[str, str]:
        """
        extract context and source (using cache to avoid too many queries) from the document, or set bogus values if not found.

        Args:
            doc (dict): the document to extract context and source from.

        Returns:
            tuple[str, str]: the context and source id.
        """

        ctx_id: str = None
        src_id: str = None
        ctx: GulpContext = None

        # get context and field
        record_context = self._custom_params.get("context_field")
        record_source = self._custom_params.get("source_field")
        # MutyLogger.get_instance().debug(f"record_context={record_context}, record_source={record_source}, ingest_index={self._ingest_index}")

        if not self._ingest_index:
            # no ingestion, get context and source from the record, setting bogus if parameters are not set
            self._context_id = doc.get(record_context, "default")
            self._source_id = doc.get(record_source, "default")
            return self._context_id, self._source_id

        if not self._operation:
            self._operation = await GulpOperation.get_by_id(
                self._sess, self._operation_id
            )

        if record_context is None:
            # add bogus context
            record_context = "default"
        if record_source is None:
            # add bogus source
            record_source = "default"

        src_cache_key = "%s-%s" % (record_context, record_source)
        if src_cache_key in self._ctx_cache:
            # we have them both
            ctx_id = self._ctx_cache[record_context]
            src_id = self._src_cache[src_cache_key]
            # MutyLogger.get_instance().debug(f"cache hit ctx & src: {ctx_id}, {src_id}")
            return ctx_id, src_id

        if record_context not in self._ctx_cache:
            # context cache miss
            ctx, _ = await self._operation.add_context(
                self._sess, self._user_id, record_context
            )
            self._ctx_cache[record_context] = ctx.id
            ctx_id = ctx.id
            MutyLogger.get_instance().warning(
                "cache miss, context=%s, ctx_id=%s" % (record_context, ctx_id)
            )
        else:
            # hit
            ctx_id = self._ctx_cache[record_context]
            # MutyLogger.get_instance().debug(f"cache hit ctx: {ctx_id}")

        if src_cache_key not in self._src_cache:
            # source cache miss
            src, _ = await ctx.add_source(self._sess, self._user_id, record_source)
            self._src_cache[src_cache_key] = src.id
            src_id = src.id
            MutyLogger.get_instance().warning(
                "cache miss, context=%s, ctx_id=%s source=%s, src_id=%s"
                % (record_context, src_id, record_source, src_id)
            )
        else:
            # hit
            src_id = self._src_cache[src_cache_key]
            # MutyLogger.get_instance().debug(f"cache hit src: {src_id}")

        return ctx_id, src_id

    async def query_external(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: Any,
        q: Any,
        q_options: GulpQueryParameters,
    ) -> tuple[int, int]:
        """
        query an external source and stream results, converted to gulpdocument dictionaries, to the websocket.

        optionally ingest them.

        Args:
            sess (AsyncSession): The database session.
            req_id (str): the request id
            ws_id (str): the websocket id
            user (str): the user performing the query
            index (Any): the index to query on the external source (format is plugin specific)
            q(Any): the query to perform, format is plugin specific
            q_options (GulpQueryParameters): additional query options, `q_options.external_parameters` must be set accordingly.

        Notes:
            - implementers must call super().query_external first
            - this function *MUST NOT* raise exceptions.

        Returns:
            tuple[int, int]: total documents found, total processed(ingested)
        Raises:
            ObjectNotFound: if no document is found.
        """
        MutyLogger.get_instance().debug(
            "GulpPluginBase.query_external: q=%s, index=%s, q_options=%s"
            % (q, index, q_options)
        )

        self._sess = sess
        self._ws_id = ws_id
        self._req_id = req_id
        self._user_id = user_id
        self._external_query = True
        self._enrich_during_ingestion = False

        # setup internal state to be able to call process_record as during ingestion
        self._stats = None

        if q_options.external_parameters.plugin_params.is_empty():
            q_options.external_parameters.plugin_params = GulpPluginParameters()

        # load any mapping set
        await self._initialize(q_options.external_parameters.plugin_params)

        self._operation_id = q_options.external_parameters.operation_id
        self._note_parameters = q_options.note_parameters
        self._external_plugin_params = q_options.external_parameters.plugin_params

        if q_options.external_parameters.ingest_index:
            # ingestion is enabled
            self._ingest_index = q_options.external_parameters.ingest_index
        else:
            # just query, no ingestion
            self._ingestion_enabled = False

        MutyLogger.get_instance().debug(
            "querying external source, plugin_params=%s"
            % (q_options.external_parameters.plugin_params)
        )

        return (0, 0)

    async def ingest_raw(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        chunk: list[dict],
        stats: GulpRequestStats = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:
        """
        ingest a chunk of GulpDocument dictionaries.

        Args:
            sess (AsyncSession): The database session.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation_id (str): id of the operation on collab database.
            context_id (str): id of the context on collab database.
            source_id (str): id of the source on collab database.
            chunk (list[dict]): The chunk of documents to ingest.
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
        self._context_id = context_id
        self._ingest_index = index
        self._source_id = source_id
        self._plugin_params = plugin_params or GulpPluginParameters()
        MutyLogger.get_instance().debug(
            f"ingesting raw source_id={source_id}, num documents={len(chunk)}, plugin {self.name}, user_id={user_id}, operation_id={
                operation_id}, context_id={context_id}, index={index}, ws_id={ws_id}, req_id={req_id}"
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)
        return GulpRequestStatus.ONGOING

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        """
        to be implemented in a plugin to enrich a chunk of documents.

        NOTE: do not call this function directly:
            this is called by the engine right before ingesting a chunk of documents in _flush_buffer()
            also, this is also called by the engine when enriching documents on-demand through the enrich_documents function.

        Args:
            docs (list[dict]): the GulpDocuments as dictionaries, to be enriched
            kwargs: additional keyword arguments
        """
        return docs

    async def _enrich_documents_chunk_wrapper(self, docs: list[dict], **kwargs):
        last = kwargs.get("last", False)

        # call the plugin function
        docs = await self._enrich_documents_chunk(docs, **kwargs)
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
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.DOCUMENTS_CHUNK,
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
            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.ENRICH_DONE,
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
        index: str,
        flt: GulpQueryFilter,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> None:
        """
        to be implemented in a plugin to enrich a chunk of GulpDocuments dictionaries on-demand.

        the resulting documents will be streamed to the websocket `ws_id` as GulpDocumentsChunkPacket.

        usually, this fun
        Args:
            sess (AsyncSession): The database session.
            user_id (str): The user performing the ingestion (id on collab database)
            req_id (str): The request ID.
            ws_id (str): The websocket ID to stream on
            index (str): the index to query
            flt(GulpQueryFilter): a filter to restrict the documents to enrich
            plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.
            kwargs: additional keyword arguments:
                - rq (dict): the raw query to use instead of the filter

        NOTE: implementers must implement _enrich_documents_chunk, call self._parse_custom_parameters and then super().enrich_documents
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment" % (self.name)
            )

        self._user_id = user_id
        self._req_id = req_id
        self._ws_id = ws_id
        self._enrich_index = index
        self._plugin_params = plugin_params or GulpPluginParameters()

        await self._initialize(plugin_params=plugin_params)

        # check if the caller provided a raw query to be used
        rq = kwargs.get("rq", None)
        q: dict = {}
        if rq:
            # raw query provided by the caller
            q = rq
        else:
            if not flt:
                # match all query
                q = {"query": {"match_all": {}}}
            else:
                # convert filter
                q = flt.to_opensearch_dsl()

        # force return all fields
        q_options = GulpQueryParameters(fields="*")
        await GulpQueryHelpers.query_raw(
            sess=sess,
            user_id=self._user_id,
            req_id=self._req_id,
            ws_id=self._ws_id,
            q=q,
            index=index,
            q_options=q_options,
            callback_chunk=self._enrich_documents_chunk_wrapper,
            callback_chunk_args={"done_type": GulpWsQueueDataType.ENRICH_DONE},
        )

    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        """
        to be implemented in a plugin to enrich a single document on-demand.

        Args:
            sess (AsyncSession): The database session.
            doc (dict): the document to enrich
            index (str): the index to query
            plugin_params (GulpPluginParameters): the plugin parameters
        Returns:
            dict: the enriched document

        NOTE: implementers must implement _enrich_documents_chunk, call self._parse_custom_parameters and then super().enrich_single_document
        """
        if inspect.getmodule(self._enrich_documents_chunk) == inspect.getmodule(
            GulpPluginBase._enrich_documents_chunk
        ):
            raise NotImplementedError(
                "plugin %s does not support enrichment" % (self.name)
            )

        await self._initialize(plugin_params=plugin_params)

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
        self._plugin_params = plugin_params or GulpPluginParameters()

        MutyLogger.get_instance().debug(
            f"ingesting file source_id={source_id}, file_path={file_path}, original_file_path={original_file_path}, plugin {self.name}, user_id={user_id}, operation_id={operation_id}, \
                plugin_params={plugin_params}, flt={flt}, context_id={context_id}, index={index}, ws_id={ws_id}, req_id={req_id}"
        )

        # initialize
        await self._initialize(plugin_params=plugin_params)
        return GulpRequestStatus.ONGOING

    async def load_plugin(
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

        Returns:
            GulpPluginBase: the loaded plugin.
        """
        if not plugin_params:
            plugin_params = GulpPluginParameters()

        lower = await GulpPluginBase.load(plugin)

        # initialize private fields
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
        self, plugin: str, ignore_cache: bool = False, *args, **kwargs
    ) -> "GulpPluginBase":
        """
        setup the caller plugin as the "lower" plugin in a stack.

        the engine, when processing records, will:

        - for any record, call the running plugin _record_to_gulp_document function
            - if a plugin is stacked above, will call the upper plugin _record_to_gulp_document function
        - when flushing a chunk of documents to opensearch, call the running plugin _enrich_documents function
            - if a plugin is stacked above, will call the upper plugin _enrich_documents function

        Args:
            plugin (str): the plugin to load
            ignore_cache (bool, optional): ignore cache. Defaults to False.
            *args: additional arguments to pass to the plugin constructor.
            **kwargs: additional keyword arguments to pass to the plugin constructor.
        Returns:
            PluginBase: the loaded plugin
        """
        p = await GulpPluginBase.load(
            plugin, extension=False, ignore_cache=ignore_cache, *args, **kwargs
        )

        # set the upper plugin as stacked, so it can call our (lower) functions
        p._upper_record_to_gulp_document_fun = self._record_to_gulp_document
        p._upper_enrich_documents_chunk_fun = self._enrich_documents_chunk
        p._upper_instance = self

        # set the lower plugin as stacked
        p._stacked = True
        return p

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
            """
            MutyLogger.get_instance().debug(
                "creating new doc with base=\n%s\ndata=\n%s\nnew_doc=%s"
                % (
                    json.dumps(base_doc_dump, indent=2),
                    json.dumps(new_doc_data, indent=2),
                    json.dumps(new_doc.model_dump(), indent=2),
                )
            )
            """
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

        Args:
            record (any): the record to convert.
                NOTE: in a stacked plugin, this method always receives `record` ad `dict` (GulpDocument.model_dump()) and returns the same `dict` after processing.
            record_idx (int): the index of the record in the source
            kwargs: additional keyword arguments
        Returns:
            GulpDocument: the GulpDocument

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
            kwargs: additional keyword arguments
        Returns:
            list[dict]: zero or more GulpDocument dictionaries

        NOTE: called by the engine, do not call this function directly.
        """

        # process documents with our record_to_gulp_document
        doc = await self._record_to_gulp_document(record, record_idx, **kwargs)
        if not doc:
            return []

        docs = self._finalize_process_record(doc)

        if self._upper_record_to_gulp_document_fun:
            # postprocess documents with the stacked plugin
            for i, doc in enumerate(docs):
                docs[i] = await self._upper_record_to_gulp_document_fun(
                    doc, record_idx, **kwargs
                )

        return docs

    def selected_mapping(self) -> GulpMapping:
        """
        Returns the selected (self._mapping_id) mapping or an empty one.

        NOTE: just a shortcut for self._mappings.get(self._mapping_id, GulpMapping())

        Returns:
            GulpMapping: The selected mapping.
        """
        return self._mappings.get(self._mapping_id, GulpMapping())

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

        def _try_map_ecs(
            fields_mapping: GulpMappingField, d: dict, source_value: Any
        ) -> dict:
            # fields are added to d if found

            # print(fields_mapping)
            mapping = fields_mapping.ecs
            if isinstance(mapping, str):
                # single mapping
                mapping = [mapping]

            if mapping:
                # source key is mapped, add the mapped key to the document
                for k in mapping:
                    kk, vv = self._type_checks(k, source_value)
                    if vv:
                        d[kk] = vv
            else:
                # unmapped key
                d[
                    f"{GulpOpenSearch.UNMAPPED_PREFIX}.{
                    source_key}"
                ] = source_value

            return d

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
            return {f"{GulpOpenSearch.UNMAPPED_PREFIX}.{source_key}": source_value}

        d = {}
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
            mapped = _try_map_ecs(fields_mapping, d, source_value)
            for k, _ in mapped.items():
                extra[k] = None

            self._extra_docs.append(extra)

            # we also add this key to the main document
            return mapped

        return _try_map_ecs(fields_mapping, d, source_value)

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
            kwargs: additional keyword arguments.
        Returns:
            None
        """
        if self._external_query:
            # external query, documents have been already filtered by the query
            flt = None

        ingestion_buffer_size = GulpConfig.get_instance().documents_chunk_size()
        if (
            self._plugin_params.override_chunk_size
            or self._external_plugin_params.override_chunk_size
        ):
            # using the provided chunk size instead
            if self._external_query:
                ingestion_buffer_size = self._external_plugin_params.override_chunk_size
            else:
                ingestion_buffer_size = self._plugin_params.override_chunk_size

        self._extra_docs = []

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

        # ingest record (may have generated multiple documents)
        for d in docs:
            self._docs_buffer.append(d)
            if len(self._docs_buffer) >= ingestion_buffer_size:

                # flush chunk to opensearch (ingest and stream to ws)
                ingested, skipped = await self._flush_buffer(
                    flt, wait_for_refresh, **kwargs
                )
                if self._req_canceled:
                    raise RequestCanceledError("request canceled!")

                # check threshold
                failure_threshold = (
                    GulpConfig.get_instance().ingestion_evt_failure_threshold()
                )
                if failure_threshold > 0 and (
                    self._tot_skipped_in_source >= failure_threshold
                    or self._tot_failed_in_source >= failure_threshold
                ):
                    # abort this source
                    raise SourceCanceledError(
                        "ingestion per-source failure threshold reached (tot_skipped=%d, tot_failed=%d, threshold=%d), canceling source..."
                        % (
                            self._tot_skipped_in_source,
                            self._tot_failed_in_source,
                            failure_threshold,
                        )
                    )

                if self._stats:
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
                    d = dict(
                        source_id=self._source_id,
                        records_skipped=skipped,
                        records_ingested=ingested,
                        records_processed=self._records_processed_per_chunk,
                        records_failed=self._records_failed_per_chunk,
                    )
                    await self._stats.update(
                        self._sess, d=d, ws_id=self._ws_id, user_id=self._user_id
                    )

                # reset buffer and counters inside the plugin run
                self._docs_buffer = []
                self._records_processed_per_chunk = 0
                self._records_failed_per_chunk = 0

    def _parse_custom_parameters(self, plugin_params: GulpPluginParameters) -> None:
        """
        parse custom plugin parameters in plugin_params.custom_parameters, they must correspond to the plugin defined ones.

        Args:
            plugin_params (GulpPluginParameters): the plugin parameters.
        """
        if not plugin_params:
            plugin_params = GulpPluginParameters()

        # check any passed custom parameter against the plugin defined ones
        defined_custom_params = self.custom_parameters()

        for p in defined_custom_params:
            k = p.name
            if p.required and k not in plugin_params.custom_parameters:
                raise ValueError(
                    "required plugin parameter '%s' not found in plugin_params=%s"
                    % (k, plugin_params)
                )
            if k not in self._custom_params:
                self._custom_params[k] = plugin_params.custom_parameters.get(
                    k, p.default_value
                )
                MutyLogger.get_instance().debug(
                    "setting plugin custom parameter: %s=%s"
                    % (k, self._custom_params[k])
                )

    async def _initialize(
        self,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        """
        initialize mapping and plugin custom parameters

        this sets self._mappings, self._mapping_id, self._custom_params (the plugin specific parameters), self._index_type_mapping

        Args:
            plugin_params (GulpPluginParameters, optional): plugin parameters. Defaults to None.

        Raises:
            ValueError: if mapping_id is set but mappings/mapping_file is not.
            ValueError: if a specific parameter is required but not found in plugin_params.

        """

        async def _setup_mapping(plugin_params: GulpPluginParameters) -> None:
            if plugin_params.mappings:
                # mappings dict provided
                mappings_dict = {
                    k: GulpMapping.model_validate(v)
                    for k, v in plugin_params.mappings.items()
                }
                MutyLogger.get_instance().debug(
                    'using plugin_params.mappings="%s"' % plugin_params.mappings
                )
                self._mappings = mappings_dict
            else:
                if plugin_params.mapping_file:
                    # load from file
                    mapping_file = plugin_params.mapping_file
                    MutyLogger.get_instance().debug(
                        "using plugin_params.mapping_file=%s"
                        % (plugin_params.mapping_file)
                    )
                    mapping_file_path = (
                        GulpConfig.get_instance().build_mapping_file_path(mapping_file)
                    )
                    f = await muty.file.read_file_async(mapping_file_path)
                    js = json.loads(f)
                    if not js:
                        raise ValueError(
                            "mapping file %s is empty!" % (mapping_file_path)
                        )

                    gmf: GulpMappingFile = GulpMappingFile.model_validate(js)
                    self._mappings = gmf.mappings

            if plugin_params.mapping_id:
                # mapping id provided
                self._mapping_id = plugin_params.mapping_id
                MutyLogger.get_instance().debug(
                    "using plugin_params.mapping_id=%s" % (plugin_params.mapping_id)
                )

            # checks
            if not self._mappings and self._mapping_id:
                raise ValueError("mapping_id is set but mappings/mapping_file is not!")
            if not self._mappings and not self._mapping_id:
                MutyLogger.get_instance().warning(
                    "mappings/mapping_file and mapping_id are both None/empty!"
                )
                self._mappings = {"default": GulpMapping(fields={})}

            # ensure mapping_id is set
            self._mapping_id = self._mapping_id or list(self._mappings.keys())[0]

            MutyLogger.get_instance().debug("mapping_id=%s" % (self._mapping_id))

            # now go for additional mappings
            if not plugin_params.mappings and plugin_params.additional_mapping_files:
                MutyLogger.get_instance().debug(
                    "loading additional mapping files/id: %s ..."
                    % (plugin_params.additional_mapping_files)
                )
                for f in plugin_params.additional_mapping_files:
                    # each entry is a tuple (file, mapping_id)
                    additional_mapping_file_path = (
                        GulpConfig.get_instance().build_mapping_file_path(f[0])
                    )
                    additional_mapping_id = f[1]
                    f = await muty.file.read_file_async(additional_mapping_file_path)
                    js = json.loads(f)
                    if not js:
                        raise ValueError(
                            "additional mapping file %s is empty!"
                            % (additional_mapping_file_path)
                        )

                    a_gmf: GulpMappingFile = GulpMappingFile.model_validate(js)

                    # update mappings
                    MutyLogger.get_instance().debug(
                        "adding additional mappings from %s.%s to %s '%s' ..."
                        % (
                            additional_mapping_file_path,
                            additional_mapping_id,
                            self.filename,
                            self._mapping_id,
                        )
                    )

                    main_mapping = self.selected_mapping()
                    add_mapping = a_gmf.mappings[additional_mapping_id]
                    for k, v in add_mapping.fields.items():
                        main_mapping.fields[k] = v

                    self._mappings[self._mapping_id] = main_mapping

        # ensure we have a plugin_params object
        if not plugin_params:
            plugin_params = GulpPluginParameters()
        MutyLogger.get_instance().debug(
            "---> _initialize: plugin=%s, plugin_params=%s"
            % (
                self.filename,
                json.dumps(plugin_params.model_dump(), indent=2),
            )
        )

        # check any custom plugin paramter
        self._parse_custom_parameters(plugin_params)
        if (
            GulpPluginType.EXTENSION in self.type()
            or GulpPluginType.ENRICHMENT in self.type()
        ):
            MutyLogger.get_instance().debug(
                "extension/enrichment plugin, no mappings needed"
            )
            return

        # set mappings
        await _setup_mapping(plugin_params)

        if self._stacked:
            # if we are in a stacked plugin, and we are the lower
            # pass mappings we are called with to the upper, so
            # the upper can apply them
            if not self._upper_instance._mapping_id:
                self._upper_instance._mapping_id = self._mapping_id
            if not self._upper_instance._mappings:
                self._upper_instance._mappings = self._mappings

        # initialize index types k,v mapping from opensearch
        self._index_type_mapping = (
            await GulpOpenSearch.get_instance().datastream_get_key_value_mapping(
                self._ingest_index
            )
        )
        MutyLogger.get_instance().debug(
            "got index type mappings with %d entries" % (len(self._index_type_mapping))
        )
        # MutyLogger.get_instance().debug("---> finished _initialize: plugin=%s, mappings=%s" % ( self.filename, self._mappings))

    def _type_checks(self, k: str, v: Any) -> tuple[str, Any]:
        """
        check the type of a value and convert it if needed.

        Args:
            k (str): the key
            v (any): the value

        Returns:
            tuple[str, any]: the key and the value
        """
        index_type = self._index_type_mapping.get(k)
        if not index_type:
            # MutyLogger.get_instance().warning("key %s not found in index_type_mapping" % (k))
            # return an unmapped key, so it is guaranteed to be a string
            # k = f{GulpOpenSearch.UNMAPPED_PREFIX}.{k}
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
        return self._stats.status

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
                    self._docs_buffer = await self._upper_enrich_documents_chunk_fun(
                        self._docs_buffer, **kwargs
                    )
            except Exception as ex:
                pass

        # then finally ingest the chunk
        ingested, skipped = await self._ingest_chunk_and_or_send_to_ws(
            self._docs_buffer,
            flt,
            wait_for_refresh,
            all_fields_on_ws=self._external_query,
        )

        self._tot_failed_in_source += self._records_failed_per_chunk
        self._tot_skipped_in_source += skipped
        self._tot_ingested_in_source += ingested

        """
        if wait_for_refresh:
            # update index type mapping too
            el = GulpOpenSearch.get_instance()
            self._index_type_mapping = await el.datastream_get_key_value_mapping(
                self._index
            )
            MutyLogger.get_instance().debug(
                "got index type mappings with %d entries"
                % (len(self._index_type_mapping))
            )
        """
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
            self._req_canceled = True
        else:
            self._is_source_failed = True

        if not isinstance(err, str):
            # , with_full_traceback=True)
            err = muty.log.exception_to_string(err)
        MutyLogger.get_instance().error(
            "SOURCE FAILED: source=%s, ex=%s, processed in this source=%d, canceled=%r, failed=%r, ingestion=%r"
            % (
                self._file_path,
                err,
                self._records_processed_per_chunk,
                self._req_canceled,
                self._is_source_failed,
                self._ingestion_enabled,
            )
        )

        err = "source=%s, %s" % (self._file_path, err)
        self._source_error = err

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
                self._file_path,
                len(self._docs_buffer),
                self._stats.status if self._stats else GulpRequestStatus.DONE,
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

        if not self._stats and not self._ingestion_enabled:
            # this also happens on query external
            self._sess = None
            return

        if self._ws_id:
            # send ingest_source_done packet on ws
            if self._is_source_failed:
                status = GulpRequestStatus.FAILED
            elif self._req_canceled:
                status = GulpRequestStatus.CANCELED
            else:
                status = GulpRequestStatus.DONE

            GulpSharedWsQueue.get_instance().put(
                type=GulpWsQueueDataType.INGEST_SOURCE_DONE,
                ws_id=self._ws_id,
                user_id=self._user_id,
                operation_id=self._operation_id,
                data=GulpIngestSourceDonePacket(
                    source_id=self._source_id,
                    context_id=self._context_id,
                    req_id=self._req_id,
                    docs_ingested=self._tot_ingested_in_source,
                    docs_skipped=self._tot_skipped_in_source,
                    docs_failed=self._tot_failed_in_source,
                    status=status,
                ),
            )

        if self._stats:
            d = dict(
                source_failed=1 if self._is_source_failed else 0,
                source_processed=1,
                source_id=self._source_id,
                records_ingested=ingested,
                records_skipped=skipped,
                records_failed=self._records_failed_per_chunk,
                records_processed=self._records_processed_per_chunk,
                error=self._source_error,
            )
            try:
                await self._stats.update(
                    self._sess,
                    d,
                    ws_id=self._ws_id,
                    user_id=self._user_id,
                )
            except RequestCanceledError as ex:
                MutyLogger.get_instance().warning("request canceled, source_done!")
            finally:
                self._sess = None

    async def _query_external_done(
        self, query_name: str, hits: int, error: Exception | str = None
    ) -> None:
        """
        for external sources, called by the engine when the query is done to send a GulpQueryDonePacket.

        Args:
            query_name (str): The name of the query.
            hits (int): The number of hits.
            error (Exception | str, optional): The error that caused the query to fail. Defaults to None
        """
        MutyLogger.get_instance().debug(
            "EXTERNAL QUERY DONE: query %s, %d hits, error=%s"
            % (
                query_name,
                hits,
                error,
            )
        )

        # any error sets status to failed
        if not error:
            # but also 0 hits
            status = GulpRequestStatus.DONE if hits > 0 else GulpRequestStatus.FAILED
        else:
            status = GulpRequestStatus.FAILED

        if error and isinstance(error, Exception):
            error = muty.log.exception_to_string(error, True)

        # send a GulpQueryDonePacket
        p = GulpQueryDonePacket(
            status=status,
            total_hits=hits,
            name=query_name,
            error=error,
        )
        GulpSharedWsQueue.get_instance().put(
            type=GulpWsQueueDataType.QUERY_DONE,
            ws_id=self._ws_id,
            user_id=self._user_id,
            req_id=self._req_id,
            data=p.model_dump(exclude_none=True),
        )

    @staticmethod
    def load_sync(
        plugin: str,
        extension: bool = False,
        ignore_cache: bool = False,
        *args,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        same as calling load, but synchronous.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            ignore_cache (bool, optional): Whether to ignore the cache. Defaults to False.
            *args: Additional arguments (args[0]: pickled).
            **kwargs: Additional keyword arguments.
        """
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # if there's already a running event loop, run the coroutine in a separate thread
            from gulp.process import GulpProcess

            executor = GulpProcess.get_instance().thread_pool
            future = executor.submit(
                asyncio.run,
                GulpPluginBase.load(plugin, extension, ignore_cache, *args, **kwargs),
            )
            return future.result()

        # either, create a new event loop to run the coroutine
        return loop.run_until_complete(
            GulpPluginBase.load(plugin, extension, ignore_cache, *args, **kwargs)
        )

    @staticmethod
    async def load(
        plugin: str,
        extension: bool = False,
        ignore_cache: bool = False,
        *args,
        **kwargs,
    ) -> "GulpPluginBase":
        """
        Load a plugin by name.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            ignore_cache (bool, optional): Whether to ignore the cache. Defaults to False.
            *args: Additional arguments (args[0]: pickled).
            **kwargs: Additional keyword arguments.
        """
        # this is set in __reduce__(), which is called when the plugin is pickled(=loaded in another process)
        # pickled=True: running in worker
        # pickled=False: running in main process
        pickled = args[0] if args else False

        # get plugin full path by name
        path = GulpPluginBase.path_from_plugin(
            plugin, extension, raise_if_not_found=True
        )
        bare_name = os.path.splitext(os.path.basename(path))[0]
        m = GulpPluginCache.get_instance().get(bare_name)
        if ignore_cache and m:
            MutyLogger.get_instance().warning(
                "ignoring cache for plugin %s" % (bare_name)
            )
            m = None
            GulpPluginCache.get_instance().remove(bare_name)
        if m:
            # return from cache
            return m.Plugin(path, pickled=pickled, **kwargs)

        if extension:
            module_name = f"gulp.plugins.extension.{bare_name}"
        else:
            module_name = f"gulp.plugins.{bare_name}"

        # load from file
        m = muty.dynload.load_dynamic_module_from_file(module_name, path)
        MutyLogger.get_instance().debug(
            f"loading plugin m={m}, pickled={pickled}, kwargs={kwargs}"
        )
        p: GulpPluginBase = m.Plugin(path, pickled=pickled, **kwargs)
        MutyLogger.get_instance().debug(f"LOADED plugin m={m}, p={p}, name()={p.name}")
        if not ignore_cache:
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
        # try:
        #    if _sess:
        #        await self._sess.close()
        # finally:
        self._sess = None
        self._stats = None
        self._mappings.clear()
        self._mappings = None
        self._custom_params.clear()
        self._custom_params = None
        self._index_type_mapping.clear()
        self._index_type_mapping = None
        self._upper_record_to_gulp_document_fun = None
        self._upper_enrich_documents_chunk_fun = None
        self._upper_instance = None
        self._docs_buffer.clear()
        self._docs_buffer = None
        self._extra_docs.clear()
        self._extra_docs = None
        self._extra_docs: list[dict]
        self._note_parameters = None
        self._external_plugin_params = None
        self._plugin_params = None
        if GulpConfig.get_instance().plugin_cache_enabled():
            # do not unload if cache is enabled
            return

        MutyLogger.get_instance().debug("unloading plugin: %s" % (self.name))
        GulpPluginCache.get_instance().remove(self.name)

    @staticmethod
    def path_from_plugin(
        plugin: str, is_extension: bool = False, raise_if_not_found: bool = False
    ) -> str:
        """
        Get the path of a plugin.

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
            raise FileNotFoundError(f"plugin {plugin} not found in {p}!")
        return p

    @staticmethod
    async def list(
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
                if "__init__" in f or "__pycache__" in f:
                    continue

                if "/extension/" in f:
                    extension = True
                else:
                    extension = False

                if extension_only and not extension:
                    continue

                try:
                    p = await GulpPluginBase.load(
                        f, extension=extension, ignore_cache=True
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

                entry = GulpPluginEntry(
                    path=f,
                    display_name=p.display_name(),
                    type=p.type(),
                    desc=p.desc(),
                    filename=p.filename,
                    sigma_support=p.sigma_support(),
                    custom_parameters=p.custom_parameters(),
                    depends_on=p.depends_on(),
                    tags=p.tags(),
                    version=p.version(),
                    data=p.data(),
                )

                l.append(entry)
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
