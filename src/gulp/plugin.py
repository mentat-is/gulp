"""Gulp plugin base class and plugin utilities.
"""

import ipaddress
import json
import os
from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable
from copy import copy
import muty.crypto
import muty.dynload
import muty.file
import muty.jsend
import muty.string
import muty.time

from gulp import config
from gulp import utils as gulp_utils
from gulp.api import elastic_api
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.elastic.structs import (
    GulpDocument,
    GulpIngestionFilter,
    GulpQueryFilter,
)
from gulp.api.mapping.models import (
    GulpMappingField,
    GulpMapping,
    GulpMappingFile,
)
from gulp.defs import (
    GulpEventFilterResult,
    GulpLogLevel,
    GulpPluginType,
)
from gulp.plugin_internal import GulpPluginSpecificParams, GulpPluginGenericParams
from gulp.utils import logger


class GulpPluginCache:
    """
    Plugin cache singleton.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self.cache = {}    
    
    def clear(self):
        """
        Clear the cache.
        """
        if not config.plugin_cache_enabled():
            return

        self._cache = {}

    def add(self, plugin: ModuleType, name: str) -> None:
        """
        Add a plugin to the cache.

        Args:
            plugin (PluginBase): The plugin to add to the cache.
            name (str): The name of the plugin.
        """
        if not config.plugin_cache_enabled():
            return
        if not name in self._cache:
            logger().debug("adding plugin %s to cache" % (name))
            self._cache[name] = plugin

    def get(self, name: str) -> ModuleType:
        """
        Get a plugin from the cache.

        Args:
            name (str): The name of the plugin to get.
        Returns:
            PluginBase: The plugin if found in the cache, otherwise None.
        """        
        if not config.plugin_cache_enabled():
            return None
        
        p = self._cache.get(name, None)
        if p:
            logger().debug("found plugin %s in cache !" % (name))
        return p

    def remove(self, name: str):
        """
        Remove a plugin from the cache.

        Args:
            name (str): The name of the plugin to remove from the cache.
        """
        if not config.plugin_cache_enabled():
            return
        if name in self._cache:
            logger().debug("removing plugin %s from cache" % (name))
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
        return (GulpPluginBase.load, (self.path, extension, True, True), self.__dict__)

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
        # plugin file path
        self.path = path
    
        #
        # the following, if available, are stored in the plugin instance at the query/ingest entrypoint
        #
        # for ingestion, the mappings to apply
        self._mappings: dict[str, GulpMapping] = {}
        # for ingestion, the key in the mappings dict to be used
        self._mapping_id: str = None    
        # plugin specific parameters (k: v, where k is one of the GulpPluginSpecificParams "name") 
        self._specific_params: dict = {}
        # calling user
        self._user: str = None
        # current gulp operation
        self._operation: str = None
        # current gulp context
        self._context: str = None
        # opensearch index to operate on
        self._index: str = None
        # websocket to stream data to
        self._ws_id: str = None
        # current request id
        self._req_id: str = None        
        # current log file path
        self._log_file_path: str = None

        # for ingestion, the lower plugin record_to_gulp_document function to call (if this is a stacked plugin on top of another)
        self._lower_record_to_gulp_document_fun: Callable = None

        s = os.path.basename(self.path)
        s = os.path.splitext(s)[0]
        # to have faster access to the plugin file name (without ext)
        self.plugin_file = s

        # to have faster access to the plugin name
        self.name = self.display_name()

        # to bufferize gulpdocuments
        self._docs_buffer: list[dict] = []

        # to keep track of processed/skipped/failed records
        self._records_skipped = 0
        self._records_failed = 0
        self._records_processed = 0

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

    def version(self) -> str:
        """
        Returns plugin version.
        """
        return "1.0"

    def desc(self) -> str:
        """
        Returns a description of the plugin.
        """
        return ""

    def specific_params(self) -> list[GulpPluginSpecificParams]:
        """
        if any, returns a list of plugin specific parameters.
        """
        return []

    def depends_on(self) -> list[str]:
        """
        Returns a list of plugin "name" this plugin depends on.
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

    async def query_sigma(
        self,
    ) -> tuple[int, GulpRequestStatus]:
        raise NotImplementedError("not implemented!")

    async def query_external(
        self,
        operation_id: int,
        client_id: int,
        user_id: int,
        username: str,
        ws_id: str,
        req_id: str,
        plugin_params: GulpPluginGenericParams,
        flt: GulpQueryFilter,
    ) -> tuple[int, GulpRequestStatus]:
        """
        used in query plugins to query data directly from external sources.

        Args:
            operation_id (int): operation ID
            client_id (int): client ID
            user_id (int): user ID performing the query
            username (str): username performing the query
            ws_id (str): websocket ID to stream the returned data to
            req_id (str): request ID
            plugin_params (GulpPluginParams, optional): plugin parameters, including i.e. in GulpPluginParams.extra the login/pwd/token to connect to the external source, plugin dependent.
            flt (GulpQueryFilter): query filter (will be converted to the external source query format)
            options (GulpQueryOptions, optional): query options, i.e. to limit the number of returned records. Defaults to None.
                due to the nature of query plugins, not all options may be supported (i.e. limit, offset, ...) and notes creation is always disabled.
        Returns:
            tuple[int, GulpRequestStatus]: the number of records returned and the status of the query.
        """
        raise NotImplementedError("not implemented!")

    async def query_external_single(
        self,
        plugin_params: GulpPluginGenericParams,
        event: dict,
    ) -> dict:
        """
        used in query plugins to query a single **full** event from external sources.

        Args:
            plugin_params (GulpPluginParams, optional): plugin parameters, including i.e. in GulpPluginParams.extra the login/pwd/token to connect to the external source, plugin dependent.
            event (dict): the event to query for, i.e. as returned by the `query` method.

        Returns:
            dict: the event found
        """
        raise NotImplementedError("not implemented!")

    async def ingest_raw(
        self,
        req_id: str,
        ws_id: str,
        user: str,
        index: str,
        operation: str,
        context: str,
        data: list[dict]|bytes,
        raw: bool=False,
        log_file_path: str=None,
        plugin_params: GulpPluginGenericParams = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        """
        Ingests a file using the plugin.

        Args:
            req_id (str): The request ID.
            ws_id (str): The websocket ID.
            user (str): The user performing the ingestion.
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation (str): The operation.
            context (str): The context.
            data (list[dict]|bytes): this may be an array of already processed GulpDocument dictionaries, or a raw buffer.
            raw (bool, optional): if True, data is a raw buffer. Defaults to False (data is a list of GulpDocument dictionaries).
            plugin_params (GulpPluginParams, optional): The plugin parameters. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.

        Returns:
            GulpRequestStatus: The status of the ingestion.

        Notes:
            - implementers must call super().ingest_raw first.<br>
            - this function *MUST NOT* raise exceptions.
        """
        self._ws_id = ws_id
        self._req_id = req_id
        self._user = user
        self._operation = operation
        self._context = context
        self._index = index
        self._log_file_path = log_file_path        
        return GulpRequestStatus.ONGOING

    async def ingest_file(
        self,
        req_id: str,
        ws_id: str,
        user: str,
        index: str,
        operation: str,
        context: str,
        log_file_path: str,
        plugin_params: GulpPluginGenericParams = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        """
        Ingests a file using the plugin.

        Args:
            req_id (str): The request ID.
            ws_id (str): The websocket ID.
            user (str): The user performing the ingestion.
            index (str): The name of the target opensearch/elasticsearch index or datastream.
            operation (str): The operation.
            context (str): The context.
            log_file_path (str): The path to the log file.
            plugin_params (GulpPluginParams, optional): The plugin parameters. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.

        Returns:
            GulpRequestStatus: The status of the ingestion.

        Notes:
            - implementers must call super().ingest_file first.<br>
            - this function *MUST NOT* raise exceptions.
        """
        self._ws_id = ws_id
        self._req_id = req_id
        self._user = user
        self._operation = operation
        self._context = context
        self._index = index
        self._log_file_path = log_file_path        
        return GulpRequestStatus.ONGOING

    def _load_lower_plugin(
        self, plugin: str, ignore_cache: bool = False, *args, **kwargs
    ) -> "GulpPluginBase":
        """
        in a stacked plugin, load the lower plugin and set the _lower_record_to_gulp_document_fun to the lower plugin record_to_gulp_document function.

        Args:
            plugin (str): the plugin to load
            ignore_cache (bool, optional): ignore cache. Defaults to False.
            *args: additional arguments to pass to the plugin constructor.
            **kwargs: additional keyword arguments to pass to the plugin constructor.
        Returns:
            PluginBase: the loaded plugin
        """
        p = GulpPluginBase.load(plugin, extension=False, ignore_cache=ignore_cache, *args, **kwargs)

        # store its record_to_gulp_document function for us to call first
        self._lower_record_to_gulp_document_fun = p.record_to_gulp_document
        return p

    def _finalize_process_record(self, doc: GulpDocument, extra: list[dict]=None) -> list[dict]:
        """
        finalize processing a record, generating extra documents if needed.

        Args:
            doc (GulpDocument): the gulp document to finalize
            extra (list[dict], optional): these are the dicts returned from _process_key having tuple[1]=True, to generate further documents. Defaults to None.

        Returns:
            list[dict]: the final list of documents to be ingested (doc is always the first one).
        
        NOTE: called by the engine, do not call this function directly.
        """
        # turn to dict
        doc = doc.model_dump()

        if not extra:
            return [doc]

        def _update_document(base_doc, extra_fields):
            new_doc = copy(base_doc)
            new_doc.update(extra_fields)
            new_doc['event.hash'] = muty.crypto.hash_blake2b(
                f"{new_doc['event.original']}{new_doc['event.code']}{new_doc['event.sequence']}"
            )
            new_doc['_id'] = new_doc['event.hash']
            new_doc['gulp.event.code'] = (
                int(new_doc['event.code'])
                if new_doc['event.code'].isnumeric()
                else muty.crypto.hash_crc24(new_doc['event.code'])
            )
            return new_doc

        return [doc] + [_update_document(doc, e) for e in extra]        
    

    async def _postprocess_gulp_documents(d: list[dict]) -> list[dict]:
        """
        to be implemented in a stacked plugin to further process GulpDocument dictionaries returned by record_to_gulp_document().

        Args:
            d (list[dict]): the GulpDocument dictionaries to process.

        Returns:
            list[dict]: processed GulpDocument dictionaries.
        
        NOTE: called by the engine, do not call this function directly.
        """
        raise NotImplementedError("not implemented!")

    async def _record_to_gulp_document(self, record: any, record_idx: int, operation: str, context: str, log_file_path: str=None) -> tuple[GulpDocument,dict]:
        """
        to be implemented in a plugin to convert a record to a GulpDocument

        Args:
            record (any): the record to convert
            record_idx (int): the index of the record in the source
            operation (str): the operation associated with the record
            context (str): the context associated with the record
            log_file_path (str, optional): the source file name/path

        Returns:
            tuple[0]: the resulting GulpDocument
            tuple[1]: the list of dictionaries returned from _process_key() having tuple[1]=True, to generate further documents
        
        NOTE: called by the engine, do not call this function directly.
        """
        raise NotImplementedError("not implemented!")
    
    async def _record_to_gulp_documents_wrapper(
        self,
        record: any,
        record_idx: int,
    ) -> list[dict]:
        """
        turn a record in one or more gulp documents, taking care of calling lower plugin if any.

        Args:
            record (any): the record to convert
            record_idx (int): the index of the record in the source

        Returns:
            list[dict]: zero or more GulpDocument dictionaries

        NOTE: called by the engine, do not call this function directly.
        """
        # first, check if we are in a stacked plugin:
        # a stacked plugin have _lower_record_to_gulp_document_fun set
        if self._lower_record_to_gulp_document_fun:
            # call lower 
            doc: GulpDocument
            extras: list[dict]
            doc, extras = await self._lower_record_to_gulp_document_fun(
                record, record_idx, self._operation, self._context, self._log_file_path
            )

            # generate extra documents if needed
            docs = self._finalize_process_record(doc, extras)
            
            # post-process docs in the plugin above (may generate further documents)
            docs = self._postprocess_gulp_documents(docs)
        else:
            # call my record_to_gulp_document
            doc, extras = await self.record_to_gulp_documents(
                record, record_idx, self._operation, self._context, self._log_file_path)
            
            # generate extra documents if needed
            docs = self._finalize_process_record(doc, extras)            

        return docs

    async def process_record(
        self,
        stats: GulpIngestionStats,
        record: any,
        record_idx: int,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> None:        
        """
        Processes a single record by converting it to one or more documents and ingesting them.
        Args:
            stats (GulpIngestionStats): The ingestion statistics object to update.
            record (any): The record to process.
            record_idx (int): The index of the record.
            flt (GulpIngestionFilter, optional): The filter to apply during ingestion. Defaults to None.
            wait_for_refresh (bool, optional): Whether to wait for a refresh after ingestion. Defaults to False.
        Returns:
            None
        """
        ingestion_buffer_size = config.config().get("ingestion_buffer_size", 1000)

        # convert record to one or more documents
        self._records_processed += 1
        try:
            docs = await self._record_to_gulp_documents_wrapper(
                stats, record, record_idx
            )
        except Exception as ex:
            self._records_failed += 1
            logger().exception(ex)

        # ingest record
        for d in docs:
            self._docs_buffer.append(d)
            if len(self._docs_buffer) >= ingestion_buffer_size:
                # flush to opensearch and update stats
                ingested, skipped = await self._flush_buffer(
                    stats, flt, wait_for_refresh
                )
                # update stats
                await stats.update(
                    ws_id=self._ws_id,
                    records_skipped=skipped,
                    records_ingested=ingested,
                    records_processed=self._records_processed,
                    records_failed=self._records_failed,
                )

                # reset buffer
                self._docs_buffer = []


    async def _initialize(
        self,
        mapping_file: str = None,
        mapping_id: str = None,
        plugin_params: GulpPluginGenericParams = None,
    ) -> None:
        """
        initialize the plugin, setting mappings and specific parameters if any

        Args:
            mapping_file (str, optional): the mapping file to use. Defaults to None.
            mapping_id (str, optional): the mapping id to use. Defaults to None.
            plugin_params (GulpPluginParams, optional): plugin parameters. Defaults to None.
        
        Raises:
            ValueError: if mapping_id is set but mappings/mapping_file is not.
            ValueError: if a specific parameter is required but not found in plugin_params.

        """
        if not plugin_params:
            # ensure we have a plugin_params object
            plugin_params = GulpPluginGenericParams()

        # check if mapping_file, mappings and mapping_id are set in PluginParams
        # if so, override the values
        if plugin_params and GulpPluginType.EXTENSION not in self.type():
            # override provided mapping_file, mappings and mapping_id            
            if plugin_params.mappings:
                # ignore mapping_file
                self._mappings = {k: GulpMapping.model_validate(v) for k, v in plugin_params.mappings.items()}
                logger().debug('using plugin_params.mappings="%s"' % plugin_params.mappings)
            else:
                # mapping_file must exist if "mappings" is not set
                if plugin_params.mapping_file:
                    mapping_file = plugin_params.mapping_file
                    logger().debug(
                        "using plugin_params.mapping_file=%s"
                        % (plugin_params.mapping_file)
                    )
            if plugin_params.mapping_id:
                # use this mapping_id
                mapping_id = plugin_params.mapping_id
                logger().debug(
                    "using plugin_params.mapping_id=%s" % (plugin_params.mapping_id)
                )

        for p in self.specific_params() or []:
            # set specific plugin parameters
            k = p.name
            if p.required and (not plugin_params or k not in plugin_params.model_extra):                    
                raise ValueError(
                    "required plugin parameter '%s' not found in plugin_params !" % (k)
                )
            self._specific_params[k] = plugin_params.get('model_extra', {}).get(k, p.default_value)
            logger().debug(
                "setting specific parameter %s=%s" % (k, self._specific_params[k])
            )
        if GulpPluginType.EXTENSION in self.type():
            # extension plugins do not need mappings
            logger().debug("extension plugin, no mappings needed")
            return
        
        # some checks
        if not mapping_file and not self._mappings and not mapping_id:
            logger().warning(
                "mappings/mapping_file and mapping id are both None/empty!"
            )
            return
        if mapping_id and (not mapping_file and not self._mappings):
            raise ValueError("mapping_id is set but mappings/mapping_file is not!")

        if not self._mappings:
            # read mapping file
            mapping_file_path = gulp_utils.build_mapping_file_path(mapping_file)
            js = json.loads(await muty.file.read_file(mapping_file_path))
            if not js:                
                raise ValueError("mapping file %s is empty!" % (mapping_file_path))

            gmf: GulpMappingFile = GulpMappingFile.model_validate(js)
            self._mappings = gmf.mappings

        # set mapping_id (if not set, use first mapping found)
        self._mapping_id = mapping_id or list(self._mappings.keys())[0]
        logger().warning(
            "no mapping_id provided, using first mapping found: %s"
            % (self._mapping_id)
        )


    def cleanup(self) -> None:
        """
        Optional cleanup routine to call on unload.
        """
        return

    def _type_checks(self, v: any, k: str, index_type_mapping: dict) -> any:
        """
        check if the value should be fixed based on the index type mapping

        Args:
            v (any): The value to check.
            k (str): The mapped field (i.e. "user.id", may also be an unmapped (i.e. "gulp.unmapped") field)
            index_type_mapping (dict): The elasticsearch index key->type mappings.
        """
        if k not in index_type_mapping:
            # logger().debug("key %s not found in index_type_mapping" % (k))
            return str(v)

        index_type = index_type_mapping[k]
        if index_type == "long":
            # logger().debug("converting %s:%s to long" % (k, v))
            if isinstance(v, str):
                if v.isnumeric():
                    return int(v)
                if v.lower().startswith("0x"):
                    return int(v, 16)
            return v

        if index_type == "float" or index_type == "double":
            if isinstance(v, str):
                return float(v)
            return v

        if index_type == "date" and isinstance(v, str) and v.lower().startswith("0x"):
            # convert hex to int
            return int(v, 16)

        if index_type == "keyword" or index_type == "text":
            # logger().debug("converting %s:%s to keyword" % (k, v))
            return str(v)

        if index_type == "ip":
            # logger().debug("converting %s:%s to ip" % (k, v))
            if "local" in v.lower():
                return "127.0.0.1"
            try:
                ipaddress.ip_address(v)
            except ValueError as ex:
                logger().exception(ex)
                return None

        # add more types here if needed ...
        # logger().debug("returning %s:%s" % (k, v))
        return str(v)

    def _process_key(self, source_key: str, source_value: any) -> tuple[dict, bool]:
        """
        Maps the source key

        Args:
            source_key (str): The source key to map.
            source_value (any): The source value to map.
        Returns:
            tuple[0]: the mapped key/s and value/s
            tuple[1]: a boolean indicating if an extra document should be created with tuple[0] as base
        """
        # check if we have a mapping for source_key
        mapping = self._mappings.get(self._mapping_id, {}).fields.get(source_key, None)
        if not mapping:
            # unmapped key
            return {f"{elastic_api.UNMAPPED_PREFIX}.{source_key}": source_value}, False
        
        d = {}        
        if mapping.opt_is_timestamp_chrome:
            # timestamp chrome, turn to nanoseconds from epoch
            source_value = muty.time.chrome_epoch_to_nanos(int(source_value))
        
        if mapping.opt_extra_doc_with_event_code:
           d["event.code"] = str(mapping.opt_extra_doc_with_event_code)
           d["@timestamp"] = source_value
           return d, True

        for k in mapping.ecs:
            d[k] = source_value
        return d, False

    async def _check_raw_ingestion_enabled(
        self, plugin_params: GulpPluginGenericParams
    ) -> tuple[str, dict]:
        """
        check if we need to ingest the events using the raw ingestion plugin (from the query plugin)

        Args:
            plugin_params (GulpPluginParams): The plugin parameters.

        Returns:
            tuple[str, dict]: The ingest index and the index type mapping.
        """
        raw_plugin: GulpPluginBase = plugin_params.extra.get("raw_plugin", None)
        if raw_plugin is None:
            logger().warning("no raw ingestion plugin found, skipping!")
            return None, None
        ingest_index = plugin_params.extra.get("ingest_index", None)
        if ingest_index is None:
            logger().warning("no ingest index found, skipping!")
            return None, None

        # get kv index mapping for the ingest index
        el = elastic_api.elastic()
        index_type_mapping = await elastic_api.index_get_key_value_mapping(
            el, ingest_index, False
        )
        return ingest_index, index_type_mapping

    async def _perform_raw_ingest_from_query_plugin(
        self,
        plugin_params: GulpPluginGenericParams,
        events: list[dict],
        operation_id: int,
        client_id: int,
        ws_id: str,
        req_id: str,
    ):
        """
        ingest events using the raw ingestion plugin (from the query plugin)

        Args:
            plugin_params (GulpPluginParams): The plugin parameters.
            events (list[dict]): The events to ingest.
            operation_id (int): The operation id.
            client_id (int): The client id.
            ws_id (str): The websocket id.
            req_id (str): The request id.
        """
        raw_plugin: GulpPluginBase = plugin_params.extra.get("raw_plugin", None)

        # ingest events using the raw ingestion plugin
        ingest_index = plugin_params.extra.get("ingest_index", None)
        logger().debug(
            "ingesting %d events to gulp index %s using the raw ingestion plugin from query plugin"
            % (len(events), ingest_index)
        )
        await raw_plugin.ingest_file(
            ingest_index, req_id, client_id, operation_id, None, events, ws_id
        )

    async def _flush_buffer(
        self,
        stats: GulpIngestionStats,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> tuple[int,int]:
        """
        flushes the ingestion buffer to openssearch, updating the ingestion stats on the collab db.

        once updated, the ingestion stats are sent to the websocket.

        Args:
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            wait_for_refresh (bool, optional): Tell opensearch to wait for index refresh. Defaults to False (faster).
        Returns:
            tuple[int,int]: ingested, skipped records
        """
        ingested_docs: list[dict]=[]
        skipped = 0
        if self._docs_buffer:
            # logger().debug('flushing ingestion buffer, len=%d' % (len(self.buffer)))
            skipped, ingestion_errors, ingested_docs = await elastic_api.ingest_bulk(
                elastic_api.elastic(),
                self._index,
                self._docs_buffer,
                flt=flt,
                wait_for_refresh=wait_for_refresh,
            )
            # print(json.dumps(ingested_docs, indent=2))
            if ingestion_errors > 0:
                """
                NOTE: errors here means something wrong with the format of the documents, and must be fixed ASAP.
                ideally, function should NEVER append errors and the errors total should be the same before and after this function returns (this function may only change the skipped total, which means some duplicates were found).
                """
                if config.debug_abort_on_elasticsearch_ingestion_error():
                    raise Exception(
                        "elasticsearch ingestion errors means GulpDocument contains invalid data, review errors on collab db!"
                    )

            # send ingested docs to websocket
            if flt:
                # copy filter to avoid changing the original, if any,
                flt = copy(flt)
                # ensure data on ws is filtered
                flt.opt_storage_ignore_filter = False

            ws_docs = [
                {
                    "_id": doc["_id"],
                    "@timestamp": doc["@timestamp"],
                    "log.file.path": doc["log.file.path"],
                    "event.duration": doc["event.duration"],
                    "gulp.context": doc["gulp.context"],
                    "event.code": doc["event.code"],
                    "gulp.event.code": doc["gulp.event.code"],
                }
                for doc in ingested_docs
                if elastic_api.filter_doc_for_ingestion(
                    doc, flt
                )
                == GulpEventFilterResult.ACCEPT
            ]
            if ws_docs:
                # TODO: send to ws
                """ws_api.shared_queue_add_data(
                    WsQueueDataType.INGESTION_CHUNK,
                    req_id,
                    {"plugin": self.display_name(), "events": ws_docs},
                    ws_id=ws_id,
                )"""

        return len(ingested_docs), skipped
    
    async def _source_done(
        self,
        stats: GulpIngestionStats,
        flt: GulpIngestionFilter=None
    ) -> GulpIngestionStats:
        """
        Finalizes the ingestion process for a source by flushing the buffer and updating the ingestion statistics.
        Args:
            stats (GulpIngestionStats): The current ingestion statistics.
            flt (GulpIngestionFilter, optional): An optional filter to apply during ingestion. Defaults to None.
        Returns:
            GulpIngestionStats: The updated ingestion statistics.
        """        
        logger().debug(
            "INGESTION SOURCE DONE: "
            % (self._log_file_path)
        )
        ingested, skipped = await self._flush_buffer(
                stats, flt, wait_for_refresh=True
            )

        return await stats.update(ws_id=self._ws_id, 
                                  source_processed=1, records_ingested=ingested, records_skipped=skipped)

    async def _source_failed(
        self,
        stats: GulpIngestionStats,
        err: str | Exception,
    ) -> GulpIngestionStats:        
        """
        Handles the failure of a source during ingestion.
        Logs the error and updates the ingestion statistics with the failure details.
        Args:
            stats (GulpIngestionStats): The current ingestion statistics.
            err (str | Exception): The error that caused the source to fail.
        Returns:
            GulpIngestionStats: The updated ingestion statistics.
        """
        logger().error(
            "INGESTION SOURCE FAILED: source=%s, ex=%s"
            % (self._log_file_path, str(err))
        )
        # update and force-flush stats
        err = '%s: %s' % (self._log_file_path or '-', str(err))
        return await stats.update(ws_id=self._ws_id, source_failed=1, source_processed=1, error=err)

    @staticmethod
    async def path_by_name(name: str, extension: bool=False) -> str:
        """
        Get the path of a plugin by name.

        Args:
            name (str): The name of the plugin.
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
        Returns:
            str: The path of the plugin.        
        Raises:
            FileNotFoundError: If the plugin is not found.
        """
        async def _path_by_name_internal(name: str, base_path: str) -> str:
            path_py = muty.file.safe_path_join(base_path, f"{name}.py")
            path_pyc = muty.file.safe_path_join(base_path, f"{name}.pyc")
            if await muty.file.exists_async(path_py):
                logger().debug(f"Plugin {name}.py found in {base_path} !")
                return path_py
            if await muty.file.exists_async(path_pyc):
                logger().debug(f"Plugin {name}.pyc found in {base_path} !")
                return path_pyc
            raise FileNotFoundError(f"Plugin {name} not found !")

        # ensure name is stripped of .py/.pyc
        name = os.path.splitext(name)[0]        
        if extension:
            return await _path_by_name_internal(name, config.path_plugins(extension=True))
        return await _path_by_name_internal(name, config.path_plugins())
    
    @staticmethod
    async def load(plugin: str, extension: bool=False, ignore_cache: bool=False, *args, **kwargs) -> "GulpPluginBase":
        """
        Load a plugin by name.

        Args:
            plugin (str): The name of the plugin (may also end with .py/.pyc) or the full path
            extension (bool, optional): Whether the plugin is an extension. Defaults to False.
            ignore_cache (bool, optional): Whether to ignore the cache. Defaults to False.
            *args: Additional arguments (args[0]: pickled).
            **kwargs: Additional keyword arguments.
        """
        # this is set in __reduce__
        pickled=args[0] if args else False

        if plugin.startswith('/'):
            path = plugin
        else:
            # get plugin full path by name
            path = await GulpPluginBase.path_by_name(plugin, extension)
        bare_name = os.path.splitext(os.path.basename(path))[0]

        m = GulpPluginCache().get(bare_name) 
        if ignore_cache:
            logger().warning("ignoring cache for plugin %s" % (bare_name))
            m = None
        if m:
            # return from cache
            return m.Plugin(path, pickled=pickled, **kwargs)
        
        if extension:
            module_name = f"gulp.plugins.extension.{bare_name}"
        else:
            module_name = f"gulp.plugins.{bare_name}"

        # load from file        
        m = muty.dynload.load_dynamic_module_from_file(module_name, path)
        p: GulpPluginBase = m.Plugin(path, _pickled=pickled, **kwargs)
        logger().debug(f"loaded plugin m={m}, p={p}, name()={p.name}")
        GulpPluginCache().add(m, bare_name)
        return p

    def unload(self) -> None:
        """
        unload the plugin module by calling its `cleanup` method and deleting the module object.

        NOTE: the plugin module is **no more valid** after this function returns.

        Returns:
            None
        """
        if config.plugin_cache_enabled():
            # do not unload if cache is enabled
            return

        logger().debug("unloading plugin: %s" % (self.name))
        self.cleanup()
        GulpPluginCache().remove(self.name)


    @staticmethod
    async def list(name: str=None) -> list[dict]:
        """
        List all available plugins.

        Args:
            name (str, optional): if set, only plugins with filename matching this will be returned. Defaults to None.
        Returns:
            list[dict]: The list of available plugins.
        """
        path_plugins = config.path_plugins()
        path_extension = config.path_plugins(extension=True)
        plugins = await muty.file.list_directory_async(path_plugins, "*.py*")
        extensions = await muty.file.list_directory_async(path_extension, "*.py*")
        files = plugins + extensions
        l = []
        for f in files:
            if "__init__" or "__pycache__" in f:
                continue            
            if '/extension/' in f:
                extension=True
            else:
                extension=False
            try:
                p = await GulpPluginBase.load(f, extension=extension, ignore_cache=True)
            except Exception as ex:
                logger().exception(ex)
                logger().error("could not load plugin %s" % (f))
                continue

            if name is not None:
                # filter by name
                if name.lower() not in p.name.lower():
                    continue
            n = {
                "display_name": p.name,
                "type": p.type(),
                "desc": p.desc(),
                "filename": p.plugin_file,
                "options": [o.model_dump() for o in p.specific_params()],
                "depends_on": p.depends_on(),
                "tags": p.tags(),
                "version": p.version(),
            }
            l.append(n)
            p.unload()

        return l
