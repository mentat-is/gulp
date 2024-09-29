"""Gulp plugin base class and plugin utilities.
"""

import importlib
import ipaddress
import os
from abc import ABC, abstractmethod
from types import ModuleType
from typing import Any, Callable

import muty.crypto
import muty.dynload
import muty.file
import muty.jsend
import muty.string
import muty.time
from sigma.processing.pipeline import ProcessingPipeline

from gulp import config
from gulp import utils as gulp_utils
from gulp.api import collab_api, elastic_api
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import (
    GulpDocument,
    GulpIngestionFilter,
    GulpQueryFilter,
    GulpQueryOptions,
)
from gulp.api.mapping import helpers as mapping_helpers
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping, GulpMappingOptions
from gulp.api.rest import ws as ws_api
from gulp.api.rest.ws import WsQueueDataType
from gulp.defs import (
    GulpEventFilterResult,
    GulpLogLevel,
    GulpPluginType,
    ObjectNotFound,
)
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams
from gulp.utils import logger

# caches plugin modules for the running process
_cache: dict = {}


class PluginBase(ABC):
    """
    Base class for all Gulp plugins.
    """
    def _check_pickled(self):
        if self._pickled:
            return True
        return False

    def __reduce__(self):
        return (load_plugin, (self.path, self.type(), True, True), self.__dict__)

    def __init__(
        self,
        path: str,
        **kwargs,
    ) -> None:
        """
        Initializes a new instance of the PluginBase class.

        Args:
            path (str): The path to the plugin.
            kwargs: additional arguments if any
        """
        self._pickled = False
        self.req_id: str = None
        self.index: str = None
        self.client_id: str = None
        self.operation_id: str = None
        self.context: str = None
        self.ws_id: str = None
        self.path = path

        self.buffer: list[GulpDocument] = []
        for k, v in kwargs.items():
            self.__dict__[k] = v

        super().__init__()

    def options(self) -> list[GulpPluginOption]:
        """
        return available GulpPluginOption list (plugin specific parameters)
        """
        return []

    @abstractmethod
    def type(self) -> GulpPluginType:
        """
        Returns the plugin type.
        """

    @abstractmethod
    def version(self) -> str:
        """
        Returns plugin version.
        """

    @abstractmethod
    def desc(self) -> str:
        """
        Returns a description of the plugin.
        """

    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of the plugin.
        """

    def event_type_field(self) -> str:
        """
        Returns the field name for the event type.
        """
        return "event.code"

    def depends_on(self) -> list[str]:
        """
        Returns a list of plugins this plugin depends on.
        """
        return []

    async def query(
        self,
        operation_id: int,
        client_id: int,
        user_id: int,
        username: str,
        ws_id: str,
        req_id: str,
        plugin_params: GulpPluginParams,
        flt: GulpQueryFilter,
        options: GulpQueryOptions = None,
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
        return 0, GulpRequestStatus.FAILED

    async def query_single(
        self,
        operation_id: int,
        client_id: int,
        user_id: int,
        username: str,
        ws_id: str,
        req_id: str,
        plugin_params: GulpPluginParams,
        event: dict
    ) -> dict:
        """
        used in query plugins to query a single **full** event from external sources.

        Args:
            operation_id (int): operation ID
            client_id (int): client ID
            user_id (int): user ID performing the query
            username (str): username performing the query
            ws_id (str): websocket ID to stream the returned data to
            req_id (str): request ID
            plugin_params (GulpPluginParams, optional): plugin parameters, including i.e. in GulpPluginParams.extra the login/pwd/token to connect to the external source, plugin dependent.
            event (dict): the event to query for, i.e. as returned by the `query` method.
            
        Returns:
            dict: the event found
        """
        return {}

    def internal(self) -> bool:
        """
        Returns whether the plugin is for internal use only
        """
        return False

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

    def _process_plugin_params(
        self, custom_mapping: GulpMapping, plugin_params: GulpPluginParams = None
    ) -> tuple[GulpMapping, GulpPluginParams]:
        """
        Process the plugin parameters checking parameters as `timestamp_field`, ... and update the custom mapping accordingly.

        Args:
            custom_mapping (GulpMapping): The custom mapping provided: if it is not empty, it will be used as is (plugin_params will be ignored)
            plugin_params (GulpPluginParams, optional): The plugin parameters. Defaults to None.

        Returns:
            tuple[GulpMapping, GulpPluginParams]: The updated custom mapping and plugin parameters.
        """
        if plugin_params is None:
            plugin_params = GulpPluginParams()

        if len(custom_mapping.fields) > 0:
            # custom_mapping provided
            return custom_mapping, plugin_params

        logger().warning("no custom mapping provided")

        tf = plugin_params.timestamp_field
        if tf is not None:
            logger().debug("using timestamp_field=%s" % (tf))
            # build a proper custom_mapping with just timestamp
            custom_mapping.fields[tf] = FieldMappingEntry(is_timestamp=True)
        # logger().debug(custom_mapping)
        return custom_mapping, plugin_params

    async def sigma_plugin_initialize(
        self,
        pipeline: ProcessingPipeline = None,
        mapping_file: str = None,
        mapping_id: str = None,
        product: str = None,
        plugin_params: GulpPluginParams = None,
    ) -> ProcessingPipeline:
        """
        Initializes the Sigma plugin to convert sigma rules YAML to elasticsearch DSL query.

        Args:
            pipeline (ProcessingPipeline, optional): The processing pipeline. Defaults to None (empty pipeline, plugin must fill it).
            mapping_file (str, optional): The name of the mapping file (i.e. 'windows.json') in the gulp/mapping_files directory. Defaults to None (use pipeline mapping only).
            mapping_id (str, optional): The mapping ID (["options"]["mapping_id"] in the mapping file, to get a specific mapping). Defaults to None (use first).
            product (str, optional): The product. Defaults to None.
            plugin_params (GulpPluginParams, optional): The plugin parameters, to override i.e. mapping_file_path, mapping_id, ... Defaults to None.
        Returns:
            ProcessingPipeline: The initialized processing pipeline.
        """
        logger().debug("INITIALIZING SIGMA plugin=%s" % (self.name()))

        mapping_file_path = None
        if mapping_file is not None:
            # get path in mapping directory
            mapping_file_path = gulp_utils.build_mapping_file_path(mapping_file)

        if plugin_params is not None:
            # override with plugin_params
            if plugin_params.mapping_file is not None:
                mapping_file_path = gulp_utils.build_mapping_file_path(
                    plugin_params.mapping_file
                )
            if plugin_params.mapping_id is not None:
                mapping_id = plugin_params.mapping_id

        p = await mapping_helpers.get_enriched_pipeline(
            pipeline=pipeline,
            mapping_file_path=mapping_file_path,
            mapping_id=mapping_id,
            product=product,
        )
        return p

    async def _process_record(
        self,
        index: str,
        record: any,
        record_idx: int,
        my_record_to_gulp_document_fun: Callable,
        ws_id: str,
        req_id: str,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> tuple[TmpIngestStats, bool]:
        """
        Process a record for ingestion, updating ingestion stats.
        Args:
            index (str): The index to ingest the record into.
            record (any): The record to process.
            record_idx (int): The index of the record as in the source.
            my_record_to_gulp_document_fun (Callable): The function (for this plugin) to convert the record to one or more GulpDocument/s.
            ws_id (str): The websocket ID
            req_id (str): The request ID.
            operation_id (int): The operation ID.
            client_id (int): The client ID.
            context (str): The context of the record.
            source (str): The source of the record.
            fs (TmpIngestStats): The temporary ingestion statistics.
            custom_mapping (GulpMapping, optional): The custom mapping for the record. Defaults to None.
            index_type_mapping (dict, optional): The elastic index type mapping. Defaults to None.
            plugin (str, optional): The plugin name. Defaults to None.
            plugin_params (GulpPluginParams, optional): The plugin parameters. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            **kwargs: Additional keyword arguments.
        Returns:
            tuple[TmpIngestStats, bool]: A tuple containing the updated temporary ingestion statistics and a flag indicating whether to break the ingestion process.
        """

        # convert record to one or more GulpDocument objects
        docs = await self._call_record_to_gulp_document_funcs(
            operation_id=operation_id,
            client_id=client_id,
            context=context,
            source=source,
            fs=fs,
            record=record,
            record_idx=record_idx,
            custom_mapping=custom_mapping,
            index_type_mapping=index_type_mapping,
            plugin=plugin,
            plugin_params=plugin_params,
            record_to_gulp_document_fun=my_record_to_gulp_document_fun,
            **kwargs,
        )
        # ingest record
        for d in docs:
            fs = await self._ingest_record(index, d, fs, ws_id, req_id, flt, **kwargs)

        status, _ = await GulpStats.update(
            await collab_api.collab(),
            req_id,
            ws_id,
            fs=fs.update(processed=len(docs)),
        )
        must_break = False
        if status in [GulpRequestStatus.FAILED, GulpRequestStatus.CANCELED]:
            must_break = True

        return fs, must_break

    async def ingest_plugin_initialize(
        self,
        index: str,
        source: str | dict,
        skip_mapping: bool = False,
        pipeline: ProcessingPipeline = None,
        mapping_file: str = None,
        mapping_id: str = None,
        plugin_params: GulpPluginParams = None,
    ) -> tuple[dict, GulpMapping]:
        """
        Initializes the ingestion plugin.

        Args:
            index (str): The name of the elasticsearch index.
            source (str|dict): The source of the record (source file name or path, usually. may also be a dictionary.).
            skip_mapping (bool, optional): Whether to skip mapping initialization (just prints source and plugin name, and return empty index mapping and custom mapping). Defaults to False.
            pipeline (ProcessingPipeline, optional): The psyigma pipeline to borrow the mapping from, if any. Defaults to None (use mapping file only).
            mapping_file (str, optional): name of the mapping file (i.e. 'windows.json') in the gulp/mapping_files directory. Defaults to None (use pipeline mapping only).
            mapping_id (str, optional): The mapping ID (options.mapping_id) in the mapping file, to get a specific mapping. Defaults to None (use first).
            plugin_params (GulpPluginParams, optional): The plugin parameters (i.e. to override mapping_file, mapping_id, ...). Defaults to None.
        Returns:
            tuple[dict, GulpMapping]: A tuple containing the elasticsearch index type mappings and the enriched GulpMapping (or an empty GulpMapping is no valid pipeline and/or mapping_file are provided).

        """
        # logger().debug("ingest_plugin_initialize: index=%s, pipeline=%s, mapping_file=%s, mapping_id=%s, plugin_params=%s" % (index, pipeline, mapping_file, mapping_id, plugin_params))
        logger().debug(
            "INITIALIZING INGESTION for source=%s, plugin=%s"
            % (muty.string.make_shorter(source, 260), self.name())
        )
        if skip_mapping:
            return {}, GulpMapping()

        # get path of the mapping file in gulp/mapping_files folder
        mapping_file_path = None
        if mapping_file is not None:
            mapping_file_path = gulp_utils.build_mapping_file_path(mapping_file)

        if plugin_params is not None:
            # override with plugin_params
            if plugin_params.mapping_file is not None:
                mapping_file_path = gulp_utils.build_mapping_file_path(
                    plugin_params.mapping_file
                )
            if plugin_params.mapping_id is not None:
                mapping_id = plugin_params.mapping_id
            if plugin_params.pipeline is not None:
                pipeline = plugin_params.pipeline

        index_type_mappings = await elastic_api.index_get_mapping(
            elastic_api.elastic(), index, False
        )
        # index_type_mappings = await elastic_api.datastream_get_mapping(self.elastic, index + '-template')
        m: GulpMapping = await mapping_helpers.get_enriched_mapping_for_ingestion(
            pipeline=pipeline,
            mapping_file_path=mapping_file_path,
            mapping_id=mapping_id,
        )
        return index_type_mappings, m

    async def _call_record_to_gulp_document_funcs(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParams = None,
        record_to_gulp_document_fun: Callable = None,
        **kwargs,
    ) -> list[GulpDocument]:
        """Stub function to call stacked plugins record_to_document_gulp_document.
        Each function is called with the previously returned GulpDocument.

        Args:
            operation_id (int): the operation ID associated with the record
            client_id (int): client ID performing the ingestion
            context (str): context associated with the record
            source (str): source of the record (source file name or path, usually)
            fs (TmpIngestStats): _description_
            record (any): a single record (first time) or a list of GulpDocument objects (in stacked plugins)
            record_idx (int): The index of the record in source.
            custom_mapping (GulpMapping, optional): The custom mapping to use for the conversion. Defaults to None.
            index_type_mapping (dict, optional): elastic search index type mappings { "field": "type", ... }. Defaults to None.
            plugin (str, optional): "agent.type" to be set in the GulpDocument. Defaults to None.
            plugin_params (GulpPluginParams, optional): The plugin parameters to use, if any. Defaults to None.
            record_to_gulp_document_fun (Callable, optional): function to parse record into a gulp document, if stacked this receives a list of GulpDocuments

        Returns:
            list[GulpDocument]: zero or more GulpDocument objects
        """
        # plugin_params=deepcopy(plugin_params)

        if plugin_params is None:
            plugin_params = GulpPluginParams()

        docs = record

        if record_to_gulp_document_fun is not None:
            docs = await record_to_gulp_document_fun(
                operation_id,
                client_id,
                context,
                source,
                fs,
                record,
                record_idx,
                custom_mapping,
                index_type_mapping,
                plugin,
                plugin_params,
                **kwargs,
            )

        for fun in plugin_params.record_to_gulp_document_fun:
            docs = await fun(
                operation_id,
                client_id,
                context,
                source,
                fs,
                docs,
                record_idx,
                custom_mapping,
                index_type_mapping,
                plugin,
                plugin_params,
                **kwargs,
            )

        if docs is None:
            return []
        return docs

    async def record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParams = None,
        **kwargs,
    ) -> list[GulpDocument]:
        """
        Converts a record to one or more GulpDocument objects based on the provided index mappings.

        Args:
            operation_id (int): The operation ID associated with the record.
            client_id (int): The client ID associated with the record.
            context (str): The context associated with the record.
            source (str): The source of the record (source file name or path, usually).
            fs (TmpIngestStats): The temporary ingestion statistics (may be updated on return).
            record (any): record to convert, plugin dependent format: note that here stacked plugins receives a list of GulpDocument objects instead (since the original record may generate one or more documents).
            record_idx (int): The index of the record in source.
            custom_mapping (GulpMapping, optional): The custom mapping to use for the conversion. Defaults to None.
            index_type_mapping (dict, optional): elastic search index type mappings { "ecs_field": "type", ... }. Defaults to None.
            plugin (str, optional): "agent.type" to be set in the GulpDocument. Defaults to None.
            plugin_params (GulpPluginParams, optional): The plugin parameters to use, if any. Defaults to None.
            extra (dict, optional): Additional fields to add to the GulpDocument (after applying mapping). Defaults to None.
            **kwargs: Additional keyword arguments:

        Returns:
            list[GulDocument]: The converted GulpDocument objects or None if an exception occurred (fs is updated then).

        Raises:
            NotImplementedError: This method is not implemented yet.
        """
        raise NotImplementedError("not implemented!")

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:
        """
        Ingests a file using the plugin.

        NOTE: implementers should call super().ingest() in their implementation.
        NOTE: this function *SHOULD NOT* raise exceptions

        Args:
            index (str): name of the elasticsearch index to ingest the document into.
            req_id (str): The request ID related to this ingestion (must exist on the collab db).
            client_id (int): The client ID performing the ingestion.
            operation_id (int): The operation ID related to this ingestion.
            context (str): Context related to this ingestion.
            source (str|list[dict]): The path to the file to ingest, or a list of events dicts.
            ws_id (str): The websocket ID
            plugin_params (GulpPluginParams): additional parameters to pass to the ingestion function. Defaults to None.
            flt (GulpIngestionFilter, optional): filter to apply to this ingestion, if any. Defaults to None.
            kwargs: additional arguments if any
        """
        self.req_id = req_id
        self.client_id = client_id
        self.operation_id = operation_id
        self.context = context
        self.ws_id = ws_id
        self.index = index
        # raise NotImplementedError("not implemented!")

    async def pipeline(
        self, plugin_params: GulpPluginParams = None, **kwargs
    ) -> ProcessingPipeline:
        """
        Returns the pysigma processing pipeline for the plugin, if any.

        Args:
            plugin_params (GulpPluginParams, optional): additional parameters to pass to the pipeline. Defaults to None.
            kwargs: additional arguments if any.
        Returns:
            ProcessingPipeline: The processing pipeline.
        """
        raise NotImplementedError("not implemented!")

    def cleanup(self) -> None:
        """
        Optional cleanup routine to call on unload.
        """
        return

    def _build_gulpdocuments(
        self,
        fme: list[FieldMappingEntry],
        idx: int,
        operation_id: int,
        context: str,
        plugin: str,
        client_id: int,
        raw_event: str,
        original_id: str,
        src_file: str,
        timestamp: int = None,
        timestamp_nsec: int = None,
        event_code: str = None,
        cat: list[str] = None,
        duration_nsec: int = 0,
        gulp_log_level: GulpLogLevel = None,
        original_log_level: str = None,
        remove_raw_event: bool = False,
        **kwargs,
    ) -> list[GulpDocument]:
        """
        build one or more GulpDocument objects from a list of FieldMappingEntry objects:

        this function creates as many GulpDocument objects as there are FieldMappingEntry objects with is_timestamp=True.
        if no FieldMappingEntry object has is_timestamp=True, it creates a single GulpDocument object with the first FieldMappingEntry object.
        """
        docs: list[GulpDocument] = []
        append_doc = docs.append  # local variable for faster access

        common_params = {
            "idx": idx,
            "operation_id": operation_id,
            "context": context,
            "plugin": plugin,
            "client_id": client_id,
            "raw_event": raw_event,
            "original_id": original_id,
            "src_file": src_file,
            "timestamp": timestamp,
            "timestamp_nsec": timestamp_nsec,
            "event_code": event_code,
            "cat": cat,
            "duration_nsec": duration_nsec,
            "gulp_log_level": gulp_log_level,
            "original_log_level": original_log_level,
            **kwargs,
        }
        for f in fme:
            # print("%s\n\n" % (f))
            # for each is_timestamp build a gulpdocument with all the fields in fme
            if f.is_timestamp:
                d = GulpDocument(fme=fme, f=f, **common_params)
                if remove_raw_event:
                    d.original_event = None

                # print("%s\n\n" % (d))
                append_doc(d)

        if len(docs) == 0:
            # create a document with the given timestamp in timestamp/timestamp_nsec (if any, either it will be set to 0/invalid)
            d = GulpDocument(fme=fme, **common_params)
            if remove_raw_event:
                d.original_event = None
            append_doc(d)

        return docs

    def get_unmapped_field_name(self, field: str) -> str:
        """
        Returns the name of the unmapped field.

        Parameters:
        - field (str): The name of the field.

        Returns:
        - str: The name of the unmapped field.
        """
        if not elastic_api.UNMAPPED_PREFIX:
            return field

        return f"{elastic_api.UNMAPPED_PREFIX}.{field}"

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
                if v.startswith("0x"):
                    return int(v, 16)
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
    
    def _map_source_key_lite(self, event: dict, fields: dict) -> dict:
        # for each field, check if key exist: if so, map it using "map_to"
        for k, field in fields.items():
            if k in event:                
                map_to = field.get("map_to", None)
                if map_to is not None:
                    event[map_to] = event[k]
                elif field.get("is_event_code", False):
                    # event code is a special case: 
                    # it is always stored as "event.code" and "gulp.event.code", the first being a string and the second being a number.
                    v = event[k]
                    event["event.code"] = str(v)
                    if isinstance(v, int) or str(v).isnumeric():
                        # already numeric
                        event["gulp.event.code"] = int(v)
                    else:
                        # string, hash it
                        event["gulp.event.code"] = muty.crypto.hash_crc24(v)
        return event
    
    def _map_source_key(
        self,
        plugin_params: GulpPluginParams,
        custom_mapping: GulpMapping,
        source_key: str,
        v: Any,
        index_type_mapping: dict = None,
        ignore_custom_mapping: bool = False,
        **kwargs,
    ) -> list[FieldMappingEntry]:
        """
        map source key to a field mapping entry with "result": {mapped_key: v}

        Args:
            plugin_params (GulpPluginParams): The plugin parameters.
            custom_mapping (GulpMapping): The custom mapping.
            source_key (str): The key to look for(=the event record key to be mapped) in the custom_mapping dictionary
            v (any): value to set for mapped key/s.
            index_type_mapping (dict, optional): The elasticsearch index key->type mappings. Defaults to None.
            ignore_custom_mapping (bool, optional): Whether to ignore custom_mapping and directly map source_key to v. Defaults to False.
            kwargs: Additional keyword arguments.

        Returns:
            list[FieldMappingEntry]: zero or more FieldMappingEntry objects with "result" set.
        """
        # get mapping and option from custom_mapping
        if index_type_mapping is None:
            index_type_mapping = {}
        # logger().debug('len index type mapping=%d' % (len(index_type_mapping)))
        mapping_dict: dict = custom_mapping.fields
        mapping_options = (
            custom_mapping.options
            if custom_mapping.options is not None
            else GulpMappingOptions()
        )
        
        # basic checks
        if v == "-" or v is None:
            return []

        if isinstance(v, str):
            v = v.strip()
            if not v and mapping_options.ignore_blanks:
                # not adding blank strings
                return []

        # fix value if needed, and add to extra
        if ignore_custom_mapping or (
            plugin_params is not None and plugin_params.ignore_mapping_ingest
        ):
            # direct mapping, no need to check custom_mappings
            return [FieldMappingEntry(result={source_key: v})]

        if source_key not in mapping_dict:
            # logger().error('key "%s" not found in custom mapping, mapping_dict=%s!' % (source_key, muty.string.make_shorter(str(mapping_dict))))
            # key not found in custom_mapping, check if we have to map it anyway
            if not mapping_options.ignore_unmapped:
                return [
                    FieldMappingEntry(
                        result={self.get_unmapped_field_name(source_key): str(v)}
                    )
                ]

        # there is a mapping defined to be processed
        fm: FieldMappingEntry = mapping_dict[source_key]
        map_to_list = (
            [fm.map_to] if isinstance(fm.map_to, (str, type(None))) else fm.map_to
        )

        # in the end, this function will return a list of FieldMappingEntry objects with "result" set: these results will be used to create the GulpDocument object
        fme_list: list[FieldMappingEntry] = []
        for k in map_to_list:
            # make a copy of fme without using deepcopy)
            dest_fm = FieldMappingEntry(
                is_timestamp=fm.is_timestamp,
                event_code=fm.event_code,
                do_multiply=fm.do_multiply,
                is_timestamp_chrome=fm.is_timestamp_chrome,
                is_variable_mapping=fm.is_variable_mapping,
                result={},
            )

            # check if it is a number and/or a timestamp (including chrome timestamp, which is a special case)
            is_numeric = isinstance(v, int) or str(v).isnumeric()
            if is_numeric:
                v = int(v)
                # ensure chrome timestamp is properly converted to nanos
                # logger().debug('***** is_numeric, v=%d' % (v))
                if fm.is_timestamp_chrome:
                    v = int(muty.time.chrome_epoch_to_nanos(v))
                    # logger().debug('***** is_timestamp_chrome, v nsec=%d' % (v))

                if fm.do_multiply is not None:
                    # apply a multipler if any (must turn v to nanoseconds)
                    # logger().debug("***** is_numeric, multiply, v=%d" % (v))
                    v = int(v * fm.do_multiply)
                    # logger().debug("***** is_numeric, AFTER multiply, v=%d" % (v))

            elif isinstance(v, str) and fm.is_timestamp:
                v = int(
                    muty.time.string_to_epoch_nsec(
                        v,
                        utc=mapping_options.timestamp_utc,
                        dayfirst=mapping_options.timestamp_dayfirst,
                        yearfirst=mapping_options.timestamp_yearfirst,
                    )
                )
                # logger().debug('***** str and is_timestamp, v nsec=%d' % (v))
            if fm.is_timestamp:
                # it's a timestamp, another event will be generated
                vv = muty.time.nanos_to_millis(v)
                dest_fm.result["@timestamp"] = vv
                dest_fm.result["@timestamp_nsec"] = v
                # logger().debug('***** timestamp nanos, v=%d' % (v))
                # logger().debug('***** timestamp to millis, v=%d' % (vv))

            if fm.is_timestamp or fm.is_timestamp_chrome:
                # logger().debug('***** timestamp or timestamp_chrome, v=%d' % (v))
                if v < 0:
                    # logger().debug('***** adding invalid timestamp')
                    v = 0
                    GulpDocument.add_invalid_timestamp(dest_fm.result)
                if k is not None:
                    # also add to mapped key
                    dest_fm.result[k] = v
            else:
                # not a timestamp, map
                if k is None:
                    # add unmapped
                    k = self.get_unmapped_field_name(source_key)
                else:
                    v = self._type_checks(v, k, index_type_mapping)
                dest_fm.result[k] = v

            fme_list.append(dest_fm)
            """
            logger().debug('FME LIST FOR THIS RECORD:')
            for p in fme_list:
                logger().debug(p)
            logger().debug('---------------------------------')
            """
        return fme_list

    def _build_ingestion_chunk_for_ws(
        self, docs: list[dict], flt: GulpIngestionFilter = None
    ) -> list[dict]:
        """
        Builds the ingestion chunk for the websocket, filtering if needed.
        """
        # logger().debug("building ingestion chunk, flt=%s" % (flt))
        if not docs:
            return []

        ws_docs = [
            {
                "_id": doc["_id"],
                "@timestamp": doc["@timestamp"],
                "gulp.source.file": doc["gulp.source.file"],
                "event.duration": doc["event.duration"],
                "gulp.context": doc["gulp.context"],
                "gulp.log.level": doc.get("gulp.log.level", int(GulpLogLevel.INFO)),
                "event.category": doc.get("event.category", None),
                "event.code": doc["event.code"],
                "gulp.event.code": doc["gulp.event.code"],
            }
            for doc in docs
            if elastic_api.filter_doc_for_ingestion(
                doc, flt, ignore_store_all_documents=True
            )
            == GulpEventFilterResult.ACCEPT
        ]

        return ws_docs

    async def _flush_buffer(
        self,
        index: str,
        fs: TmpIngestStats,
        ws_id: str,
        req_id: str,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> TmpIngestStats:
        """
        NOTE: errors appended by this function are intended as INGESTION errors:
        it means something wrong with the format of the event, and must be fixed ASAP if this happens.
        ideally, function should NEVER append errors and the errors total should be the same before and after this function returns (this function may only change the skipped total, which means some duplicates were found).
        """
        if len(self.buffer) == 0:
            # already flushed
            return fs

        # logger().debug('flushing ingestion buffer, len=%d' % (len(self.buffer)))

        skipped, failed, failed_ar, ingested_docs = await elastic_api.ingest_bulk(
            elastic_api.elastic(),
            index,
            self.buffer,
            flt=flt,
            wait_for_refresh=wait_for_refresh,
        )
        # print(json.dumps(ingested_docs, indent=2))

        if failed > 0:
            if config.debug_abort_on_elasticsearch_ingestion_error():
                raise Exception(
                    "elasticsearch ingestion errors means GulpDocument contains invalid data, review errors on collab db!"
                )

        self.buffer = []

        # build ingestion chunk
        ws_docs = self._build_ingestion_chunk_for_ws(ingested_docs, flt)

        # send ingested docs to websocket
        if len(ws_docs) > 0:
            ws_api.shared_queue_add_data(
                WsQueueDataType.INGESTION_CHUNK,
                req_id,
                {"plugin": self.name(), "events": ws_docs},
                ws_id=ws_id,
            )

        # update stats
        fs = fs.update(
            failed=failed,
            skipped=skipped,
            ingest_errors=failed_ar,
        )
        return fs

    async def _ingest_record(
        self,
        index: str,
        doc: GulpDocument | dict,
        fs: TmpIngestStats,
        ws_id: str,
        req_id: str,
        flt: GulpIngestionFilter = None,
        flush_enabled: bool = True,
        **kwargs,
    ) -> TmpIngestStats:
        """
        bufferize as much as ingestion_buffer_size, then flush (writes to elasticsearch)
        """
        ingestion_buffer_size = config.config().get("ingestion_buffer_size", 1000)
        self.buffer.append(doc)
        if len(self.buffer) >= ingestion_buffer_size and flush_enabled:
            # time to flush
            fs = await self._flush_buffer(index, fs, ws_id, req_id, flt)

        return fs

    def _parser_failed(
        self, fs: TmpIngestStats, source: str | dict, ex: Exception | str
    ) -> TmpIngestStats:
        """
        whole source failed ingestion (error happened before the record parsing loop), helper to update stats
        """
        logger().exception(
            "PARSER FAILED: source=%s, ex=%s"
            % (muty.string.make_shorter(str(source), 260), ex)
        )
        fs = fs.update(ingest_errors=[ex], parser_failed=1)
        return fs

    def _record_failed(
        self, fs: TmpIngestStats, entry: any, source: str | dict, ex: Exception | str
    ) -> TmpIngestStats:
        """
        record failed ingestion (in the record parser loop), helper to update stats

        Args:
            fs (TmpIngestStats): The temporary ingestion statistics.
            entry (any): The entry that failed.
            source (str): The source of the record.
            ex (Exception|str): The exception that caused the failure.
        Returns:
            TmpIngestStats: The updated temporary ingestion statistics.
        """
        # logger().exception("RECORD FAILED: source=%s, record=%s, ex=%s" % (muty.string.make_shorter(str(source),260), muty.string.make_shorter(str(entry),260), ex))
        fs = fs.update(failed=1, ingest_errors=[ex])
        return fs

    async def _finish_ingestion(
        self,
        index: str,
        source: str | dict,
        req_id: str,
        client_id: int,
        ws_id: str,
        fs: TmpIngestStats,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        """
        to be called whenever ingest() must exit: flushes the buffer and updates the ingestion stats
        """
        try:
            # finally flush ingestion buffer
            fs = await self._flush_buffer(index, fs, ws_id, req_id, flt)
            logger().info(
                "INGESTION DONE FOR source=%s,\n\tclient_id=%d (processed(ingested)=%d, failed=%d, skipped=%d, errors=%d, parser_errors=%d)"
                % (
                    muty.string.make_shorter(str(source), 260),
                    client_id,
                    fs.ev_processed,
                    fs.ev_failed,
                    fs.ev_skipped,
                    len(fs.ingest_errors),
                    fs.parser_failed,
                )
            )
        except Exception as ex:
            fs = fs.update(ingest_errors=[ex])
            logger().exception(
                "FAILED finalizing ingestion for source=%s"
                % (muty.string.make_shorter(str(source), 260))
            )

        finally:
            status, _ = await GulpStats.update(
                await collab_api.collab(),
                req_id,
                ws_id,
                fs=fs,
                force=True,
                file_done=True,
            )
        return status


def _get_plugin_path(
    plugin: str, plugin_type: GulpPluginType = GulpPluginType.INGESTION
) -> str:

    # try plain .py first
    # TODO: on license manager, disable plain .py load (only encrypted pyc)
    # get path according to plugin type
    path_plugins = config.path_plugins(plugin_type)
    plugin_path = os.path.join(path_plugins, f"{plugin}.py")

    if not muty.file.exists(plugin_path):
        plugin_path = os.path.join(path_plugins, f"{plugin}.pyc")
        if not muty.file.exists(plugin_path):
            raise ObjectNotFound(
                f"Plugin {plugin} not found (tried {plugin}.py and {plugin}.pyc in {path_plugins})"
            )
    return plugin_path

def load_plugin(
    plugin: str,
    plugin_type: GulpPluginType = GulpPluginType.INGESTION,
    ignore_cache: bool = False,
    from_reduce: bool = False,
    **kwargs,
) -> PluginBase:
    """
    Load a plugin from a given path or from the default plugin path.

    Args:
        plugin (str): The name or path of the plugin to load.
        plugin_type (GulpPluginType, optional): The type of the plugin to load. Defaults to GulpPluginType.INGESTION.
            this is ignored if the plugin is an absolute path or if "plugin_cache" is enabled and the plugin is already cached.
        ignore_cache (bool, optional): Whether to ignore the plugin cache. Defaults to False.
        from_reduce (bool, optional): Whether the plugin is being loaded from a reduce call. Defaults to False.
        **kwargs (dict, optional): Additional keyword arguments.
    Returns:
        PluginBase: The loaded plugin.

    Raises:
        Exception: If the plugin could not be loaded.
    """
    logger().debug("load_plugin %s, type=%s, ignore_cache=%r, kwargs=%s ..." % (plugin, plugin_type, ignore_cache, kwargs))

    if not '/' in plugin and (plugin.lower().endswith(".py") or plugin.lower().endswith(".pyc")):
        # remove extension
        plugin = plugin.rsplit(".", 1)[0]

    m = plugin_cache_get(plugin)
    if ignore_cache:
        logger().debug("ignoring cache for plugin %s" % (plugin))
        m = None

    if "/" in plugin:
        # plugin is an absolute path
        path = muty.file.abspath(plugin)
    else:
        # use plugin_type to load from the correct subfolder
        path = _get_plugin_path(plugin, plugin_type=plugin_type)

    module_name = f"gulp.plugins.{plugin_type.value}.{plugin}"
    try:
        m = muty.dynload.load_dynamic_module_from_file(module_name, path)
    except Exception as ex:
        raise Exception(
            f"Failed to load plugin {path}: {str(ex)}"
        ) from ex

    mod: PluginBase = m.Plugin(path, _pickled=from_reduce, **kwargs)
    logger().debug("loaded plugin m=%s, mod=%s, name()=%s" % (m, mod, mod.name()))
    plugin_cache_add(m, plugin)

    return mod


async def list_plugins() -> list[dict]:
    """
    List all available plugins.

    Returns:
        list[dict]: The list of available plugins.
    """
    path_plugins = config.path_plugins(t=None)
    l = []
    for plugin_type in GulpPluginType:
        subdir_path = os.path.join(path_plugins, plugin_type.value)
        files = await muty.file.list_directory_async(subdir_path, "*.py*")
        for f in files:
            if "__init__" not in f and "__pycache__" not in f:
                try:
                    p = load_plugin(os.path.splitext(os.path.basename(f))[0], plugin_type)
                    n = {
                        "display_name": p.name(),
                        "type": str(p.type()),
                        "desc": p.desc(),
                        "filename": os.path.basename(p.path),
                        "internal": p.internal(),
                        "options": [o.to_dict() for o in p.options()],
                        "depends_on": p.depends_on(),
                        "tags": p.tags(),
                        "event_type_field": p.event_type_field(),
                        "version": p.version(),
                    }
                    l.append(n)
                    unload_plugin(p)
                except Exception as ex:
                    logger().exception(ex)
                    logger().error("could not load plugin %s" % (f))
                    continue
    return l


async def get_plugin_tags(plugin: str) -> list[str]:
    """
    Get the tags for a given (ingestion) plugin.

    Args:
        plugin (str): The name of the plugin to get the tags for.
    Returns:
        list[str]: The tags for the given plugin.
    """
    p = load_plugin(plugin)
    tags = p.tags()
    return tags


def unload_plugin(mod: PluginBase) -> None:
    """
    Unloads a plugin module by calling its `unload` method and deletes the module object

    NOTE: mod is **no more valid** after this function returns.

    Args:
        mod (PluginBase): The plugin module to unload.
        run_gc (bool): if set, garbage collector is called after unloading the module. Defaults to True.

    Returns:
        None
    """
    if config.plugin_cache_enabled():
        return

    if mod is not None:
        # delete from cache if any
        # plugin_cache_delete(mod)

        logger().debug("unloading plugin: %s" % (mod.name()))
        mod.cleanup()
        del mod


def plugin_cache_clear() -> None:
    """
    Clear the process's own plugin cache.

    Returns:
        None
    """
    global _cache
    if not config.plugin_cache_enabled():
        return

    _cache = {}


def plugin_cache_remove(plugin: str) -> None:
    """
    Remove a plugin from the process's own plugin cache.

    Args:
        plugin (str): The name/path of the plugin to remove from the cache.

    Returns:
        None
    """
    global _cache
    if not config.plugin_cache_enabled():
        return

    if plugin in _cache:
        logger().debug("removing plugin %s from cache" % (plugin))

        # cleanup module and delete
        m = _cache[plugin]
        del _cache[plugin]


def plugin_cache_add(m: ModuleType, name: str) -> None:
    """
    Add a plugin to the process's own plugin cache.

    Args:
        m (ModuleType): The plugin module to add to the cache.
        name (str): The name/path of the plugin.

    Returns:
        None
    """
    global _cache
    if not config.plugin_cache_enabled():
        return

    mm = _cache.get(name, None)
    if mm is None:
        logger().debug("adding plugin %s (%s) to cache" % (name, m))
        _cache[name] = m


def plugin_cache_get(plugin: str) -> ModuleType:
    """
    Retrieve a plugin from the process's own plugin cache.

    Args:
        plugin (str): The name/path of the plugin to retrieve.

    Returns:
        ModuleType: The plugin module if found in the cache, otherwise None.
    """
    global _cache
    if not config.plugin_cache_enabled():
        return None

    p = _cache.get(plugin, None)
    if p is not None:
        logger().debug("found plugin %s in cache" % (plugin))
    else:
        logger().warning("plugin %s not found in cache" % (plugin))
    return p
