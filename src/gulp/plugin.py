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
from sigma.processing.pipeline import ProcessingPipeline

from gulp import config
from gulp import utils as gulp_utils
from gulp.api import collab_api, elastic_api
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.stats import GulpIngestionStats
from gulp.api.elastic.structs import (
    GulpDocument,
    GulpIngestionFilter,
    GulpQueryFilter,
)
from gulp.api.mapping import helpers as mapping_helpers
from gulp.api.mapping.models import (
    GulpMappingField,
    GulpMapping,
    GulpMappingFile,
    GulpMappingOptions,
)
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

    def __reduce__(self):
        """
        This method is automatically used by the pickle module to serialize the object when it is passed to the multiprocessing module.

        Returns:
            tuple: A tuple containing the callable, its arguments, and the object's state.
        """

        # load the plugin module setting the pickled flag to True
        return (load_plugin, (self.path, self.type(), True, True), self.__dict__)

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
        # for ingestion, the mappings to apply
        self.mappings: dict[str, GulpMapping] = {}
        # for ingestion, the key in the mappings dict to be used
        self.mapping_id: str = None
        # for ingestion, the lower plugin record_to_gulp_document function to call (if this is a stacked plugin on top of another)
        self._lower_record_to_gulp_document_fun: Callable = None

        s = os.path.basename(self.path)
        s = os.path.splitext(s)[0]
        # to have faster access to the plugin file name (without ext)
        self.plugin_file = s

        # to have faster access to the plugin name
        self.name = self.display_name()

        # to bufferize gulpdocuments
        self._buffer: list[dict] = []

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
    def type(self) -> GulpPluginType:
        """
        Returns the plugin type.
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

    def options(self) -> list[GulpPluginOption]:
        """
        return available GulpPluginOption list (plugin specific parameters)
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
        return 0, GulpRequestStatus.FAILED

    async def query_external(
        self,
        operation_id: int,
        client_id: int,
        user_id: int,
        username: str,
        ws_id: str,
        req_id: str,
        plugin_params: GulpPluginParams,
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
        return 0, GulpRequestStatus.FAILED

    async def query_external_single(
        self,
        plugin_params: GulpPluginParams,
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
        return {}

    async def ingest(
        self,
        req_id: str,
        ws_id: str,
        user: str,
        index: str,
        operation: str,
        context: str,
        log_file_path: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        """
        Ingests a file using the plugin.

        Args:
            req_id (str): The request ID.
            ws_id (str): The websocket ID.
            user (str): The user performing the ingestion.
            index (str): The name of the targeet opensearch/elasticsearch index.
            operation (str): The operation.
            context (str): The context.
            log_file_path (str): The path to the log file.
            plugin_params (GulpPluginParams, optional): The plugin parameters. Defaults to None.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.

        Returns:
            GulpRequestStatus: The status of the ingestion.

        Notes:
            - implementers should call super().ingest() first.<br>
            - this function *MUST NOT* raise exceptions.
        """
        raise NotImplementedError("not implemented!")

    def _load_lower_plugin(
        self, plugin: str, ignore_cache: bool = False
    ) -> "PluginBase":
        """
        in a stacked plugin, load the lower plugin and set the _lower_record_to_gulp_document_fun to the lower plugin record_to_gulp_document function.

        Args:
            plugin (str): the plugin to load
            ignore_cache (bool, optional): ignore cache. Defaults to False.

        Returns:
            PluginBase: the loaded plugin
        """
        p = load_plugin(plugin, ignore_cache=ignore_cache)
        self._lower_record_to_gulp_document_fun = p.record_to_gulp_document
        return p

    async def _postprocess_gulp_document(d: dict) -> dict:
        """
        to be implemented in a stacked plugin to further process a GulpDocument object before ingestion.

        Args:
            d (dict): the GulpDocument object to process

        Returns:
            dict: the processed GulpDocument object
        """
        raise NotImplementedError("not implemented!")

    async def _record_to_documents(
        self,
        record: any,
        record_idx: int,
        operation: str,
        context: str,
        log_file_path: str,
        stats: GulpIngestionStats = None,
    ) -> list[dict]:
        """
        this function calls
        """
        # first, check if we are in a stacked plugin:
        # a stacked plugin have _lower_record_to_gulp_document_fun set
        if self._lower_record_to_gulp_document_fun:
            # call lower
            docs = await self._lower_record_to_gulp_document_fun(
                record, record_idx, operation, context, log_file_path
            )

            # post-process docs
            for d in docs:
                await self._postprocess_gulp_document(d)
        else:
            # call my record_to_gulp_document
            docs = await self.record_to_gulp_document(
                record, record_idx, operation, context, log_file_path, stats
            )

        return docs

    async def _process_record(
        self,
        stats: GulpIngestionStats,
        ws_id: str,
        index: str,
        record: any,
        record_idx: int,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> GulpRequestStatus:

        ingestion_buffer_size = config.config().get("ingestion_buffer_size", 1000)
        self._records_processed += 1
        docs = await self._record_to_documents(
            record, record_idx, stats.operation, stats.context, stats.source, stats
        )

        # ingest record
        for d in docs:
            self._buffer.append(d)
            if len(self._buffer) >= ingestion_buffer_size:
                # time to flush
                fs = await self._flush_buffer(
                    index, ws_id, stats.req_id, stats, flt, wait_for_refresh
                )

                status, _ = await GulpStats.update(
                    await collab_api.session(),
                    req_id,
                    ws_id,
                    fs=fs.update(processed=len(docs)),
                )
        must_break = False
        if status in [GulpRequestStatus.FAILED, GulpRequestStatus.CANCELED]:
            must_break = True

        return fs, must_break

    async def initialize(
        self,
        mapping_file: str = None,
        mapping_id: str = None,
        plugin_params: GulpPluginParams = None,
    ) -> None:
        """
        Asynchronously initializes the plugin with the provided mapping file and ID.
        This method reads a mapping file and validates its contents, storing the
        resulting "mappings" and "mapping_id" in the instance.
        If `plugin_params` is provided, it can override the `mapping_file` and `mapping_id` values or provide
        a full `mappings` dictionary.
        Args:
            mapping_file (str, optional): name of the mapping file. Defaults to None.
            mapping_id (str, optional): The ID of the mapping inside the mapping file. Defaults to None.
            plugin_params (GulpPluginParams, optional): Parameters that may override
                `mapping_file` and `mapping_id`. Defaults to None.
        Raises:
            ValueError: If the mapping file is empty or if both `mapping_file` and `mapping_id` are None.
            ValidationError: If the mapping file is invalid.
        Notes:
            - If both `mapping_file` and `mapping_id` are None, a warning is logged
              and an empty dictionary is returned.
            - The method logs debug information if `plugin_params` overrides the
              `mapping_file` or `mapping_id`.
        """

        # check if mapping_file, mappings and mapping_id are set in PluginParams
        # if so, override the values
        if plugin_params:
            if plugin_params.mappings:
                # ignore mapping_file
                self.mappings = {}
                for k, v in plugin_params.mappings.items():
                    self.mappings[k] = GulpMapping.model_validate(v)
                logger().debug(
                    'using plugin_params.mappings="%s"' % (plugin_params.mappings)
                )
            else:
                if plugin_params.mapping_file:
                    mapping_file = plugin_params.mapping_file
                    logger().debug(
                        "using plugin_params.mapping_file=%s"
                        % (plugin_params.mapping_file)
                    )
            if plugin_params.mapping_id:
                mapping_id = plugin_params.mapping_id
                logger().debug(
                    "using plugin_params.mapping_id=%s" % (plugin_params.mapping_id)
                )

        if (not mapping_file and not self.mappings) and not mapping_id:
            logger().warning(
                "mappings/mapping_file and mapping id are both None/empty!"
            )
            raise ValueError(
                "mappings/mapping_file and mapping id are both None/empty!"
            )
        if mapping_id and (not mapping_file and not self.mappings):
            raise ValueError("mapping_id is set but mappings/mapping_file is not!")

        self.mapping_id = mapping_id
        if not self.mapping_id:
            self.mapping_id = list(self.mappings.keys())[0]
            logger().warning(
                "no mapping_id provided, using first mapping found: %s"
                % (self.mapping_id)
            )
        if self.mappings:
            # mappings provided directly
            return

        # read mapping file
        mapping_file_path = gulp_utils.build_mapping_file_path(mapping_file)
        js = json.loads(await muty.file.read_file(mapping_file_path))
        if not js:
            raise ValueError("mapping file %s is empty!" % (mapping_file_path))

        gmf: GulpMappingFile = GulpMappingFile.model_validate(js)
        self.mappings = gmf.mappings

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

    async def pipeline(
        self, plugin_params: GulpPluginParams = None
    ) -> ProcessingPipeline:
        """
        Returns the pysigma processing pipeline implemented in the plugin, if any

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
        fme: list[GulpMappingField],
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

    def _remap_event_fields(
        self, event: dict, fields: dict, index_type_mapping: dict = None
    ) -> dict:
        """
        apply mapping to event, handling special cases:

            - event code (always map to "event.code" and "gulp.event.code")

        Args:
            event (dict): The event to map.
            fields (dict): describes the mapping, a structure with the following format
            {
                "field1": {
                    # if "field1" exists in event, map it to "mapped_field"
                    "map_to": "mapped_field",
                },
                "field2: {
                    # if "field2" exists in event, create "event.code" (str) and "gulp.event.code" (int) with the value of "field2"
                    "is_event_code": True
                }
            }
            index_type_mapping (dict, optional): The elasticsearch index key->type mappings. Defaults to None.

        Returns:
            dict: The mapped event.
        """

        mapped_ev: dict = {}
        if index_type_mapping is None:
            index_type_mapping = {}
        if fields is None:
            fields = {}
        for k, v in event.items():
            if k in index_type_mapping.keys():
                # found in index mapping, fix value if needed. @timestamp is handled here
                mapped_ev[k] = self._type_checks(v, k, index_type_mapping)

            # check for custom mapping
            if k in fields.keys():
                field = fields[k]
                map_to = field.get("map_to", None)
                is_event_code = field.get("is_event_code", False)
                if is_event_code:
                    # event code is a special case:
                    # it is always stored as "event.code" and "gulp.event.code", the first being a string and the second being a number.
                    mapped_ev["event.code"] = str(v)
                    if isinstance(v, int) or str(v).isnumeric():
                        # already numeric
                        mapped_ev["gulp.event.code"] = int(v)
                    else:
                        # string, hash it
                        mapped_ev["gulp.event.code"] = muty.crypto.hash_crc24(v)
                elif map_to is not None:
                    # apply mapping
                    mapped_ev[map_to] = event[k]
            else:
                # add as unmapped, forced to string
                if k == "_id":
                    # this is not in the index type mapping even if it is provided
                    mapped_ev["_id"] = str(v)
                else:
                    mapped_ev["%s.%s" % (elastic_api.UNMAPPED_PREFIX, k)] = str(v)

        return mapped_ev

    def _map_source_key_lite(self, event: dict, fields: dict) -> dict:
        """
        handles special cases for:

        - event code (always map to "event.code" and "gulp.event.code")
        """
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
    ) -> list[GulpMappingField]:
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
        if ignore_custom_mapping:
            # direct mapping, no need to check custom_mappings
            return [GulpMappingField(result={source_key: v})]

        if source_key not in mapping_dict:
            # logger().error('key "%s" not found in custom mapping, mapping_dict=%s!' % (source_key, muty.string.make_shorter(str(mapping_dict))))
            # key not found in custom_mapping, check if we have to map it anyway
            if not mapping_options.ignore_unmapped:
                return [
                    GulpMappingField(
                        result={self.get_unmapped_field_name(source_key): str(v)}
                    )
                ]

        # there is a mapping defined to be processed
        fm: GulpMappingField = mapping_dict[source_key]
        map_to_list = (
            [fm.map_to] if isinstance(fm.map_to, (str, type(None))) else fm.map_to
        )

        # in the end, this function will return a list of FieldMappingEntry objects with "result" set: these results will be used to create the GulpDocument object
        fme_list: list[GulpMappingField] = []
        for k in map_to_list:
            # make a copy of fme without using deepcopy)
            dest_fm = GulpMappingField(
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
                dest_fm.result["gulp.timestamp.nsec"] = v
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
                "gulp.timestamp": doc["gulp.timestamp"],
                "log.file.path": doc["log.file.path"],
                "event.duration": doc["event.duration"],
                "gulp.context": doc["gulp.context"],
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

    async def _check_raw_ingestion_enabled(
        self, plugin_params: GulpPluginParams
    ) -> tuple[str, dict]:
        """
        check if we need to ingest the events using the raw ingestion plugin (from the query plugin)

        Args:
            plugin_params (GulpPluginParams): The plugin parameters.

        Returns:
            tuple[str, dict]: The ingest index and the index type mapping.
        """
        raw_plugin: PluginBase = plugin_params.extra.get("raw_plugin", None)
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
        plugin_params: GulpPluginParams,
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
        raw_plugin: PluginBase = plugin_params.extra.get("raw_plugin", None)

        # ingest events using the raw ingestion plugin
        ingest_index = plugin_params.extra.get("ingest_index", None)
        logger().debug(
            "ingesting %d events to gulp index %s using the raw ingestion plugin from query plugin"
            % (len(events), ingest_index)
        )
        await raw_plugin.ingest(
            ingest_index, req_id, client_id, operation_id, None, events, ws_id
        )

    async def _flush_buffer(
        self,
        index: str,
        ws_id: str,
        stats: GulpIngestionStats,
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> None:
        """
        flushes the ingestion buffer to openssearch, updating the ingestion stats on the collab db.

        once updated, the ingestion stats are sent to the websocket.

        Args:
            index (str): The index to flush to.
            ws_id (str): The websocket ID.
            stats (GulpIngestionStats): The ingestion stats.
            flt (GulpIngestionFilter, optional): The ingestion filter. Defaults to None.
            wait_for_refresh (bool, optional): Tell opensearch to wait for index refresh. Defaults to False (faster).

        """
        ingested_docs = 0
        if self._buffer:
            # logger().debug('flushing ingestion buffer, len=%d' % (len(self.buffer)))
            skipped, ingestion_errors, ingested_docs = await elastic_api.ingest_bulk(
                elastic_api.elastic(),
                index,
                self._buffer,
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

            # update stats
            self._records_skipped += skipped

            # send ingested docs to websocket
            if flt:
                # copy filter to avoid changing the original, if any,
                # ensure data on ws filtered
                flt = copy(flt)
                flt.opt_storage_ignore_filter = False
            ws_docs = self._build_ingestion_chunk_for_ws(ingested_docs, flt)
            if len(ws_docs) > 0:
                # TODO: send to ws
                """ws_api.shared_queue_add_data(
                    WsQueueDataType.INGESTION_CHUNK,
                    req_id,
                    {"plugin": self.display_name(), "events": ws_docs},
                    ws_id=ws_id,
                )"""

        # update stats
        await stats.update(
            ws_id=ws_id,
            records_skipped=self._records_skipped,
            records_ingested=len(ingested_docs),
            records_processed=self.records_processed,
            records_failed=self._records_failed,
            force_flush=True,
        )
        self._buffer = []
        self._records_skipped = 0
        self._records_failed = 0

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
        self._buffer.append(doc)
        if len(self._buffer) >= ingestion_buffer_size and flush_enabled:
            # time to flush
            fs = await self._flush_buffer(index, fs, ws_id, req_id, flt)

        return fs

    async def _source_failed(
        self,
        stats: GulpIngestionStats,
        err: str | Exception,
        ws_id: str,
        source: str = None,
    ) -> GulpIngestionStats:
        """
        to be called whenever a whole source fails to be ingested, also flushes the ingestion stats.
        """
        logger().error(
            "INGESTION SOURCE FAILED: source=%s, ex=%s"
            % (muty.string.make_shorter(str(source), 260), str(err))
        )
        return await stats.update(ws_id=ws_id, source_failed=1, force_flush=True)

    async def _record_failed(
        self,
        stats: GulpIngestionStats,
        err: str | Exception,
        ws_id: str,
        source: str = None,
    ) -> GulpIngestionStats:
        return await stats.update(records_failed=1)

    async def _record_skipped(self, stats: GulpIngestionStats) -> GulpIngestionStats:
        return await stats.update(records_skipped=1)

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
            fs = await self._flush_buffer(
                index, fs, ws_id, req_id, flt, wait_for_refresh=True
            )
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
                await collab_api.session(),
                req_id,
                ws_id,
                fs=fs,
                force=True,
                file_done=True,
            )
        return status


def get_plugin_path(
    plugin: str, plugin_type: GulpPluginType = GulpPluginType.INGESTION
) -> str:
    """
    try different paths to get plugin path for a certain type

    Args:
        plugin (str): The name of the plugin.
        plugin_type (GulpPluginType, optional): The type of the plugin. Defaults to GulpPluginType.INGESTION.

    Returns:
        str: The plugin path.

    Raises:
        ObjectNotFound: If the plugin could not be found.
    """
    # try plain .py first
    # TODO: on license manager, disable plain .py load (only encrypted pyc)
    # get path according to plugin type
    path_plugins = config.path_plugins(plugin_type)
    plugin_path = muty.file.safe_path_join(path_plugins, f"{plugin}.py")
    paid_plugin_path = muty.file.safe_path_join(
        path_plugins, f"paid/{plugin}.py", allow_relative=True
    )
    plugin_path_pyc = muty.file.safe_path_join(path_plugins, f"{plugin}.pyc")
    paid_plugin_path_pyc = muty.file.safe_path_join(
        path_plugins, f"paid/{plugin}.pyc", allow_relative=True
    )
    logger().debug(
        "trying to load plugin %s from paths: %s, %s, %s, %s"
        % (plugin, plugin_path, paid_plugin_path, plugin_path_pyc, paid_plugin_path_pyc)
    )
    if muty.file.exists(paid_plugin_path):
        return paid_plugin_path
    if muty.file.exists(plugin_path):
        return plugin_path
    if muty.file.exists(paid_plugin_path_pyc):
        return paid_plugin_path_pyc
    if muty.file.exists(plugin_path_pyc):
        return plugin_path_pyc
    raise ObjectNotFound(f"Plugin {plugin} not found!")


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
        from_reduce (bool, optional, INTERNAL): Whether the plugin is being loaded from a __reduce__ call, defaults to False
        **kwargs (dict, optional): Additional keyword arguments:
    Returns:
        PluginBase: The loaded plugin.

    Raises:
        Exception: If the plugin could not be loaded.
    """
    logger().debug(
        "load_plugin %s, type=%s, ignore_cache=%r, kwargs=%s ..."
        % (plugin, plugin_type, ignore_cache, kwargs)
    )
    plugin_bare_name = plugin
    is_absolute_path = plugin.startswith("/")
    if is_absolute_path:
        plugin_bare_name = os.path.basename(plugin)

    if plugin_bare_name.lower().endswith(".py") or plugin_bare_name.lower().endswith(
        ".pyc"
    ):
        # remove extension
        plugin_bare_name = plugin_bare_name.rsplit(".", 1)[0]

    m = plugin_cache_get(plugin_bare_name)
    if ignore_cache:
        logger().debug("ignoring cache for plugin %s" % (plugin_bare_name))
        m = None

    if is_absolute_path:
        # plugin is an absolute path
        path = muty.file.abspath(plugin)
    else:
        # use plugin_type to load from the correct subfolder
        path = get_plugin_path(plugin_bare_name, plugin_type=plugin_type)

    module_name = f"gulp.plugins.{plugin_type.value}.{plugin_bare_name}"
    try:
        m = muty.dynload.load_dynamic_module_from_file(module_name, path)
    except Exception as ex:
        raise Exception(f"Failed to load plugin {path}: {str(ex)}") from ex

    mod: PluginBase = m.Plugin(path, pickled=from_reduce, **kwargs)
    logger().debug(
        "loaded plugin m=%s, mod=%s, name()=%s" % (m, mod, mod.display_name())
    )
    plugin_cache_add(m, plugin_bare_name)
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
        files = await muty.file.list_directory_async(
            subdir_path, "*.py*", recursive=True
        )
        for f in files:
            if "__init__" not in f and "__pycache__" not in f:
                try:
                    p = load_plugin(
                        os.path.splitext(os.path.basename(f))[0],
                        plugin_type,
                        ignore_cache=True,
                    )
                    n = {
                        "display_name": p.display_name(),
                        "type": str(p.type()),
                        "paid": "/paid/" in f.lower(),
                        "desc": p.desc(),
                        "filename": os.path.basename(p.path),
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


async def get_plugin_tags(
    plugin: str, t: GulpPluginType = GulpPluginType.INGESTION
) -> list[str]:
    """
    Get the tags for a given (ingestion) plugin.

    Args:
        plugin (str): The name of the plugin to get the tags for.
        t (GulpPluginType, optional): The type of the plugin. Defaults to GulpPluginType.INGESTION.
    Returns:
        list[str]: The tags for the given plugin.
    """
    p = load_plugin(plugin, plugin_type=t, ignore_cache=True)
    tags = p.tags()
    unload_plugin(p)
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

        logger().debug("unloading plugin: %s" % (mod.display_name()))
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
