"""
This module provides the GulpOpenSearch class, which serves as a singleton wrapper around OpenSearch client functionality.

It handles:
- Connection to OpenSearch instances with SSL/TLS support
- Creation, deletion, and management of datastreams and indices
- Document ingestion and querying
- Aggregation operations for statistics and filtering
- Templating and mapping operations
- Rebasing of documents
- Integration with the GulpCollab system for field mappings

The module serves as the primary interface between Gulp and OpenSearch, providing both low-level
operations (like bulk ingestion) and high-level functionality (like querying operations).

"""

# pylint: disable=too-many-lines

import asyncio
import orjson
import json
from importlib import resources as impresources
import os
import tempfile
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import aiofiles
import muty.dict
import muty.file
import muty.string
import muty.time
from elasticsearch import AsyncElasticsearch
from muty.log import MutyLogger
from opensearchpy import AsyncOpenSearch, NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession
from json_stream import streamable_list, streamable_dict
from gulp.api.collab.fields import GulpSourceFields
from gulp.api.collab.note import GulpNote
from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpCollabFilter, GulpRequestStatus
from gulp.api.collab.user import GulpUser
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import (
    GulpDocumentFilterResult,
    GulpIngestionFilter,
    GulpQueryFilter,
)
from gulp.api.ws_api import (
    WSDATA_DOCUMENTS_CHUNK,
    WSDATA_SOURCE_FIELDS_CHUNK,
    GulpDocumentsChunkPacket,
    GulpSourceFieldsChunkPacket,
    GulpWsSharedQueue,
)
from gulp.config import GulpConfig
from gulp.structs import ObjectNotFound

if TYPE_CHECKING:
    from gulp.api.opensearch.query import GulpQueryParameters
    from gulp.plugin import GulpPluginBase


class GulpOpenSearch:
    """
    singleton class to handle OpenSearch client connection.

    for ssl, it will use the CA certificate and client certificate/key if they exist in the certs directory (see config.path_certs()).

    they should be named os-ca.pem, os.pem, os.key.
    """

    _instance: "GulpOpenSearch" = None

    # to be used in dynamic templates
    UNMAPPED_PREFIX: str = "gulp.unmapped"

    def __init__(self):
        self._initialized: bool = True
        self._opensearch: AsyncOpenSearch = self._get_client()

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpOpenSearch":
        """
        returns the singleton instance of the OpenSearch client.

        Returns:
            GulpOpenSearch: The singleton instance of the OpenSearch client.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    async def reinit(self):
        """
        reinitializes the OpenSearch client in the singleton instance.
        """
        if self._opensearch is not None:
            await self._opensearch.close()

        self._opensearch = self._get_client()

    def _get_client(self) -> AsyncOpenSearch:
        """
        creates an OpenSearch client instance.

        Returns:
            AsyncOpenSearch: An instance of the OpenSearch client.

        """
        url = GulpConfig.get_instance().opensearch_url()
        verify_certs = GulpConfig.get_instance().opensearch_verify_certs()

        # split into user:pwd@host:port
        parsed = urlparse(url)

        # url = "%s://%s:***********@%s:%s" % (parsed.scheme, parsed.username, parsed.hostname, parsed.port)
        # MutyLogger.get_instance().debug('%s, opensearch hostname=%s, port=%d, user_id=%s, password=***********' % (url, parsed.hostname, parsed.port, parsed.username))

        host = parsed.scheme + "://" + parsed.hostname + ":" + str(parsed.port)
        ca = None
        certs_dir = GulpConfig.get_instance().path_certs()
        if certs_dir and parsed.scheme.lower() == "https":
            # https and certs_dir is set
            ca: str = muty.file.abspath(
                muty.file.safe_path_join(certs_dir, "os-ca.pem")
            )

            # check if client certificate exists. if so, it will be used
            client_cert = muty.file.safe_path_join(certs_dir, "os.pem")
            client_key = muty.file.safe_path_join(certs_dir, "os.key")
            if os.path.exists(client_cert) and os.path.exists(client_key):
                MutyLogger.get_instance().debug(
                    "using client certificate: %s, key=%s, ca=%s"
                    % (client_cert, client_key, ca)
                )
                return AsyncOpenSearch(
                    host,
                    use_ssl=True,
                    http_auth=(parsed.username, parsed.password),
                    ca_certs=ca,
                    client_cert=client_cert,
                    client_key=client_key,
                    verify_certs=verify_certs,
                )
            else:
                MutyLogger.get_instance().debug(
                    "no client certificate found, using CA certificate only: %s" % (ca)
                )
                return AsyncOpenSearch(
                    host,
                    use_ssl=True,
                    http_auth=(parsed.username, parsed.password),
                    ca_certs=ca,
                    verify_certs=verify_certs,
                )

        # no https
        el = AsyncOpenSearch(host, http_auth=(parsed.username, parsed.password))
        MutyLogger.get_instance().debug("created opensearch client: %s" % (el))
        return el

    async def shutdown(self) -> None:
        """
        Shutdown the OpenSearch client.

        Returns:
            None
        """
        MutyLogger.get_instance().debug(
            "shutting down opensearch client: %s" % (self._opensearch)
        )
        await self._opensearch.close()
        MutyLogger.get_instance().debug("opensearch client shutdown")
        self._opensearch = None

    async def check_alive(self) -> None:
        """
        Check if the OpenSearch client is alive.

        Raises:
            Exception: If the client is not reachable

        """
        res = await self._opensearch.info()
        MutyLogger.get_instance().debug("opensearch info: %s" % (res))

    async def datastream_get_key_value_mapping(
        self, index: str, return_raw_result: bool = False
    ) -> dict:
        """
        Get and parse mappings for the given datastream or index: it will result in a dict (if return_raw_result is not set) like:
        {
            "field1": "type",
            "field2": "type",
            ...
        }

        Args:
            index (str): an index/datastream to query
            return_raw_result (bool, optional): Whether to return the raw result (mapping + settings). Defaults to False.
        Returns:
            dict: The mapping dict.
        """

        def _parse_mappings_internal(d: dict, parent_key="", result=None) -> dict:
            if result is None:
                result = {}
            for k, v in d.items():
                new_key = "%s.%s" % (parent_key, k) if parent_key else k
                if isinstance(v, dict):
                    if "properties" in v:
                        _parse_mappings_internal(v["properties"], new_key, result)
                    elif "type" in v:
                        result[new_key] = v["type"]
                else:
                    result[new_key] = v
            return result

        try:
            res = await self._opensearch.indices.get_mapping(index=index)
        except Exception as e:
            MutyLogger.get_instance().warning(
                'no mapping for index "%s" found: %s' % (index, e)
            )
            return {}

        # MutyLogger.get_instance().debug("index_get_mapping: %s" % (orjson.dumps(res, option=orjson.OPT_INDENT_2).decode()))
        if return_raw_result:
            return res

        idx = list(res.keys())[0]
        properties = res[idx]["mappings"]["properties"]
        return _parse_mappings_internal(properties)

    async def datastream_get_mapping_by_src(
        self,
        sess: AsyncSession,
        operation_id: str,
        context_id: str,
        source_id: str,
        user_id: str,
    ) -> dict:
        """
        get source->fields mappings from the collab database

        Args:
            sess (AsyncSession): The database session.
            operation_id (str): The operation ID.
            context_id (str): The context ID.
            source_id (str): The source ID.
            user_id (str): The user ID.
        Returns:
            dict: The mapping dict (same as index_get_mapping with return_raw_result=False), or None if the mapping does not exist

        """
        # check if if a mapping already exists on the database
        flt = GulpCollabFilter(
            operation_ids=[operation_id],
            context_ids=[context_id],
            source_ids=[source_id],
        )
        fields: list[GulpSourceFields] = await GulpSourceFields.get_by_filter(
            sess,
            flt,
            throw_if_not_found=False,
            user_id=user_id,
        )
        if fields:
            # cache hit!
            return fields[0].fields

        return None

    def _extract_ids_from_query_operations_result(
        self, operations: list[dict]
    ) -> list[tuple[str, str, str]]:
        """
        Extracts operation_id, context_id and source_id from the query_operations result

        Args:
            operations (list): List of operation dictionaries

        Returns:
            list[tuple]: List of tuples containing (operation_id, context_id, source_id)

        Example:
            [
                ("test_operation", "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                 "fabae8858452af6c2acde7f90786b3de3a928289"),
                ("test_operation", "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
                 "60213bb57e849a624b7989c448b7baec75043a1b"),
                ...
            ]
        """
        result = []
        for operation in operations:
            operation_id = operation.get("id")
            for context in operation.get("contexts", []):
                context_id = context.get("id")
                for plugin in context.get("plugins", []):
                    for source in plugin.get("sources", []):
                        source_id = source.get("id")
                        result.append((operation_id, context_id, source_id))

        return result

    async def datastream_update_mapping_by_operation(
        self,
        index: str,
        user_id: str,
        operation_ids: list[str],
        context_ids: list[str] = None,
        source_ids: list[str] = None,
    ) -> None:
        """
        Updates the mappings for the given operation/context/source IDs on the given index/datastream.

        Args:
            index (str): The index/datastream name.
            user_id (str): The user ID.
            operation_ids (list[str]): The operation IDs to update mappings for.
            context_ids (list[str], optional): The context IDs to update mappings for. Defaults to None.
            source_ids (list[str], optional): The source IDs to update mappings for. Defaults to None.

        """
        MutyLogger.get_instance().debug(
            "updating mappings for index=%s, operation_ids=%s" % (index, operation_ids)
        )

        l = await self.query_operations(index, user_id)
        ids = self._extract_ids_from_query_operations_result(l)
        for op, ctx, src in ids:
            if (
                (not operation_ids or (operation_ids and op in operation_ids))
                and (not context_ids or (context_ids and ctx in context_ids))
                and (not source_ids or (source_ids and src in source_ids))
            ):
                await self.datastream_update_mapping_by_src(
                    index=index, operation_id=op, context_id=ctx, source_id=src
                )

                MutyLogger.get_instance().info(
                    "mappings created/updated for index=%s, operation_id=%s, context_id=%s, source_id=%s"
                    % (index, op, ctx, src)
                )

    async def datastream_update_mapping_by_src(
        self,
        index: str,
        operation_id: str = None,
        context_id: str = None,
        source_id: str = None,
        doc_ids: list[str] = None,
        user_id: str = None,
        ws_id: str = None,
        req_id: str = None,
        el: AsyncElasticsearch = None,
    ) -> tuple[dict, bool]:
        """
        create/update source->fields mappings for the given operation/context/source on the collab database

        - SOURCE_FIELDS_CHUNK are streamed to the websocket ws_id if it is not None.

        WARNING: this call may take long time, so it is better to offload it to a worker coroutine.

        Args:
            index (str): The index/datastream name.
            operation_id (str): The operation ID, may be None to indicate all operations.
            context_id (str, optional): The context ID, may be None to indicate all contexts.
            source_id (str, optional): The source ID, may be None to indicate all sources.
            doc_ids (list[str], optional): limit to these document IDs. Defaults to None.
            user_id (str, optional): The user ID. Defaults to None.
            ws_id (str, optional): The websocket ID to stream SOURCE_FIELDS_CHUNK during the loop. Defaults to None.
            req_id (str, optional): The request ID. Defaults to None.
            el: AsyncElasticsearch, optional): The Elasticsearch client. Defaults to None (use the default OpenSearch client).
        Returns:
            dict: The mapping dict (same as index_get_mapping with return_raw_result=False), may be empty if no documents are found

        """

        MutyLogger.get_instance().debug(
            "creating/updating source->fields mapping for source_id=%s, context_id=%s, operation_id=%s, doc_ids=%s ..."
            % (source_id, context_id, operation_id, doc_ids)
        )

        from gulp.api.opensearch.query import GulpQueryParameters

        options = GulpQueryParameters()
        options.limit = 1000
        options.fields = ["*"]
        if not operation_id:
            # all
            q = {"query": {"match_all": {}}}
        else:
            q = {
                "query": {
                    "query_string": {"query": "gulp.operation_id: %s" % (operation_id)}
                }
            }
            if context_id:
                q["query"]["query_string"]["query"] += " AND gulp.context_id: %s" % (
                    context_id
                )
            if source_id:
                q["query"]["query_string"]["query"] += " AND gulp.source_id: %s" % (
                    source_id
                )
            if doc_ids:
                # limit to these document IDs
                q["query"]["query_string"]["query"] += " AND _id: (%s)" % (
                    " OR ".join(doc_ids)
                )

        # get mapping
        mapping = await self.datastream_get_key_value_mapping(index)
        filtered_mapping = {}

        # loop with query_raw until there's data and update filtered_mapping
        processed: int = 0
        total_hits: int = 0
        while True:
            last: bool = False
            parsed_options = options.parse()
            total_hits, docs, search_after = await self._search_dsl_internal(
                index, parsed_options, q, el=el, raise_on_error=False
            )

            options.search_after = search_after
            processed += len(docs)
            # MutyLogger.get_instance().debug("processed=%d, total=%d" % (processed, total_hits))
            if processed >= total_hits:
                # last chunk
                last = True

            # update filtered_mapping with the current batch of documents
            chunk: dict = {}
            for doc in docs:
                for k in mapping.keys():
                    if k in doc:
                        if k not in filtered_mapping:
                            filtered_mapping[k] = mapping[k]
                            if k not in chunk:
                                chunk[k] = mapping[k]

            send: bool = True
            if ws_id:
                if not last:
                    # avoid sending empty chunks except last
                    if not chunk:
                        send = False

                if send:
                    # send this chunk over the ws
                    p = GulpSourceFieldsChunkPacket(
                        operation_id=operation_id,
                        source_id=source_id,
                        context_id=context_id,
                        fields=chunk,
                        last=last,
                    )
                    wsq = GulpWsSharedQueue.get_instance()
                    await wsq.put(
                        type=WSDATA_SOURCE_FIELDS_CHUNK,
                        ws_id=ws_id,
                        user_id=user_id,
                        req_id=req_id,
                        data=p.model_dump(exclude_none=True),
                    )

            if last:
                # no more results
                break

        if not filtered_mapping:
            MutyLogger.get_instance().warning(
                "no documents found for source_id=%s, context_id=%s, operation_id=%s"
                % (
                    source_id,
                    context_id,
                    operation_id,
                )
            )
            return {}

        # sort the keys
        # filtered_mapping = dict(sorted(filtered_mapping.items()))

        # store on database
        MutyLogger.get_instance().debug(
            "found %d mappings, storing on db ..." % (len(filtered_mapping))
        )
        async with GulpCollab.get_instance().session() as sess:
            await GulpSourceFields.create(
                sess, user_id, operation_id, context_id, source_id, filtered_mapping
            )

        return filtered_mapping

    async def index_template_delete(self, index: str) -> None:
        """
        Delete the index template for the given index/datastream.

        Args:
            index (str): The index/datastream name.
        """
        try:
            template_name = "%s-template" % (index)
            MutyLogger.get_instance().debug(
                "deleting index template: %s ..." % (template_name)
            )
            res = await self._opensearch.indices.delete_index_template(
                name=template_name
            )
            MutyLogger.get_instance().debug(
                "index template deleted: %s, res=%s" % (template_name, res)
            )
        except Exception as e:
            MutyLogger.get_instance().warning("index template does not exist: %s" % (e))

    async def index_template_get(self, index: str) -> dict:
        """
        Get the index template for the given index/datastream.

        Args:
            index (str): The index/datastream name.

        Returns:
            dict: The index template, like:

            {
                "index_patterns": [
                    "test_operation"
                ],
                "template": {
                    "settings": {
                    "index": {
                        "number_of_replicas": "0",
                        "mapping": {
                        "total_fields": {
                            "limit": "10000"
                        }
                        },
                        "refresh_interval": "5s"
                    }
                    },
                    "mappings": {
                        "numeric_detection": false,
                        "_meta": {
                            "version": "8.8.0-dev"
                        },
                        "dynamic_templates: [ ... ],
                        ...
                    }
                },
                ...
            }
        """
        template_name = "%s-template" % (index)
        MutyLogger.get_instance().debug(
            "getting index template: %s ..." % (template_name)
        )

        try:
            res = await self._opensearch.indices.get_index_template(name=template_name)
        except Exception as e:
            raise ObjectNotFound(
                "no template found for datastream/index %s" % (index)
            ) from e

        return res["index_templates"][0]["index_template"]

    async def index_template_put(self, index: str, d: dict) -> dict:
        """
        Put the index template for the given index/datastream.

        Args:
            index (str): The index/datastream name.
            d (dict): The index template.

        Returns:
            dict: The response from the OpenSearch client.
        """
        template_name = "%s-template" % (index)
        MutyLogger.get_instance().debug(
            "putting index template: %s ..." % (template_name)
        )
        headers = {"accept": "application/json", "content-type": "application/json"}
        res = await self._opensearch.indices.put_index_template(
            name=template_name, body=d, headers=headers
        )
        MutyLogger.get_instance().debug(
            "index template set for index: %s, res=%s" % (index, res)
        )
        return res

    async def index_template_set_from_file(
        self, index: str, path: str, apply_patches: bool = True
    ) -> dict:
        """
        Asynchronously sets an index template in OpenSearch from a JSON file.

        Args:
            index (str): The name of the index/datastream to set the template for.
            path (str): The file path to the JSON file containing the index template.
            apply_patches (bool, optional): Whether to apply specific patches to the mappings and settings before setting the index template. Defaults to True.
        Returns:
            dict: The response from OpenSearch after setting the index template.
        """

        MutyLogger.get_instance().debug(
            'loading index template from file "%s" ...' % (path)
        )
        d = muty.dict.from_json_file(path)
        return await self.build_and_set_index_template(index, d, apply_patches)

    async def build_and_set_index_template(
        self, index: str, d: dict, apply_patches: bool = True
    ) -> dict:
        """
        Sets an index template in OpenSearch, applying the needed patches to the mappings and settings.

        Args:
            index (str): The name of the index for which the template is being set.
            d (dict): The dictionary containing the template, mappings, and settings for the index.
            apply_patches (bool, optional): Whether to apply specific patches to the mappings and settings before setting the index template. Defaults to True.
        Returns:
            dict: The response from the OpenSearch client after setting the index template.
        Raises:
            ValueError: If the 'template' or 'mappings' key is not found in the provided dictionary.
        Notes:
            - The function modifies the provided dictionary to include default settings and mappings if they are not present.
            - It applies specific patches to the mappings and settings before setting the index template.
            - If the OpenSearch cluster is configured for a single node, the number of replicas is set to 0 to optimize performance.
        """
        MutyLogger.get_instance().debug('setting index template for "%s" ...' % (index))
        template = d.get("template", None)
        if template is None:
            raise ValueError('no "template" key found in the index template')
        mappings = template.get("mappings", None)
        if mappings is None:
            raise ValueError('no "mappings" key found in the index template')
        settings = template.get("settings", None)
        if settings is None:
            template["settings"] = {}
            settings = template["settings"]
        if settings.get("index", None) is None:
            settings["index"] = {}
        if settings["index"].get("mapping", None) is None:
            settings["index"]["mapping"] = {}
        dt = mappings.get("dynamic_templates", None)
        if dt is None:
            dt = []
            mappings["dynamic_templates"] = dt

        # apply our patches
        d["index_patterns"] = [index]
        d["data_stream"] = {}
        d["priority"] = 100

        # always set gulp specific mappings
        mappings["properties"]["gulp"] = {
            "properties": {
                "event_code": {"type": "long"},
                "context_id": {"type": "keyword"},
                "operation_id": {"type": "keyword"},
                "source_id": {"type": "keyword"},
                "timestamp": {"type": "long"},
                "timestamp_invalid": {"type": "boolean"},
            }
        }

        # add dynamic templates for unmapped fields
        dtt = []

        ignore_above = GulpConfig.get_instance().index_dynamic_keyword_ignore_above()
        if not ignore_above:
            # use default
            ignore_above = 1024

        MutyLogger.get_instance().debug("index ignore_above: %d" % (ignore_above))

        # TODO: this may be handy but leave it commented for now ....
        # dtt.append(
        #     {
        #         # all strings as keywords + text
        #         "all_strings_as_keywords": {
        #             "match_mapping_type": "string",
        #             "mapping": {
        #                 "type": "text",
        #                 "analyzer": "standard",
        #                 "fields": {
        #                     "keyword": {
        #                         "type": "keyword",
        #                         "ignore_above": ignore_above,
        #                     }
        #                 }
        #             },
        #         }
        #     }
        # )

        # handle object fields
        dtt.append(
            {
                "objects": {
                    "path_match": "%s.*" % (self.UNMAPPED_PREFIX),
                    "match_mapping_type": "object",
                    "mapping": {"type": "object", "dynamic": True},
                }
            }
        )

        # handle string fields (all unmapped to keyword)
        dtt.append(
            {
                "hex_values": {
                    "path_match": "%s.*" % (self.UNMAPPED_PREFIX),
                    "match_pattern": "regex",
                    "match": "^0[xX][0-9a-fA-F]+$",
                    "mapping": {"type": "keyword"},
                }
            }
        )
        dtt.append(
            {
            "unmapped_fields": {
                "path_match": "%s.*" % (self.UNMAPPED_PREFIX),
                "match_mapping_type": "*",
                "mapping": {
                    "type": "keyword",
                    "fields": {
                        "text": { # added multified in order to better support matching in partial queries (by enabling text field and using standard normalizer)
                            "type": "text"
                            }
                        }
                    },
                }
            }
        )

        dtt.extend(dt)
        mappings["dynamic_templates"] = dtt

        if apply_patches:
            # only set these if we do not want to use the template as is
            # mappings['date_detection'] = True
            # mappings['numeric_detection'] = True
            # mappings['dynamic'] = False

            mappings["numeric_detection"] = False
            mappings["date_detection"] = False
            mappings["properties"]["@timestamp"] = {
                "type": "date_nanos",
                "format": "strict_date_optional_time_nanos",
            }

            # support for original event both as keyword and text
            # keyword is case sensitive, text is not
            mappings["properties"]["event"]["properties"]["original"] = {
                "type": "wildcard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": ignore_above}},
            }

            settings["index"]["mapping"]["total_fields"] = {
                "limit": GulpConfig.get_instance().index_template_default_total_fields_limit()
            }
            settings["index"][
                "refresh_interval"
            ] = GulpConfig.get_instance().index_template_default_refresh_interval()

            if not GulpConfig.get_instance().opensearch_multiple_nodes():
                # optimize for single node
                # this also removes "yellow" node in single node mode
                # this also removes "yellow" node in single node mode
                MutyLogger.get_instance().warning("setting number_of_replicas to 0")
                settings["index"]["number_of_replicas"] = 0

        # write template
        await self.index_template_put(index, d)
        return d

    async def datastream_list(self) -> list[dict]:
        """
        Retrieves a list of datastream names (with associated indices) from OpenSearch.

        Returns:
            list[dict]: The list of datastreams with their backing index as
            {
                "name": "datastream_name",
                "indexes": ["index1", "index2", ...], <== this is always a list of one element for gulp datastreams
                "template": "template_name"
            }
        """
        headers = {"accept": "text/plain,application/json"}
        l = await self._opensearch.indices.get_data_stream(headers=headers)
        # MutyLogger.get_instance().debug(orjson.dumps(l, option=orjson.OPT_INDENT_2).decode())
        ll = []
        ds = l.get("data_streams", [])
        for c in ds:
            # get count
            count = await self.datastream_get_count(c["name"])
            cc = {
                "name": c["name"],
                "doc_count": count,
                "indexes": c["indices"],
                "template": c.get("template", None),
            }
            ll.append(cc)

        return ll

    async def datastream_delete(self, ds: str, throw_on_error: bool = False) -> None:
        """
        Delete the datastream and associated index and template from OpenSearch.

        Args:
            ds (str): The name of the datastream to delete.

        Returns:
            None
        """
        # params = {"ignore_unavailable": "true"}
        headers = {"accept": "application/json"}
        exists = await self.datastream_exists(ds)
        try:
            if not exists and throw_on_error:
                raise ObjectNotFound("datastream %s does not exist" % (ds))

            res = await self._opensearch.indices.delete_data_stream(ds, headers=headers)
            MutyLogger.get_instance().debug(
                'deleted datastream "%s", res=%s' % (ds, res)
            )
            try:
                # delete index too
                res = await self._opensearch.indices.delete(ds)
                MutyLogger.get_instance().debug(
                    'deleted index "%s", res=%s' % (ds, res)
                )
            except:
                pass
        finally:
            # also (try to) delete the corresponding template
            try:
                await self.index_template_delete(ds)
            except Exception as e:
                MutyLogger.get_instance().error(
                    "cannot delete template for index/datastream: %s (%s)" % (ds, e)
                )

    async def datastream_exists(self, ds: str) -> bool:
        """
        Check if a datastream exists in OpenSearch.

        Args:
            ds (str): The name of the datastream to check.

        Returns:
            bool: True if the datastream exists, False otherwise.
        """
        try:
            await self._opensearch.indices.get_data_stream(name=ds)
            return True
        except NotFoundError:
            return False

    async def datastream_get_count(self, ds: str) -> bool:
        """
        Get the count of documents in the datastream.

        Args:
            ds (str): The name of the datastream to check.

        Raises:
            NotFoundError: If the datastream does not exist.
        Returns:
            int: The count of documents in the datastream.
        """
        res = await self._opensearch.count(index=ds)
        return res["count"]

    def _default_index_template_path(self) -> str:
        """
        Returns the path of the opensearch index template file.

        Returns:
            str: The path to the index template file.
        """
        p = impresources.files("gulp.api.mapping.index_template")
        default_path = muty.file.safe_path_join(p, "template.json")
        return default_path

    async def datastream_create(
        self, ds: str, index_template: str = None, delete_first: bool = True
    ) -> dict:
        """
        (re)creates the OpenSearch datastream (with backing index) and associates the index template from configuration (or uses the default).

        Args:
            ds(str): The name of the datastream to be created, the index template will be re/created as "<index_name>-template".
                if it already exists, it will be deleted first.
            index_template (str, optional): path to the index template to use. Defaults to None (use the default index template).
            delete_first (bool, optional): Whether to delete the datastream first if it exists. Defaults to True.

        Returns:
            dict: The response from the OpenSearch client after creating the datastream.
        """
        if delete_first:
            # attempt to delete the datastream first, if it exists
            MutyLogger.get_instance().debug(
                "datastream_create, deleting datastream before creation: %s ..." % (ds)
            )
            await self.datastream_delete(ds)

        # create index template, check if we are overriding the default index template.
        # if so, we only apply gulp-related patching and leaving everything else as is
        apply_patches = True
        if index_template is None:
            template_path = self._default_index_template_path()
        else:
            template_path = index_template
            MutyLogger.get_instance().debug(
                "datastream_create, using custom index template: %s ..."
                % (template_path)
            )
            apply_patches = False
        await self.index_template_set_from_file(
            ds, template_path, apply_patches=apply_patches
        )

        try:
            # create datastream
            headers = {"accept": "application/json", "content-type": "application/json"}
            r = await self._opensearch.indices.create_data_stream(ds, headers=headers)
            MutyLogger.get_instance().debug(
                "datastream_create, datastream created: %s" % (r)
            )
            return r
        except Exception as e:
            # also delete the index template
            MutyLogger.get_instance().error(
                "error creating datastream %s, deleting template" % (ds)
            )
            await self.index_template_delete(ds)
            raise e

    async def datastream_create_from_raw_dict(
        self, ds: str, index_template: dict = None, delete_first: bool = True
    ) -> dict:
        """
        (re)creates the OpenSearch datastream (with backing index) and associates the index template from a dictionary (or uses the default).

        Args:
            ds(str): The name of the datastream to be created, the index template will be re/created as "<index_name>-template".
                if it already exists, it will be deleted first.
            index_template (dict, optional): The index template to use (same format as the output of index_template_get). Defaults to None (use the default index template).
            delete_first (bool, optional): Whether to delete the datastream first if it exists. Defaults to True.

        Returns:
            dict: The response from the OpenSearch client after creating the datastream.
        """
        if delete_first:
            # attempt to delete the datastream first, if it exists
            MutyLogger.get_instance().debug(
                "datastream_create_from_raw_dict, re/creating datastream: %s ..." % (ds)
            )
            await self.datastream_delete(ds)

        # ensure the index template is set
        try:
            if not index_template:
                # use default
                template_path = self._default_index_template_path()
                await self.index_template_set_from_file(ds, template_path)
            else:
                # use provided as-is, just ensure the index is set
                index_template["index_patterns"] = [ds]
                await self.index_template_put(ds, index_template)

            # create datastream
            headers = {"accept": "application/json", "content-type": "application/json"}
            r = await self._opensearch.indices.create_data_stream(ds, headers=headers)
            MutyLogger.get_instance().debug(
                "datastream_create_from_raw_dict, datastream created: %s" % (r)
            )
            return r
        except Exception as e:
            # delete the index template
            await self.index_template_delete(ds)
            raise e

    def _bulk_docs_result_to_ingest_chunk(
        self, bulk_docs: list[tuple[dict, dict]], errors: list[dict] = None
    ) -> list[dict]:
        """
        Extracts the ingested documents from a bulk ingestion result, excluding the ones that failed.

        Args:
            bulk_docs (list[tuple[dict, dict]]): The bulk ingestion documents.
            errors (list[dict], optional): The errors from the bulk ingestion. Defaults to None.

        Returns:
            list[dict]: The ingested documents.
        """
        if not errors:
            return [
                {**doc, "_id": create_doc["create"]["_id"]}
                for create_doc, doc in zip(bulk_docs[::2], bulk_docs[1::2])
            ]

        error_ids = {error["create"]["_id"] for error in errors}
        result = [
            {**doc, "_id": create_doc["create"]["_id"]}
            for create_doc, doc in zip(bulk_docs[::2], bulk_docs[1::2])
            if create_doc["create"]["_id"] not in error_ids
        ]

        return result

    async def update_documents(
        self,
        index: str,
        docs: list[dict],
        wait_for_refresh: bool = False,
        replace: bool = False,
    ) -> tuple[int, list[dict]]:
        """
        updates documents in an OpenSearch datastream using update_by_query.

        NOTE: this method is not recommended for large updates as it can be slow and resource intensive, but it is ok to use for small updates.

        Args:
            index (str): Name of the datastream/index to update documents in
            docs (list[dict]): List of documents to update. Each doc must have _id field
            wait_for_refresh (bool): Whether to wait for index refresh. Defaults to False
            replace (bool): Whether to completely replace documents keeping only _id. Defaults to False

        Returns:
            tuple:
            - number of successfully updated documents
            - list of errors if any occurred

        Raises:
            ValueError: If doc is missing _id field
        """
        if not docs:
            return 0, []

        # Build document updates
        update_operations = []
        for doc in docs:
            if "_id" not in doc:
                raise ValueError("Document missing _id field")

            doc_id = doc["_id"]
            update_fields = {k: v for k, v in doc.items() if k != "_id"}

            if replace:
                # replace entire document except _id
                operation = {
                    "script": {
                        "source": "ctx._source.clear(); for (entry in params.updates.entrySet()) { ctx._source[entry.getKey()] = entry.getValue(); }",
                        "lang": "painless",
                        "params": {"updates": update_fields},
                    },
                    "query": {"term": {"_id": doc_id}},
                }
            else:
                # update only specified fields
                operation = {
                    "script": {
                        "source": """
                            for (entry in params.updates.entrySet()) {
                                ctx._source[entry.getKey()] = entry.getValue();
                            }
                        """,
                        "lang": "painless",
                        "params": {"updates": update_fields},
                    },
                    "query": {"term": {"_id": doc_id}},
                }

            update_operations.append(operation)

        # set parameters
        params = {"conflicts": "abort", "wait_for_completion": "true"}
        if wait_for_refresh:
            params["refresh"] = "true"

        # Execute updates
        headers = {"accept": "application/json", "content-type": "application/json"}

        success_count = 0
        errors = []

        try:
            for operation in update_operations:
                try:
                    res = await self._opensearch.update_by_query(
                        index=index, body=operation, params=params, headers=headers
                    )
                    success_count += res.get("updated", 0)
                except Exception as e:
                    errors.append({"query": operation["query"], "error": str(e)})

        except Exception as e:
            MutyLogger.get_instance().error(f"error updating documents: {str(e)}")
            return 0, [{"error": str(e)}]

        return success_count, errors

    async def bulk_ingest(
        self,
        index: str,
        docs: list[dict],
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> tuple[int, int, list[dict], bool]:
        """
        ingests a list of GulpDocument into OpenSearch.

        Args:
            index (str): Name of the index (or datastream) to ingest documents to.
            docs (list[dict]): The documents to be ingested
            flt (GulpIngestionFilter, optional): The filter parameters. Defaults to None.
            wait_for_refresh (bool, optional): Whether to wait for the refresh(=refreshed index is available for searching) to complete. Defaults to False.

        Returns:
            tuple:
            - number of skipped (because already existing=duplicated) events
            - number of failed events
            - list of ingested documents
            - whether the ingestion was successful after retrying
        """

        # Filter documents if needed
        filtered_docs = docs
        if flt:
            filtered_docs = [
                doc
                for doc in docs
                if GulpIngestionFilter.filter_doc_for_ingestion(doc, flt)
                != GulpDocumentFilterResult.SKIP
            ]

        if not filtered_docs:
            MutyLogger.get_instance().warning(f"No document to ingest (flt={flt})")
            return 0, 0, [], False

        # Prepare bulk operation format
        bulk_docs = []
        for doc in filtered_docs:
            bulk_docs.append({"create": {"_id": doc["_id"]}})
            bulk_docs.append({k: v for k, v in doc.items() if k != "_id"})

        # Set request parameters
        timeout = GulpConfig.get_instance().opensearch_request_timeout()
        if timeout > 0:
            params = {"timeout": timeout}
        else:
            params = {}

        if wait_for_refresh:
            params["refresh"] = "wait_for"

        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        # Execute bulk operation with retries
        max_retries = GulpConfig.get_instance().ingestion_retry_max()
        attempt = 0
        success_after_retry = False
        res = None

        while attempt < max_retries:
            try:
                res = await self._opensearch.bulk(
                    body=bulk_docs, index=index, params=params, headers=headers
                )

                # Check for server errors that would require retry
                if res["errors"]:
                    for item in res["items"]:
                        if item["create"]["status"] >= 500:
                            raise Exception(
                                f"bulk ingestion failed with status {item['create']['status']}: {item['create']['error']}"
                            )

                if attempt > 0:
                    # mark success after retry if applicable
                    # (success is intended the bulk operation did not raised an exception, including error items with status 500)
                    success_after_retry = True

                break  # Success, exit retry loop

            except Exception as ex:
                attempt += 1
                if attempt < max_retries:
                    MutyLogger.get_instance().exception(ex)
                    retry_delay = GulpConfig.get_instance().ingestion_retry_delay()
                    MutyLogger.get_instance().warning(
                        f"bulk ingestion failed, retrying in {retry_delay}s (attempt {attempt}/{max_retries})"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    raise ex  # All retries failed

        # Process results
        skipped = failed = 0
        ingested = []

        if res["errors"]:
            # Count skipped (already exists) and failed documents
            skipped = sum(1 for item in res["items"] if item["create"]["status"] == 409)
            failed = sum(
                1
                for item in res["items"]
                if item["create"]["status"] not in [200, 201, 409]
            )

            if failed > 0:
                failed_items = [
                    item
                    for item in res["items"]
                    if item["create"]["status"] not in [200, 201]
                ]
                s = orjson.dumps(failed_items, option=orjson.OPT_INDENT_2).decode()
                MutyLogger.get_instance().error(
                    f"{failed} failed ingestion, {skipped} skipped: {muty.string.make_shorter(s, max_len=10000)}"
                )

            # Extract successfully ingested documents
            error_ids = {
                item["create"]["_id"]
                for item in res["items"]
                if item["create"]["status"] not in [200, 201]
            }
            ingested = [
                {**doc, "_id": action["create"]["_id"]}
                for action, doc in zip(bulk_docs[::2], bulk_docs[1::2])
                if action["create"]["_id"] not in error_ids
            ]
        else:
            # All documents were successfully ingested
            ingested = [
                {**doc, "_id": action["create"]["_id"]}
                for action, doc in zip(bulk_docs[::2], bulk_docs[1::2])
            ]

        if skipped > 0:
            MutyLogger.get_instance().debug(
                f"{skipped} skipped, {failed} failed in this bulk ingestion of {len(filtered_docs)} documents!"
            )

        if failed > 0:
            MutyLogger.get_instance().critical(
                "Failed is set, ingestion format needs to be fixed!"
            )

        return skipped, failed, ingested, success_after_retry

    async def rebase(
        self,
        index: str,
        dest_index: str,
        offset_msec: int,
        flt: GulpQueryFilter = None,
        rebase_script: str = None,
    ) -> dict:
        """
        Rebase documents from one OpenSearch index to another with a timestamp offset.
        Args:
            index (str): The source index name.
            dest_index (str): The destination index name.
            offset_msec (int): The offset in milliseconds from unix epoch to adjust the '@timestamp' field.
            flt (GulpQueryFilter, optional): if set, it will be used to rebase only a subset of the documents. Defaults to None.
            rebase_script (str, optional): a [painless script](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-guide.html) to customize rebasing. Defaults to None (use the default script).
                the rebase script takes a single parameter `nsec_offset` which is the offset in nanoseconds to apply to the '@timestamp' field.
        Returns:
            dict: The response from the OpenSearch reindex operation.

        """
        MutyLogger.get_instance().debug(
            "rebase index %s to %s with offset=%d, flt=%s ..."
            % (index, dest_index, offset_msec, flt)
        )
        if not flt:
            flt = GulpQueryFilter()

        q = flt.to_opensearch_dsl()

        if rebase_script:
            convert_script = rebase_script
        else:
            convert_script = """
                if (ctx._source['@timestamp'] != null && ctx._source['@timestamp'] != '0') {
                    ZonedDateTime ts = ZonedDateTime.parse(ctx._source['@timestamp']);
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern('yyyy-MM-dd\\'T\\'HH:mm:ss.nnnnnnnnnX');
                    ZonedDateTime new_ts = ts.plusNanos(params.offset_nsec);
                    ctx._source['@timestamp'] = new_ts.format(fmt);
                    ctx._source['gulp.timestamp'] += params.offset_nsec;
                }
            """
        body: dict = {
            "source": {"index": index, "query": q["query"]},
            "dest": {"index": dest_index, "op_type": "create"},
            "script": {
                "lang": "painless",
                "source": convert_script,
                "params": {
                    "offset_nsec": offset_msec * muty.time.MILLISECONDS_TO_NANOSECONDS,
                },
            },
        }
        params: dict = {"refresh": "true", "wait_for_completion": "true", "timeout": 0}
        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        MutyLogger.get_instance().debug("rebase body=%s" % (body))
        res = await self._opensearch.reindex(body=body, params=params, headers=headers)
        MutyLogger.get_instance().debug("rebase result=%s" % (res))
        return res

    async def index_refresh(self, index: str) -> None:
        """
        Refresh an index(=make changes available to search) in OpenSearch.

        Args:
            index (str): Name of the index (or datastream) to refresh.

        Returns:
            None
        """
        MutyLogger.get_instance().debug("refreshing index: %s" % (index))
        res = await self._opensearch.indices.refresh(index=index)
        MutyLogger.get_instance().debug("refreshed index: %s" % (res))

    async def delete_data_by_operation(
        self, index: str, operation_id: str, refresh: bool = True
    ) -> dict:
        """
        Deletes all data from an index that matches the given operation.

        Args:
            index (str): Name of the index (or datastream) to delete data from.
            operation_id (str): The ID of the operation.
            refresh (bool, optional): Whether to refresh the index after deletion. Defaults to True.

        Returns:
            None
        """
        return await self._delete_data_by_operation_source_context(
            index=index, operation_id=operation_id, refresh=refresh
        )

    async def delete_data_by_context(
        self, index: str, operation_id: str, context_id: str, refresh: bool = True
    ) -> dict:
        """
        Deletes all data from an index that matches the given operation and context.

        Args:
            index (str): Name of the index (or datastream) to delete data from.
            operation_id (str): The ID of the operation.
            context_id (str): The ID of the context.
            refresh (bool, optional): Whether to refresh the index after deletion. Defaults to True.

        Returns:
            None
        """
        return await self._delete_data_by_operation_source_context(
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            refresh=refresh,
        )

    async def delete_data_by_source(
        self,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        refresh: bool = True,
    ) -> dict:
        """
        Deletes all data from an index that matches the given operation, context and source

        Args:
            index (str): Name of the index (or datastream) to delete data from.
            operation_id (str): The ID of the operation.
            context_id (str): The ID of the context.
            source_id (str): The ID of the source
            refresh (bool, optional): Whether to refresh the index after deletion. Defaults to True.

        Returns:
            None
        """
        return await self._delete_data_by_operation_source_context(
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            refresh=refresh,
        )

    async def _delete_data_by_operation_source_context(
        self,
        index: str,
        operation_id: str,
        context_id: str = None,
        source_id: str = None,
        refresh: bool = True,
    ) -> dict:

        # build bool query with must clauses
        must_clauses = [{"term": {"gulp.operation_id": operation_id}}]

        if context_id:
            must_clauses.append({"term": {"gulp.context_id": context_id}})

        if source_id:
            must_clauses.append({"term": {"gulp.source_id": source_id}})

        q = {"query": {"bool": {"must": must_clauses}}}

        params = None
        if refresh:
            params = {"refresh": "true"}
        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        res = await self._opensearch.delete_by_query(
            index=index, body=q, params=params, headers=headers
        )
        MutyLogger.get_instance().debug("delete_by_query result=%s" % (res))
        return res

    def _parse_query_max_min(self, d: dict) -> dict:
        """
        Parse the query result of query_max_min_per_field.

        Args:
            d (dict): The query result.

        Returns:
            dict: The parsed result as
            {
                'buckets': list[dict], # the matched events
                'total': int, # total matches
            }
        """
        buckets = d["by_type"]["buckets"]
        dd = {
            "buckets": [
                {
                    bucket["key"]: {
                        "doc_count": bucket["doc_count"],
                        "max_event.code": int(bucket["max_event.code"]["value"]),
                        "min_gulp.timestamp": int(
                            bucket["min_gulp.timestamp"]["value"]
                        ),
                        "max_gulp.timestamp": int(
                            bucket["max_gulp.timestamp"]["value"]
                        ),
                        "min_event.code": int(bucket["min_event.code"]["value"]),
                    }
                }
                for bucket in buckets
            ],
            "total": sum(bucket["doc_count"] for bucket in buckets),
        }
        return dd

    async def query_max_min_per_field(
        self,
        index: str,
        group_by: str = None,
        flt: GulpQueryFilter = None,
    ):
        """
        Queries the maximum and minimum @gulp.timestamp and event.code in an index, grouping per type if specified.

        Args:
            el (AsyncOpenSearch): The OpenSearch client.
            index (str): Name of the index (or datastream) to query.
            group_by (str): The field to group by (None to consider all fields).
            flt (GulpQueryFilter): The query filter.

        Returns: a dict like
            {
                'buckets': [
                    {
                        'type1': {
                            'doc_count': 123,
                            'max_event.code': 123,
                            'min_gulp.timestamp': 123,
                            'max_gulp.timestamp': 123,
                            'min_event.code': 123
                        }
                    },
                    ...
                ],
                total: 123
        """
        if not flt:
            flt = GulpQueryFilter()

        aggregations = {
            "count": {"value_count": {"field": "gulp.timestamp"}},
            "max_gulp.timestamp": {"max": {"field": "gulp.timestamp"}},
            "min_gulp.timestamp": {"min": {"field": "gulp.timestamp"}},
            "max_event.code": {"max": {"field": "gulp.event_code"}},
            "min_event.code": {"min": {"field": "gulp.event_code"}},
        }
        if group_by is not None:
            aggregations = {
                "by_type": {
                    "terms": {"field": group_by},
                    "aggs": aggregations,
                }
            }
            MutyLogger.get_instance().debug(
                f"aggregations with group_by={group_by}: {
                    orjson.dumps(aggregations, option=orjson.OPT_INDENT_2).decode()}"
            )

        q = flt.to_opensearch_dsl()
        MutyLogger.get_instance().debug(
            f"query_max_min_per_field: q={orjson.dumps(q, option=orjson.OPT_INDENT_2).decode()}"
        )
        body = {
            "track_total_hits": True,
            "query": q["query"],
            "aggregations": aggregations,
        }
        headers = {
            "content-type": "application/json",
        }

        res = await self._opensearch.search(body=body, index=index, headers=headers)
        hits = res["hits"]["total"]["value"]
        if not hits:
            raise ObjectNotFound()

        if group_by:
            MutyLogger.get_instance().debug(
                f"group_by={group_by}, res['aggregations']={
                    orjson.dumps(res['aggregations'], option=orjson.OPT_INDENT_2).decode()}"
            )
            return self._parse_query_max_min(res["aggregations"])

        # no group by, standardize the result
        d = {
            "by_type": {
                "buckets": [
                    {
                        "key": "*",
                        "doc_count": res["aggregations"]["count"]["value"],
                        "max_event.code": res["aggregations"]["max_event.code"],
                        "min_event.code": res["aggregations"]["min_event.code"],
                        "min_gulp.timestamp": res["aggregations"]["min_gulp.timestamp"],
                        "max_gulp.timestamp": res["aggregations"]["max_gulp.timestamp"],
                    }
                ]
            }
        }
        return self._parse_query_max_min(d)

    async def _parse_operation_aggregation(
        self, aggregations: dict, user_id: str
    ) -> list[dict]:
        """
        parse OpenSearch operations aggregations and match with collab database operations.

        Args:
            aggregations (dict): Raw OpenSearch aggregations results
            user_id (str): The user ID to filter operations
        Returns:
            list[dict]: Parsed operations with context and source details
        """
        # get all operations from collab db
        # MutyLogger.get_instance().debug(f"parsing operations aggregations: {orjson.dumps(aggregations, option=orjson.OPT_INDENT_2).decode()}")
        async with GulpCollab.get_instance().session() as sess:
            all_operations = await GulpOperation.get_by_filter(
                sess,
                user_id=user_id,
            )

        # create operation lookup map
        operation_map = {op.id: op for op in all_operations}

        result = []

        # process each operation bucket
        for op_bucket in aggregations["operations"]["buckets"]:
            operation_id = op_bucket["key"]

            # look up matching operation
            if operation_id not in operation_map:
                continue

            operation: GulpOperation = operation_map[operation_id]

            # build operation entry
            operation_entry = {
                "name": operation.name,
                "index": operation.index,
                "id": operation.id,
                "contexts": [],
            }

            # process contexts
            for ctx_bucket in op_bucket["context_id"]["buckets"]:
                context_id = ctx_bucket["key"]

                # Find matching context in operation
                matching_context = next(
                    (ctx for ctx in operation.contexts if ctx.id == context_id),
                    None,
                )
                if not matching_context:
                    continue

                context_entry = {
                    "name": matching_context.name,
                    "id": matching_context.id,
                    "doc_count": ctx_bucket["doc_count"],
                    "plugins": [],
                }

                # Process plugins
                for plugin_bucket in ctx_bucket["plugin"]["buckets"]:
                    plugin_entry = {"name": plugin_bucket["key"], "sources": []}

                    # Process sources
                    for src_bucket in plugin_bucket["source_id"]["buckets"]:
                        source_id = src_bucket["key"]

                        # Find matching source in context
                        matching_source = next(
                            (
                                src
                                for src in matching_context.sources
                                if src.id == source_id
                            ),
                            None,
                        )
                        if not matching_source:
                            continue

                        source_entry = {
                            "name": matching_source.name,
                            "id": matching_source.id,
                            "doc_count": src_bucket["doc_count"],
                            "max_event.code": int(
                                src_bucket["max_event.code"]["value"]
                            ),
                            "min_event.code": int(
                                src_bucket["min_event.code"]["value"]
                            ),
                            "min_gulp.timestamp": int(
                                src_bucket["min_gulp.timestamp"]["value"]
                            ),
                            "max_gulp.timestamp": int(
                                src_bucket["max_gulp.timestamp"]["value"]
                            ),
                        }
                        plugin_entry["sources"].append(source_entry)

                    if plugin_entry["sources"]:
                        context_entry["plugins"].append(plugin_entry)

                if context_entry["plugins"]:
                    operation_entry["contexts"].append(context_entry)

            if operation_entry["contexts"]:
                result.append(operation_entry)

        return result

    async def query_operations(self, index: str, user_id: str) -> list[dict]:
        """
        queries the OpenSearch index for each "operation_id" value found and returns the aggregations.

        Args:
            index (str): Name of the index (or datastream) to query
            user_id (str): The user ID to filter operations

        Returns:
            liist[dict]: The aggregations result (WARNING: will return at most "aggregation_max_buckets" hits, which should cover 99,99% of the usage ....).
        """

        def _create_terms_aggregation(field):
            return {"terms": {"field": field, "size": max_buckets}}

        max_buckets = GulpConfig.get_instance().aggregation_max_buckets()

        aggs = {"operations": _create_terms_aggregation("gulp.operation_id")}
        aggs["operations"]["aggs"] = {
            "context_id": _create_terms_aggregation("gulp.context_id"),
        }
        aggs["operations"]["aggs"]["context_id"]["aggs"] = {
            "plugin": _create_terms_aggregation("agent.type")
        }
        aggs["operations"]["aggs"]["context_id"]["aggs"]["plugin"]["aggs"] = {
            "source_id": _create_terms_aggregation("gulp.source_id")
        }
        aggs["operations"]["aggs"]["context_id"]["aggs"]["plugin"]["aggs"]["source_id"][
            "aggs"
        ] = {
            "max_gulp.timestamp": {"max": {"field": "gulp.timestamp"}},
            "min_gulp.timestamp": {"min": {"field": "gulp.timestamp"}},
            "max_event.code": {"max": {"field": "gulp.event_code"}},
            "min_event.code": {"min": {"field": "gulp.event_code"}},
        }
        body = {
            "track_total_hits": True,
            "aggregations": aggs,
            "size": 0,
        }
        headers = {
            "content-type": "application/json",
        }

        res = await self._opensearch.search(body=body, index=index, headers=headers)
        hits = res["hits"]["total"]["value"]
        if not hits:
            MutyLogger.get_instance().warning(
                "no results found, returning empty aggregations (possibly no data on opensearch)!"
            )
            # raise ObjectNotFound()
            return []
        MutyLogger.get_instance().debug(orjson.dumps(res, option=orjson.OPT_INDENT_2).decode())

        d = {"total": hits, "aggregations": res["aggregations"]}
        # MutyLogger.get_instance().debug(orjson.dumps(d, option=orjson.OPT_INDENT_2).decode())
        return await self._parse_operation_aggregation(d["aggregations"], user_id)

    async def query_single_document(
        self,
        datastream: str,
        doc_id: str,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
    ) -> dict:
        """
        Get a single event from OpenSearch.

        Args:
            datastream (str): The name of the datastream or index to query
            id (str): The ID of the document to retrieve
            el (AsyncElasticSearch|AsyncOpenSearch, optional): the ElasticSearch/OpenSearch client to use instead of the default OpenSearch. Defaults to None.
        Returns:
            dict: The query result.

        Raises:
            ObjectNotFound: If no results are found.
        """
        try:
            # check if datastream is an index
            if el:
                res = await el.indices.get_data_stream(name=datastream)
            else:
                res = await self._opensearch.indices.get_data_stream(name=datastream)

            # resolve to index
            index = res["data_streams"][0]["indices"][0]["index_name"]
        except Exception:
            # datastream is actually an index
            index = datastream

        try:
            if el:
                res = await el.get(index=index, id=doc_id)
            else:
                res = await self._opensearch.get(index=index, id=doc_id)
            js = res["_source"]
            js["_id"] = res["_id"]
            return js
        except KeyError as ex:
            raise ObjectNotFound(
                f'document with ID "{doc_id}" not found in datastream={
                    datastream} index={index}'
            ) from ex

    async def _search_dsl_internal(
        self,
        index: str,
        parsed_options: dict,
        q: dict,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
        raise_on_error: bool = True,
    ) -> tuple[int, list[dict], list[dict]]:
        """
        Executes a raw DSL query on OpenSearch and returns the results.

        Args:
            index (str): Name of the index (or datastream) to query.
            parsed_options (dict): The parsed query options.
            q (dict): The DSL query to execute.
            el (AsyncElasticSearch|AsyncOpenSearch, optional): an EXTERNAL ElasticSearch/OpenSearch client to use instead of the default internal gulp's OpenSearch. Defaults to None.
            raise_on_error (bool, optional): Whether to raise an exception if no more hits are found. Defaults to True.
        Returns:
            tuple:
            - total_hits (int): The total number of hits found.
            - docs (list[dict]): The documents found.
            - search_after (list[dict]): to be passed via `q_options.search_after` in the next iteration.

        Raises:
            ObjectNotFound: If no more hits are found.
        """
        body = q
        body["track_total_hits"] = True

        # build raw query body (query + parsed_options)
        for k, v in parsed_options.items():
            if v:
                body[k] = v
        MutyLogger.get_instance().debug("index=%s, query_raw body=%s, parsed_options=%s" % (index, orjson.dumps(body, option=orjson.OPT_INDENT_2).decode(), orjson.dumps(parsed_options, option=orjson.OPT_INDENT_2).decode()))

        headers = {
            "content-type": "application/json",
        }
        timeout = GulpConfig.get_instance().opensearch_request_timeout()
        params: dict = {}
        if timeout > 0:
            # set timeout in seconds
            params["timeout"] = timeout

        if el:
            if isinstance(el, AsyncElasticsearch):
                # use the ElasticSearch client provided
                res = await el.search(
                    index=index,
                    track_total_hits=True,
                    query=q["query"],
                    sort=parsed_options["sort"],
                    size=parsed_options["size"],
                    search_after=parsed_options["search_after"],
                    source=parsed_options["_source"],
                    highlight=q.get("highlight", None),
                    timeout=timeout if timeout else None,
                )
            else:
                # external opensearch
                res = await el.search(
                    body=body, index=index, headers=headers, params=params
                )
        else:
            # use the OpenSearch client (default)
            res = await self._opensearch.search(
                body=body, index=index, headers=headers, params=params
            )

        # MutyLogger.get_instance().debug("_search_dsl_internal: res=%s" % (orjson.dumps(res, option=orjson.OPT_INDENT_2).decode()))
        hits = res["hits"]["hits"]
        if not hits:
            if raise_on_error:
                raise ObjectNotFound("no more hits")
            return 0, [], []

        # get data
        total_hits = res["hits"]["total"]["value"]
        # docs = [{**hit["_source"], "_id": hit["_id"]} for hit in hits]
        docs = [
            {
                **hit["_source"],
                "_id": hit["_id"],
                **({"highlight": hit["highlight"]} if  "highlight" in hit else {}),
            }
            for hit in hits
        ]

        search_after = hits[-1]["sort"]
        return total_hits, docs, search_after

    async def search_dsl_sync(
        self,
        index: str,
        q: dict,
        q_options: "GulpQueryParameters" = None,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
        raise_on_error: bool = True,
    ) -> tuple[int, list[dict], list[dict]]:
        """
        Executes a raw DSL query on OpenSearch/Elasticsearch and returns the results directly.

        Args:
            index (str): Name of the index (or datastream) to query.
            q (dict): The DSL query to execute.
            el (AsyncElasticSearch|AsyncOpenSearch, optional): an EXTERNAL ElasticSearch/OpenSearch client to use instead of the default internal gulp's OpenSearch. Defaults to None.
            raise_on_error (bool, optional): Whether to raise an exception if no more hits are found. Defaults to True.

        Returns:
            tuple:
            - total_hits (int): The total number of hits found.
            - docs (list[dict]): The documents found.
            - search_after (list[dict]): to be passed via `q_options.search_after` in the next iteration.

        Raises:
            ObjectNotFound: If no more hits are found.
        """
        from gulp.api.opensearch.query import GulpQueryParameters

        if not q_options:
            q_options = GulpQueryParameters()

        parsed_options: dict = q_options.parse()
        total_hits, docs, search_after = await self._search_dsl_internal(
            index, parsed_options, q, el, raise_on_error=raise_on_error
        )
        return total_hits, docs, search_after

    async def search_dsl(
        self,
        sess: AsyncSession,
        index: str,
        q: dict,
        req_id: str = None,
        ws_id: str = None,
        user_id: str = None,
        q_options: "GulpQueryParameters" = None,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
        callback: callable = None,
        callback_args: dict = None,
        callback_chunk: callable = None,
        callback_chunk_args: dict = None,
        source_q: str = None,
    ) -> tuple[int, int]:
        """
        Executes a raw DSL query on OpenSearch and optionally streams the results on the websocket.

        NOTE: in the end, all gulp **local** queries and all **elasticsearch/opensearch** based queries for external plugins will be done through this function.

        Args:
            sess (AsyncSession): SQLAlchemy session (to check if request has been canceled and/or create notes on match)
            index (str): Name of the index (or datastream) to query. may also be a comma-separated list of indices/datastreams, or "*" to query all.
            q (dict): The DSL query to execute (will be run as "query": q }, so be sure it is stripped of the root "query" key)
            req_id (str), optional: The request ID for the query
            ws_id (str, optional): The websocket ID to send the results to, pass None to disable sending on the websocket.
            user_id (str, optional): The user ID performing the query
            q_options (GulpQueryOptions, optional): Additional query options. Defaults to None (use defaults).
            el (AsyncElasticSearch|AsyncOpenSearch, optional): an EXTERNAL ElasticSearch/OpenSearch client to use instead of the default internal gulp's OpenSearch. Defaults to None.
            callback (callable, optional): the callback to call for each document found. Defaults to None.
                the callback must be defined as:
                async def callback(doc: dict, idx: int, **kwargs) -> None

                NOTE: if callback is set, all postprocessing on the document is disabled (including sending on the websockets and note creations) and must be done by the callback if needed.
            callback_args (dict, optional): further arguments to pass to the callback. Defaults to None.
            callback_chunk (callable, optional): the callback to call for each chunk of documents found. Defaults to None.
                the callback must be defined as:
                async def callback_chunk(docs: list[dict], **kwargs) -> None

                NOTE: if callback is set, all postprocessing on the document is disabled (including sending on the websockets and note creations) and must done by the callback if needed.
            callback_chunk_args (dict, optional): further arguments to pass to the callback_chunk. Defaults to None.
            source_q (str, optional): the source query (i.e. a sigma rule) from which this query was generated.
        Return:
            tuple:
            - processed (int): The number of documents processed (on a clean exit, this will be equal to total_hits).
            - total_hits (int): The total number of hits found.

        Raises:
            ValueError: argument error
            Exception: If an error occurs during the query.
        """
        from gulp.api.opensearch.query import GulpQueryParameters

        if not q_options:
            # use defaults
            q_options = GulpQueryParameters()

        if q_options.note_parameters.create_notes and not sess:
            raise ValueError("sess is required if create_notes is set!")

        if el:
            # force use_elasticsearch_api if el is provided
            MutyLogger.get_instance().debug(
                "search_dsl: using provided ElasticSearch/OpenSearch client %s, class=%s"
                % (el, el.__class__)
            )

        parsed_options: dict = q_options.parse()
        processed: int = 0
        chunk_num: int = 0
        check_canceled_count: int = 0
        total_hits: int = 0

        while True:
            last: bool = False
            docs: list[dict] = []
            try:
                total_hits, docs, search_after = await self._search_dsl_internal(
                    index, parsed_options, q, el
                )
                if q_options.loop:
                    # auto setup for next iteration
                    parsed_options["search_after"] = search_after

                processed += len(docs)
                if processed >= total_hits or not q_options.loop or (q_options.total_limit and processed >= q_options.total_limit):
                    # this is the last chunk
                    last = True

                # MutyLogger.get_instance().debug("retrieved chunk of %d documents, total=%d" % (len(docs), total_hits))

                check_canceled_count += 1
                if check_canceled_count >= 10:
                    # every 10 chunk, check for request cancelation
                    check_canceled_count = 0
                    canceled = await GulpRequestStats.is_canceled(sess, req_id)
                    # MutyLogger.get_instance().debug("search_dsl: request %s stats=%s" % (req_id, stats))
                    if canceled:
                        last = True
                        MutyLogger.get_instance().warning(
                            "search_dsl: request %s canceled!" % (req_id)
                        )

                if callback:
                    # call the callback for each document
                    for idx, doc in enumerate(docs):
                        await callback(
                            doc,
                            processed + idx,
                            **callback_args if callback_args else {},
                        )

                if callback_chunk:
                    # call the callback for each chunk of documents
                    await callback_chunk(
                        docs,
                        total_hits=total_hits,
                        last=last,
                        chunk_num=chunk_num,
                        **callback_chunk_args if callback_chunk_args else {},
                    )

            except ObjectNotFound:
                if processed == 0 and ws_id:
                    # no results
                    return 0, 0
                # indicates the last result
                last = True
            except Exception as ex:
                # something went wrong
                MutyLogger.get_instance().exception(ex)
                raise ex

            if ws_id and not callback and not callback_chunk:
                # build a GulpDocumentsChunk and send to websocket
                chunk = GulpDocumentsChunkPacket(
                    docs=docs,
                    num_docs=len(docs),
                    chunk_number=chunk_num,
                    total_hits=total_hits,
                    last=last,
                    search_after=search_after,
                    name=q_options.name,
                )
                wsq = GulpWsSharedQueue.get_instance()
                await wsq.put(
                    type=WSDATA_DOCUMENTS_CHUNK,
                    ws_id=ws_id,
                    user_id=user_id,
                    req_id=req_id,
                    data=chunk.model_dump(exclude_none=True),
                )

            if (
                q_options.note_parameters.create_notes
                and not callback
                and not callback_chunk
            ):
                # automatically generate notes
                await GulpNote.bulk_create_from_documents(
                    sess,
                    user_id,
                    ws_id=ws_id,
                    req_id=req_id,
                    docs=docs,
                    name=q_options.note_parameters.note_name,
                    tags=q_options.note_parameters.note_tags,
                    color=q_options.note_parameters.note_color,
                    glyph_id=q_options.note_parameters.note_glyph_id,
                    source_q=source_q,
                )

            # next chunk
            chunk_num += 1
            if last or not q_options.loop:
                break

        MutyLogger.get_instance().info(
            "search_dsl: processed %d documents, total=%d, chunks=%d"
            % (processed, total_hits, chunk_num)
        )
        return processed, total_hits

    async def search_dsl_sync_output_file(
        self,
        sess: AsyncSession,
        index: str,
        q: dict,
        req_id: str,
        q_options: "GulpQueryParameters" = None,
        file_path: str = None,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
    ) -> str:
        """
        Executes a raw DSL query on OpenSearch and writes the results to a JSON file in the temp directory.

        Args:
            sess (AsyncSession): SQLAlchemy session (to check if request has been canceled).
            index (str): Name of the index (or datastream) to query.
            q (dict): The DSL query to execute.
            req_id (str): The request ID for the query.
            q_options (GulpQueryParameters, optional): Additional query options. Defaults to None (use defaults).
            file_path (str, optional): The path to the output file. If None, a temporary file will be created and it is the responsibility of the caller to delete it when no longer needed. Defaults to None.
            el (AsyncElasticSearch|AsyncOpenSearch, optional): an EXTERNAL ElasticSearch/OpenSearch client to use instead of the default internal gulp's OpenSearch. Defaults to None.

        Returns:
            str: The path to the output file containing the results in JSON format
        Raises:
            ValueError: If `sess` is None and `q_options.note_parameters.create_notes` is True.
            ObjectNotFound: If no results are found and `processed` is 0.
            Exception: If an error occurs during the query or file writing.
        """
        from gulp.api.opensearch.query import GulpQueryParameters

        # create output file in the temp directory
        if not file_path:
            # generate temporary file
            t = tempfile.mkstemp(suffix=".json", prefix="gulp_search_")
            path: str = t[1]
        else:
            path = file_path

        if not q_options:
            # use defaults
            q_options = GulpQueryParameters()

        if el:
            # force use_elasticsearch_api if el is provided
            MutyLogger.get_instance().debug(
                "search_dsl_sync_output_file: using provided ElasticSearch/OpenSearch client %s, class=%s"
                % (el, el.__class__)
            )

        parsed_options: dict = q_options.parse()
        processed: int = 0
        chunk_num: int = 0
        check_canceled_count: int = 0
        total_hits: int = 0

        MutyLogger.get_instance().info("streaming responses to file %s ..." % (path))

        async with aiofiles.open(path, 'wb') as f:
            # write json start
            await f.write("{\n\t\"docs\": [\n".encode())

            # loop
            while True:
                last: bool = False
                docs: list[dict] = []
                try:
                    total_hits, docs, search_after = await self._search_dsl_internal(
                        index, parsed_options, q, el
                    )

                    # write this chunk
                    for d in docs:
                        # convert to json and write to file, remove highlight if any
                        d.pop("highlight", None)
                        d = json.dumps(d, indent="\t")

                        await f.write(f"\t{d},\n".encode())

                    if q_options.loop:
                        # auto setup for next iteration
                        parsed_options["search_after"] = search_after

                    processed += len(docs)
                    if processed >= total_hits or not q_options.loop or (q_options.total_limit and processed >= q_options.total_limit):
                        # this is the last chunk
                        last = True

                    # MutyLogger.get_instance().debug("retrieved chunk of %d documents, total=%d" % (len(docs), total_hits))

                    check_canceled_count += 1
                    if check_canceled_count >= 10:
                        # every 10 chunk, check for request cancelation
                        check_canceled_count = 0
                        canceled = await GulpRequestStats.is_canceled(sess, req_id)
                        # MutyLogger.get_instance().debug("search_dsl: request %s stats=%s" % (req_id, stats))
                        if canceled:
                            last = True
                            MutyLogger.get_instance().warning(
                                "search_dsl: request %s canceled!" % (req_id)
                            )

                except ObjectNotFound as ex:
                    MutyLogger.get_instance().debug(
                        "search_dsl_sync_output_file: no more hits found, processed=%d, total_hits=%d"
                        % (processed, total_hits)
                    )
                    if processed == 0:
                        # no results
                        raise ex

                    # indicates the last result
                    last = True
                except Exception as ex:
                    # something went wrong
                    MutyLogger.get_instance().exception(ex)
                    if path:
                        # delete output file
                        await muty.file.delete_file_or_dir_async(path)
                    raise ex

                # next chunk
                chunk_num += 1
                if last or not q_options.loop:
                    # remove last comma
                    await f.seek(-2, os.SEEK_END)
                    await f.truncate()

                    # write json end
                    await f.write("\t]\n}\n".encode())
                    break

        MutyLogger.get_instance().info(
            "search_dsl_sync_output_file: processed %d documents, total=%d, chunks=%d"
            % (processed, total_hits, chunk_num)
        )
        return path
