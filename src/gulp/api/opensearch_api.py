import json
import os
from urllib.parse import urlparse

import muty.crypto
import muty.dict
import muty.file
import muty.string
import muty.time

import gulp.config as config
from gulp.api.opensearch.structs import (
    GulpDocumentFilterResult,
    GulpIngestionFilter,
    GulpQueryAdditionalOptions,
    GulpQueryFilter,
)
from gulp.defs import ObjectNotFound
from gulp.utils import GulpLogger

from elasticsearch import AsyncElasticsearch
from opensearchpy import AsyncOpenSearch

class GulpOpenSearch():
    """
    singleton class to handle OpenSearch client connection.

    for ssl, it will use the CA certificate and client certificate/key if they exist in the certs directory (see config.path_certs()).
    
    they should be named os-ca.pem, os.pem, os.key.
    """

    # to be used in dynamic templates
    UNMAPPED_PREFIX = "gulp.unmapped"

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._opensearch: AsyncOpenSearch = self._get_client()

    @classmethod
    def get_instance(cls) -> "GulpOpenSearch":
        """
        returns the singleton instance of the OpenSearch client.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
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
        url = config.opensearch_url()
        verify_certs = config.opensearch_verify_certs()

        # split into user:pwd@host:port
        parsed = urlparse(url)

        # url = "%s://%s:***********@%s:%s" % (parsed.scheme, parsed.username, parsed.hostname, parsed.port)
        # GulpLogger.get_instance().debug('%s, opensearch hostname=%s, port=%d, user=%s, password=***********' % (url, parsed.hostname, parsed.port, parsed.username))

        host = parsed.scheme + "://" + parsed.hostname + ":" + str(parsed.port)
        ca = None
        certs_dir = config.path_certs()
        if certs_dir is not None and parsed.scheme.lower() == "https":
            # https and certs_dir is set
            ca: str = muty.file.abspath(
                muty.file.safe_path_join(certs_dir, "os-ca.pem")
            )

            # check if client certificate exists. if so, it will be used
            client_cert = muty.file.safe_path_join(certs_dir, "os.pem")
            client_key = muty.file.safe_path_join(certs_dir, "os.key")
            if os.path.exists(client_cert) and os.path.exists(client_key):
                GulpLogger.get_instance().debug(
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
                GulpLogger.get_instance().debug(
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
        GulpLogger.get_instance().debug("created opensearch client: %s" % (el))
        return el

    async def shutdown(self) -> None:
        """
        Shutdown the OpenSearch client.

        Returns:
            None
        """
        await self._opensearch.close()
        GulpLogger.get_instance().debug("opensearch client shutdown: %s" % (self._opensearch))
        self._opensearch = None


    async def check_alive(self) -> None:
        """
        Check if the OpenSearch client is alive.

        Raises:
            Exception: If the client is not reachable

        """
        res = await self._opensearch.info()
        GulpLogger.get_instance().debug("opensearch info: %s" % (res))

    async def datastream_get_key_value_mapping(self, name: str, return_raw_result: bool = False
    ) -> dict:
        """
        Get and parse mappings for the given datastream or index: it will result in a dict (if return_raw_result is not set) like:
        {
            "field1": "type",
            "field2": "type",
            ...
        }

        Args:
            name (str): an index/datastream to query
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
            res = await self._opensearch.indices.get_mapping(index=name)
        except Exception as e:
            GulpLogger.get_instance().warning('no mapping for index "%s" found: %s' % (name, e))
            return {}

        # GulpLogger.get_instance().debug("index_get_mapping: %s" % (json.dumps(res, indent=2)))
        if return_raw_result:
            return res

        idx = list(res.keys())[0]
        properties = res[idx]["mappings"]["properties"]
        return _parse_mappings_internal(properties)


    async def datastream_get_mapping_by_src(self, name: str, context: str, src_file: str) -> dict:
        """
        Get and parse mappings for the given index/datastream, considering only "gulp.context"=context and "log.file.path"=src_file.

        The return type is the same as index_get_mapping with return_raw_result=False.

        Args:
            index (str): an index/datastream to query
            src_file (str): the source file to filter by

        Returns:
            dict: The mapping dict.
        """
        # query first
        options = {}  # GulpQueryOptions()
        options.limit = 1000
        options.fields_filter = ["*"]
        q = {
            "query_string": {
                "query": 'gulp.context: "%s" AND gulp.source.file: "%s"'
                % (context, src_file)
            }
        }

        # get mapping
        mapping = await self.datastream_get_key_value_mapping(name)
        filtered_mapping = {}

        # loop with query_raw until there's data and update filtered_mapping
        while True:
            try:
                docs = await self.query_raw(name, q, options)
                # GulpLogger.get_instance().debug("docs: %s" % (json.dumps(docs, indent=2)))
            except ObjectNotFound:
                break
            search_after = docs.get("search_after", None)

            docs = docs["results"]
            options.search_after = search_after

            # Update filtered_mapping with the current batch of documents
            for doc in docs:
                for k in mapping.keys():
                    if k in doc:
                        if k not in filtered_mapping:
                            filtered_mapping[k] = mapping[k]

            if search_after is None:
                # no more results
                break

        if not filtered_mapping:
            raise ObjectNotFound("no documents found for src_file=%s" % (src_file))

        # sort the keys
        filtered_mapping = dict(sorted(filtered_mapping.items()))
        return filtered_mapping


    async def index_template_delete(self, name: str) -> None:
        """
        Delete the index template for the given index/datastream.

        Args:
            name (str): The index/datastream name.
        """
        try:
            template_name = "%s-template" % (name)
            GulpLogger.get_instance().debug("deleting index template: %s ..." % (template_name))
            await self._opensearch.indices.delete_index_template(name=template_name)
        except Exception as e:
            GulpLogger.get_instance().error("error deleting index template: %s" % (e))

    async def index_template_get(self, name: str) -> dict:
        """
        Get the index template for the given index/datastream.

        Args:
            name (str): The index/datastream name.

        Returns:
            dict: The index template.
        """
        template_name = "%s-template" % (name)
        GulpLogger.get_instance().debug("getting index template: %s ..." % (template_name))

        try:
            res = await self._opensearch.indices.get_index_template(name=template_name)
        except Exception as e:
            raise ObjectNotFound("no template found for datastream/index %s" % (name))

        return res["index_templates"][0]["index_template"]


    async def index_template_put(self, name: str, d: dict) -> dict:
        """
        Put the index template for the given index/datastream.

        Args:
            name (str): The index/datastream name.
            d (dict): The index template.

        Returns:
            dict: The response from the OpenSearch client.
        """
        template_name = "%s-template" % (name)
        GulpLogger.get_instance().debug("putting index template: %s ..." % (template_name))
        headers = {"accept": "application/json", "content-type": "application/json"}
        return await self._opensearch.indices.put_index_template(
            name=template_name, body=d, headers=headers
        )


    async def index_template_set_from_file(self, name: str, path: str, apply_patches: bool = True
    ) -> dict:
        """
        Asynchronously sets an index template in OpenSearch from a JSON file.

                Args:
            name (str): The name of the index/datastream to set the template for.
            path (str): The file path to the JSON file containing the index template.
            apply_patches (bool, optional): Whether to apply specific patches to the mappings and settings before setting the index template. Defaults to True.
        Returns:
            dict: The response from OpenSearch after setting the index template.
        """

        GulpLogger.get_instance().debug('loading index template from file "%s" ...' % (path))
        d = muty.dict.from_json_file(path)
        return await self.build_and_set_index_template(name, d, apply_patches)


    async def build_and_set_index_template(
        self, index_name: str, d: dict, apply_patches: bool = True
    ) -> dict:
        """
        Sets an index template in OpenSearch, applying the needed patches to the mappings and settings.
        Args:
            index_name (str): The name of the index for which the template is being set.
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
        GulpLogger.get_instance().debug('setting index template for "%s" ...' % (index_name))
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
        d["index_patterns"] = [index_name]
        d["data_stream"] = {}
        d["priority"] = 100

        # always set gulp specific mappings
        mappings["properties"]["gulp"] = {
            "properties": {
                "event": {"properties": {"code": {"type": "long"}}},
                "invalid": {"properties": {"timestamp": {"type": "boolean"}}},
                "context": {"type": "keyword"},
                "timestamp": {"type": "long"},    
                "operation": {"type": "keyword"},
            }
        }
        dtt = []
        dtt.append(
            {
                # force unknown mapping to string
                "force_unmapped_to_string": {
                    "match": "%s.*" % (GulpOpenSearch.UNMAPPED_PREFIX),
                    # "match":"*",
                    "mapping": {"type": "keyword"},
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
            mappings["properties"]["@timestamp"] = {
                "type": "date_nanos",
                "format": "strict_date_optional_time_nanos",
            }

            mappings["numeric_detection"] = False

            # support for original event both as keyword and text
            mappings["properties"]["event"]["properties"]["original"] = {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
            }
            settings["index"]["mapping"]["total_fields"] = {
                "limit": config.index_template_default_total_fields_limit()
            }
            settings["index"][
                "refresh_interval"
            ] = config.index_template_default_refresh_interval()
            if not config.opensearch_multiple_nodes():
                # optimize for single node
                # this also removes "yellow" node in single node mode
                # this also removes "yellow" node in single node mode
                GulpLogger.get_instance().warning("setting number_of_replicas to 0")
                settings["index"]["number_of_replicas"] = 0

        # write template
        await self.index_template_put(index_name, d)
        return d


    async def datastream_list(self) -> list[str]:
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
        # GulpLogger.get_instance().debug(json.dumps(l, indent=2))
        ll = []
        ds = l.get("data_streams", [])
        for c in ds:
            cc = {
                "name": c["name"],
                "indexes": c["indices"],
                "template": c.get("template", None),
            }
            ll.append(cc)

        return ll

    async def datastream_delete(self, name: str) -> None:
        """
        Delete the datastream and associated index and template from OpenSearch.

        Args:
            name (str): The name of the datastream to delete.

        Returns:
            None
        """
        # params = {"ignore_unavailable": "true"}
        headers = {"accept": "application/json"}
        try:
            GulpLogger.get_instance().debug('deleting datastream "%s" ...' % (name))
            await self._opensearch.indices.delete_data_stream(name, headers=headers)
        except Exception as e:
            GulpLogger.get_instance().error("error deleting datastream: %s" % (e))
            pass
        try:
            await self.index_template_delete(name)
        except Exception as e:
            GulpLogger.get_instance().error("error deleting index template: %s" % (e))


    async def datastream_create(self, name: str, index_template: str = None
    ) -> None:
        """
        (re)creates the OpenSearch datastream (with backing index) and associates the index template from configuration (or uses the default).

        Args:
            datastream_name(str): The name of the datastream to be created, the index template will be re/created as "<index_name>-template".
                if it already exists, it will be deleted first.
            index_template (str, optional): path to the index template to use. Defaults to None (use the default index template).
        Returns:
            dict
        """

        # attempt to delete the datastream first, if it exists
        GulpLogger.get_instance().debug('re/creating datastream "%s" ...' % (name))
        await self.datastream_delete(name)

        # create index template, check if we are overriding the default index template.
        # if so, we only apply gulp-related patching and leaving everything else as is
        apply_patches = True
        if index_template is None:
            template_path = config.path_index_template()
        else:
            template_path = index_template
            GulpLogger.get_instance().debug('using custom index template "%s" ...' % (template_path))
            apply_patches = False
        await self.index_template_set_from_file(name, template_path, apply_patches=apply_patches)

        try:
            # create datastream
            headers = {"accept": "application/json", "content-type": "application/json"}
            r = await self._opensearch.indices.create_data_stream(name, headers=headers)
            GulpLogger.get_instance().debug("datastream created: %s" % (r))
            return r
        except Exception as e:
            # delete the index template
            await self.index_template_delete(name)
            raise e

    def _bulk_docs_result_to_ingest_chunk(self,
        bulk_docs: list[tuple[dict, dict]], errors: list[dict] = None
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


    async def ingest_bulk(
        self,
        index: str,
        docs: list[dict],
        flt: GulpIngestionFilter = None,
        wait_for_refresh: bool = False,
    ) -> tuple[int, int, list[dict]]:
        """
        ingests a list of GulpDocument into OpenSearch.

        Args:
            el (AsyncOpenSearch): The OpenSearch client.
            index (str): Name of the index (or datastream) to ingest documents to.
            doc (dict): The documents to be ingested
            flt (GulpIngestionFilter, optional): The filter parameters. Defaults to None.
            wait_for_refresh (bool, optional): Whether to wait for the refresh(=refreshed index is available for searching) to complete. Defaults to False.

        Returns:
            tuple:
            - number of skipped (because already existing=duplicated) events
            - number of failed events
            - list of ingested documents
        """

        # filter documents first
        bulk_docs = [
            doc
            for item in docs
            if not flt or GulpIngestionFilter.filter_doc_for_ingestion(item, flt) != GulpDocumentFilterResult.SKIP

            # create a tuple with the create action and the document
            for doc in (
                {"create": {"_id": item["_id"]}},
                {k: v for k, v in item.items() if k != "_id"},
            )
        ]
        # GulpLogger.get_instance().error('ingesting %d documents (was %d before filtering)' % (len(docs) / 2, len_first))
        if len(bulk_docs) == 0:
            GulpLogger.get_instance().warning("no document to ingest (flt=%s)" % (flt))
            return 0, 0, []

        # GulpLogger.get_instance().info("ingesting %d docs: %s\n" % (len(bulk_docs) / 2, json.dumps(bulk_docs, indent=2)))

        # bulk ingestion
        timeout = config.ingestion_request_timeout()
        params = None
        if wait_for_refresh:
            params = {"refresh": "wait_for", "timeout": timeout}
        else:
            params = {"timeout": timeout}

        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        res = await self._opensearch.bulk(body=bulk_docs, index=index, params=params, headers=headers)
        skipped = 0
        failed = 0
        ingested: list[dict] = []
        if res["errors"]:
            # count skipped
            skipped = sum(1 for r in res["items"] if r["create"]["status"] == 409)
            # count failed
            failed = sum(
                1 for r in res["items"] if r["create"]["status"] not in [201, 200, 409]
            )
            if failed > 0:
                failed_items = [
                    item
                    for item in res["items"]
                    if item["create"]["status"] not in [201, 200]
                ]
                s = json.dumps(failed_items, indent=2)
                GulpLogger.get_instance().error(
                    "%d failed ingestion, %d skipped: %s"
                    % (failed, skipped, muty.string.make_shorter(s, max_len=10000))
                )

            # take only the documents with no ingest errors
            ingested = self._bulk_docs_result_to_ingest_chunk(bulk_docs, res["items"])
        else:
            # no errors, take all documents
            ingested = self._bulk_docs_result_to_ingest_chunk(bulk_docs)

        if skipped != 0:
            # NOTE: bulk_docs/2 is because the bulk_docs is a list of tuples (create, doc)
            GulpLogger.get_instance().error(
                "**NOT AN ERROR** total %d skipped, %d failed in this bulk ingestion of %d documents !"
                % (skipped, failed, len(bulk_docs) / 2)
            )
        if failed > 0:
            GulpLogger.get_instance().critical("failed is set, ingestion format needs to be fixed!")
        return skipped, failed, ingested


    async def rebase(
        self,
        index: str,
        dest_index: str,
        msec_offset: int,
        flt: GulpQueryFilter = None,
    ) -> dict:
        """
        Rebase documents from one OpenSearch index to another with a timestamp offset.
        Args:
            index (str): The source index name.
            dest_index (str): The destination index name.
            msec_offset (int): The offset in milliseconds from unix epoch to adjust the '@timestamp' field.
            flt (GulpQueryFilter, optional): if set, it will be used to rebase only a subset of the documents. Defaults to None.
        Returns:
            dict: The response from the OpenSearch reindex operation.

        """
        GulpLogger.get_instance().debug(
            "rebase index %s to %s with offset=%d, flt=%s ..."
            % (index, dest_index, msec_offset, flt)
        )
        if not flt:
            flt = GulpQueryFilter()
        
        q = flt.to_opensearch_dsl()
        
        convert_script = """
            if (ctx._source['@timestamp'] != 0) {
                ctx._source['@timestamp'] += params.nsec_offset;
                ctx._source["gulp.timestamp"] += params.nsec_offset;
            }
        """

        body: dict = {
            "source": {"index": index, "query": q["query"]},
            "dest": {"index": dest_index, "op_type": "create"},
            "script": {
                "lang": "painless",
                "source": convert_script,
                "params": {
                    "nsec_offset": msec_offset * muty.time.MILLISECONDS_TO_NANOSECONDS,
                },
            },
        }
        params: dict = {"refresh": "true", "wait_for_completion": "true", "timeout": 0}
        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        GulpLogger.get_instance().debug("rebase body=%s" % (body))
        res = await self._opensearch.reindex(body=body, params=params, headers=headers)
        return res

    async def index_refresh(self, index: str) -> None:
        """
        Refresh an index(=make changes available to search) in OpenSearch.

        Args:
            index (str): Name of the index (or datastream) to refresh.

        Returns:
            None
        """
        GulpLogger.get_instance().debug("refreshing index: %s" % (index))
        res = await self._opensearch.indices.refresh(index=index)
        GulpLogger.get_instance().debug("refreshed index: %s" % (res))

    async def delete_data_by_operation(
        self, index: str, operation: str, refresh: bool = True
    ):
        """
        Deletes all data from an index that matches the given operation.

        Args:
            index (str): Name of the index (or datastream) to delete data from.
            operation_id (int): The ID of the operation.
            refresh (bool, optional): Whether to refresh the index after deletion. Defaults to True.

        Returns:
            None
        """
        q = {"query": {"term": {"gulp.operation": operation}}}

        params = None
        if refresh:
            params = {"refresh": "true"}
        headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }

        await self._opensearch.delete_by_query(index=index, body=q, params=params, headers=headers)

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
                        "max_event.code": bucket["max_event.code"]["value"],
                        "min_gulp.timestamp": bucket["min_gulp.timestamp"]["value"],
                        "max_gulp.timestamp": bucket["max_gulp.timestamp"]["value"],
                        "min_event.code": bucket["min_event.code"]["value"],
                    }
                }
                for bucket in buckets
            ],
            "total": sum(bucket["doc_count"] for bucket in buckets)
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
            "max_event.code": {"max": {"field": "gulp.event.code"}},
            "min_event.code": {"min": {"field": "gulp.event.code"}},
        }
        if group_by is not None:
            aggregations = {
                "by_type": {
                    "terms": {"field": group_by},
                    "aggs": aggregations,
                }
            }
            GulpLogger.get_instance().debug(
                f"aggregations with group_by={group_by}: {json.dumps(aggregations, indent=2)}"
            )

        q = flt.to_opensearch_dsl()
        GulpLogger.get_instance().debug(f"query_max_min_per_field: q={json.dumps(q, indent=2)}")
        body = {"track_total_hits": True, "query": q["query"], "aggregations": aggregations}
        headers = {
            "content-type": "application/json",
        }

        res = await self._opensearch.search(body=body, index=index, headers=headers)
        hits = res["hits"]["total"]["value"]
        if not hits:
            raise ObjectNotFound()

        if group_by:
            GulpLogger.get_instance().debug(
                f"group_by={group_by}, res['aggregations']={json.dumps(res['aggregations'], indent=2)}"
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

    @staticmethod
    def parse_query_raw_result(
        results: dict,
        include_hits: bool = True,
        include_aggregations: bool = True,
    ) -> list:
        """
        Parse an OpenSearch query result to a more meaningful format.

        Args:
            results (dict): The OpenSearch query result.
            include_hits (bool, optional): Whether to include the hits in the result. Defaults to True.
            include_aggregations (bool, optional): Whether to include the aggregations in the result. Defaults to True.
        Returns:
            list: The parsed result as
            {
                'results': list[dict], # the matched events
                'aggregations': dict,
                'total': int, # total matches
                'search_after': list # search_after for pagination (if limit was set in the query)
            }

        Raises:
            ObjectNotFound: If no results are found.
        """
        # GulpLogger.get_instance().debug(json.dumps(results, indent=2))
        hits = results["hits"]["hits"]
        # GulpLogger.get_instance().debug('hits #=%d, hits node=%s' % (len(hits), hits))
        if not hits:
            raise ObjectNotFound("no results found!")

        parsed_result = {}

        if include_hits:
            parsed_result["results"] = [{**hit["_source"], "_id": hit["_id"]} for hit in hits]

        if include_aggregations and "aggregations" in results:
            parsed_result["aggregations"] = results["aggregations"]

        total_hits = results["hits"]["total"]["value"]
        # GulpLogger.get_instance().debug("hits.total.value: %d ..." % (n["total"]))
        parsed_result["total"] = total_hits

        if hits[-1]["sort"]:
            parsed_result["search_after"] = hits[-1]["sort"]

        # GulpLogger.get_instance().debug(json.dumps(n, indent=2))
        return parsed_result

    async def query_operations(self, index: str):
        """
        Queries the OpenSearch index for operations and returns the aggregations.

        Args:
            index (str): Name of the index (or datastream) to query

        Returns:
            dict: The aggregations result (WARNING: will return at most "aggregation_max_buckets" hits, which should cover 99,99% of the usage ....).
        """
        max_buckets = config.aggregation_max_buckets()

        def _create_terms_aggregation(field):
            return {"terms": {"field": field, "size": max_buckets}}

        aggs = {
            "operations": _create_terms_aggregation("gulp.operation"),
            "aggs": {
                "context": _create_terms_aggregation("gulp.context"),
                "aggs": {
                    "plugin": _create_terms_aggregation("agent.type"),
                    "aggs": {
                        "src_file": _create_terms_aggregation("log.file.path"),
                        "aggs": {
                            "max_gulp.timestamp": {"max": {"field": "gulp.timestamp"}},
                            "min_gulp.timestamp": {"min": {"field": "gulp.timestamp"}},
                            "max_event.code": {"max": {"field": "gulp.event.code"}},
                            "min_event.code": {"min": {"field": "gulp.event.code"}},
                        },
                    },
                },
            },
        }

        body = {
            "track_total_hits": True,
            "aggregations": aggs,
        }
        headers = {
            "content-type": "application/json",
        }

        res = await self._opensearch.search(body=body, index=index, headers=headers)
        hits = res["hits"]["total"]["value"]
        if not hits:
            GulpLogger.get_instance().warning(
                "no results found, returning empty aggregations (possibly no data on opensearch)!"
            )
            # raise ObjectNotFound()
            return {
                "total": 0,
                "aggregations": {
                    "operations": {
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 0,
                        "buckets": [],
                    }
                },
            }

        return {"total": hits, "aggregations": res["aggregations"]}        

    async def query_single_document(self, datastream: str, id: str, is_index: bool=False) -> dict:
        """
        Get a single event from OpenSearch.

        Args:
            index (str): Name of the index (or datastream) to query            
            id (str): The ID of the document to retrieve
            is_index (bool, optional): Whether the datastream is an index or a datastream. Defaults to False (is a datastream).

        Returns:
            dict: The query result.

        Raises:
            ObjectNotFound: If no results are found.
        """
        if not is_index:
            try:
                res = await self._opensearch.indices.get_data_stream(name=datastream)
                index_name = res["data_streams"][0]["indices"][0]["index_name"]
            except (KeyError, IndexError):
                raise ObjectNotFound(f"datastream or index not found: {datastream}")
        else:
            index_name = datastream

        try:
            res = await self._opensearch.get(index=index_name, id=id)
            js = res["_source"]
            js["_id"] = res["_id"]
            return js
        except KeyError:
            raise ObjectNotFound(f"document with ID {id} not found in datastream={datastream} index {index_name}")

    async def query_raw(self, index: str, q: dict, options: GulpQueryAdditionalOptions=None) -> dict:
        """
        Executes a raw DSL query on OpenSearch

        Args:
            index (str): Name of the index (or datastream) to query. may also be a comma-separated list of indices/datastreams, or "*" to query all.
            q (dict): The DSL query to execute (will be run as "query": q }
            options (GulpQueryOptions, optional): Additional query options. Defaults to None.   

        Returns:
            dict: The query result.
        Raises:
            ObjectNotFound: If no results are found.
        """
        if not options:
            options = GulpQueryAdditionalOptions()
        parsed_options = options.parse()

        GulpLogger.get_instance().debug(
            "query_raw: %s, options=%s"
            % (json.dumps(q, indent=2), json.dumps(parsed_options, indent=2))
        )

        body = {"track_total_hits": True, "query": q}
        body.update(parsed_options)

        headers = {
            "content-type": "application/json",
        }
        res = await self._opensearch.search(body=body, index=index, headers=headers)

        # GulpLogger.get_instance().debug("query_raw: res=%s" % (json.dumps(res, indent=2)))
        js = GulpOpenSearch.parse_query_raw_result(res)
        return js


    @staticmethod
    async def query_raw_elastic(
        el: AsyncElasticsearch, index: str, q: dict, options: GulpQueryAdditionalOptions = None
    ) -> dict:
        """
        Executes a raw DSL query on OpenSearch.

        NOTE: we added this static method for commodity, but it may be removed later ....

        Args:
            el(AsyncElasticSearch): the ElasticSearch client to use
            index (str): Name of the index (or datastream) to query
            q (dict): The DSL query to execute (will be run as "query": q }
            options (GulpQueryOptions, optional): Additional query options. Defaults to None.

        Returns:
            dict: The query result.
        Raises:
            ObjectNotFound: If no results are found.
        """
        if not options:
            options = GulpQueryAdditionalOptions()
        parsed_options = options.parse()

        GulpLogger.get_instance().debug(
            "query_raw_elastic: %s, options=%s"
            % (json.dumps(q, indent=2), json.dumps(parsed_options, indent=2))
        )

        res = await el.search(
            index=index,
            track_total_hits=True,
            query=q,
            sort=parsed_options["_sort"],
            size=parsed_options["_size"],
            search_after=parsed_options["search_after"],
            source=parsed_options["source"],
        )
        return GulpOpenSearch().parse_query_raw_result(res)


