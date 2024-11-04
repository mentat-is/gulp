import copy
import json
import os
from urllib.parse import urlparse

import muty.crypto
import muty.dict
import muty.file
import muty.string
import muty.time

import gulp.config as config
from gulp.api.elastic.structs import (
    GulpDocument,
    GulpIngestionFilter,
    GulpQueryFilter,
)
from gulp.defs import GulpEventFilterResult, ObjectNotFound
from gulp.utils import logger

# to be used in dynamic templates
UNMAPPED_PREFIX = "gulp.unmapped"

from elasticsearch import (
    AsyncElasticsearch as AElasticSearch,
)  # use AElasticSearch for Elasticsearch

# NOTE: this was originally written for Elasticsearch, then ported to OpenSearch. maybe it should be refactored to remove this "as" alias.
from opensearchpy import (
    AsyncOpenSearch as AsyncElasticsearch,
)  # use AsyncElasticSearch for OpenSearch

_elastic: AsyncElasticsearch = None

# TODO: turn to singleton class
# TODO: properly use AsyncOpenSearch without aliasing to AsyncElasticSearch

def _parse_mappings(d: dict, parent_key="", result=None) -> dict:
    if result is None:
        result = {}
    for k, v in d.items():
        new_key = "%s.%s" % (parent_key, k) if parent_key else k
        if isinstance(v, dict):
            if "properties" in v:
                _parse_mappings(v["properties"], new_key, result)
            elif "type" in v:
                result[new_key] = v["type"]
        else:
            result[new_key] = v
    return result


async def index_template_delete(el: AsyncElasticsearch, index_name: str) -> None:
    """
    Delete the index template for the given index/datastream.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): The index/datastream name.
    """
    try:
        template_name = "%s-template" % (index_name)
        logger().debug("deleting index template: %s ..." % (template_name))
        await el.indices.delete_index_template(name=template_name)
    except Exception as e:
        logger().error("error deleting index template: %s" % (e))


async def index_template_get(el: AsyncElasticsearch, index_name: str) -> dict:
    """
    Get the index template for the given index/datastream.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): The index/datastream name.

    Returns:
        dict: The index template.
    """
    template_name = "%s-template" % (index_name)
    logger().debug("getting index template: %s ..." % (template_name))

    try:
        res = await el.indices.get_index_template(name=template_name)
    except Exception as e:
        raise ObjectNotFound("no template found for datastream/index %s" % (index_name))

    return res["index_templates"][0]["index_template"]


async def index_template_put(el: AsyncElasticsearch, index_name: str, d: dict) -> dict:
    """
    Put the index template for the given index/datastream.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): The index/datastream name.
        d (dict): The index template.

    Returns:
        dict: The response from the Elasticsearch client.
    """
    template_name = "%s-template" % (index_name)
    logger().debug("putting index template: %s ..." % (template_name))
    headers = {"accept": "application/json", "content-type": "application/json"}
    return await el.indices.put_index_template(
        name=template_name, body=d, headers=headers
    )


async def index_template_set_from_file(
    el: AsyncElasticsearch, index_name: str, path: str, apply_patches: bool = True
) -> dict:
    """
    Asynchronously sets an index template in Elasticsearch from a JSON file.
    Args:
        el (AsyncElasticsearch): The Elasticsearch client instance.
        index_name (str): The name of the index/datastream to set the template for.
        path (str): The file path to the JSON file containing the index template.
        apply_patches (bool, optional): Whether to apply specific patches to the mappings and settings before setting the index template. Defaults to True.
    Returns:
        dict: The response from Elasticsearch after setting the index template.
    """

    logger().debug('loading index template from file "%s" ...' % (path))
    d = muty.dict.from_json_file(path)
    return await build_and_set_index_template(el, index_name, d, apply_patches)


async def build_and_set_index_template(
    el: AsyncElasticsearch, index_name: str, d: dict, apply_patches: bool = True
) -> dict:
    """
    Sets an index template in Elasticsearch, applying the needed patches to the mappings and settings.
    Args:
        el (AsyncElasticsearch): The Elasticsearch client instance.
        index_name (str): The name of the index for which the template is being set.
        d (dict): The dictionary containing the template, mappings, and settings for the index.
        apply_patches (bool, optional): Whether to apply specific patches to the mappings and settings before setting the index template. Defaults to True.
    Returns:
        dict: The response from the Elasticsearch client after setting the index template.
    Raises:
        ValueError: If the 'template' or 'mappings' key is not found in the provided dictionary.
    Notes:
        - The function modifies the provided dictionary to include default settings and mappings if they are not present.
        - It applies specific patches to the mappings and settings before setting the index template.
        - If the Elasticsearch cluster is configured for a single node, the number of replicas is set to 0 to optimize performance.
    """
    logger().debug('setting index template for "%s" ...' % (index_name))
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
            "context": {"type": "keyword"},
            "operation": {"type": "keyword"},
        }
    }
    dtt = []
    dtt.append(
        {
            # force unknown mapping to string
            "force_unmapped_to_string": {
                "match": "%s.*" % (UNMAPPED_PREFIX),
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
        mappings["properties"]["@timestamp"] = {"type": "date_nanos"}
        mappings['numeric_detection'] = False

        # support for original event both as keyword and text
        mappings["properties"]["event"]["properties"]["original"] = {
            "type": "text",
            "analyzer": "standard",
            "fields": {
                "keyword": {
                "type": "keyword",
                "ignore_above": 1024
                }
            }
        }
        settings["index"]["mapping"]["total_fields"] = {
            "limit": config.index_template_default_total_fields_limit()
        }
        settings["index"][
            "refresh_interval"
        ] = config.index_template_default_refresh_interval()
        if not config.elastic_multiple_nodes():
            # optimize for single node
            # this also removes "yellow" node in single node mode
            # this also removes "yellow" node in single node mode
            logger().warning("setting number_of_replicas to 0")
            settings["index"]["number_of_replicas"] = 0

    # write template
    await index_template_put(el, index_name, d)
    return d


async def datastream_get_key_value_mapping(
    el: AsyncElasticsearch, index_name: str, return_raw_result: bool = False
) -> dict:
    """
    Get and parse mappings for the given datastream or index: it will result in a dict (if return_raw_result is not set) like:
    {
        "field1": "type",
        "field2": "type",
        ...
    }

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): an index/datastream to query
        return_raw_result (bool, optional): Whether to return the raw result (mapping + settings). Defaults to False.
    Returns:
        dict: The mapping dict.
    """
    try:
        res = await el.indices.get_mapping(index=index_name)
    except Exception as e:
        logger().warning('no mapping for index "%s" found: %s' % (index_name, e))
        return {}

    # logger().debug("index_get_mapping: %s" % (json.dumps(res, indent=2)))
    if return_raw_result:
        return res

    idx = list(res.keys())[0]
    properties = res[idx]["mappings"]["properties"]
    return _parse_mappings(properties)


async def datastream_get_mapping_by_src(
    el: AsyncElasticsearch, index_name: str, context: str, src_file: str
) -> dict:
    """
    Get and parse mappings for the given index, considering only "gulp.context"=context and "log.file.path"=src_file.

    The return type is the same as index_get_mapping with return_raw_result=False.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): an index/datastream to query
        src_file (str): the source file to filter by

    Returns:
        dict: The mapping dict.
    """
    # query first
    options = {} # GulpQueryOptions()
    options.limit = 1000
    options.fields_filter = ["*"]
    q = {
        "query_string": {
            "query": 'gulp.context: "%s" AND gulp.source.file: "%s"'
            % (context, src_file)
        }
    }

    # get mapping
    mapping = await datastream_get_key_value_mapping(el, index_name)
    filtered_mapping = {}

    # loop with query_raw until there's data and update filtered_mapping
    while True:
        try:
            docs = await query_raw(el, index_name, q, options)
            # logger().debug("docs: %s" % (json.dumps(docs, indent=2)))
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


async def datastream_list(el: AsyncElasticsearch) -> list[str]:
    """
    Retrieves a list of datastream names (with associated indices) from Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.

    Returns:
        list[dict]: The list of datastreams with their backing index as
        {
            "name": "datastream_name",
            "indexes": ["index1", "index2", ...], <== this is always a list of one element for gulp datastreams
            "template": "template_name"
        }
    """
    headers = {"accept": "text/plain,application/json"}
    l = await el.indices.get_data_stream(headers=headers)
    # logger().debug(json.dumps(l, indent=2))
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


async def datastream_delete(el: AsyncElasticsearch, datastream_name: str) -> None:
    """
    Delete the datastream and associated index and template from Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        datastream_name (str): The datastream name as passed to "index_create"

    Returns:
        None
    """
    ds: str = datastream_name

    # params = {"ignore_unavailable": "true"}
    headers = {"accept": "application/json"}
    try:
        logger().debug('deleting datastream "%s" ...' % (ds))
        await el.indices.delete_data_stream(ds, headers=headers)
    except Exception as e:
        logger().error("error deleting datastream: %s" % (e))
        pass
    try:
        await index_template_delete(el, datastream_name)
    except Exception as e:
        logger().error("error deleting index template: %s" % (e))


async def datastream_create(
    el: AsyncElasticsearch, datastream_name: str, index_template: str = None
) -> None:
    """
    (re)creates the Elasticsearch datastream (with backing index) and associates the index template from configuration (or uses the default).

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        datastream_name(str): The name of the datastream to be (re)created, the index template will be re/created as "<index_name>-template".
        total.
        index_template (str, optional): path to the index template to use. Defaults to None (use the default index template).
    Returns:
        dict
    """
    ds: str = datastream_name
    logger().debug('re/creating datastream "%s" ...' % (ds))
    await datastream_delete(el, datastream_name)

    # create index template, check if we are overriding the default index template.
    # if so, we only apply gulp-related patching and leaving everything else as is
    apply_patches = True
    if index_template is None:
        template_path = config.path_index_template()
    else:
        template_path = index_template
        logger().debug('using custom index template "%s" ...' % (template_path))
        apply_patches = False
    await index_template_set_from_file(
        el, datastream_name, template_path, apply_patches=apply_patches
    )

    try:
        # create datastream
        headers = {"accept": "application/json", "content-type": "application/json"}
        r = await el.indices.create_data_stream(ds, headers=headers)
        logger().debug("datastream created: %s" % (r))
        return r
    except Exception as e:
        # delete the index template
        await e
        raise e

def filter_doc_for_ingestion(
    doc: dict,
    flt: GulpIngestionFilter = None
) -> GulpEventFilterResult:
    """
    Check if a document is eligible for ingestion based on a filter.

    Args:
        doc (dict): The GulpDocument dictionary to check.
        flt (GulpIngestionFilter): The filter parameters, if any.

    Returns:
        GulpEventFilterResult: The result of the filter check.
    """
    # logger().error(flt)
    if not flt or flt.opt_storage_ignore_filter:
        # empty filter or ignore
        return GulpEventFilterResult.ACCEPT

    if flt.time_range:
        ts = doc["@timestamp"]
        if ts <= flt.time_range[0] or ts >= flt.time_range[1]:
            return GulpEventFilterResult.SKIP

    return GulpEventFilterResult.ACCEPT


def _bulk_docs_result_to_ingest_chunk(
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
    if errors is None:
        return [doc for _, doc in bulk_docs]

    error_ids = {error["create"]["_id"] for error in errors}
    result = [
        {**doc, "_id": create_doc["create"]["_id"]}
        for create_doc, doc in zip(bulk_docs[::2], bulk_docs[1::2])
        if create_doc["create"]["_id"] not in error_ids
    ]

    return result


async def ingest_bulk(
    el: AsyncElasticsearch,
    index: str,
    docs: list[dict],
    flt: GulpIngestionFilter = None,
    wait_for_refresh: bool = False,
) -> tuple[int, int, list[dict]]:
    """
    ingests a list of GulpDocument into Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
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
    # len_first = len(docs)
    """
    bulk_docs = []
    for item in docs:
        if not flt or filter_doc_for_ingestion(item, flt) != GulpEventFilterResult.SKIP:
            bulk_docs.append({"create": {"_id": item["_id"]}})
            bulk_docs.append(item)
    """
    bulk_docs = [
        doc
        for item in docs
        if not flt or filter_doc_for_ingestion(item, flt) != GulpEventFilterResult.SKIP
        # create a tuple with the create action and the document
        #   {
        #     "create": {"_id": item["_id"]},
        #     { doc stripped of _id }
        #   }
        for doc in ({"create": {"_id": item["_id"]}}, {k: v for k, v in item.items() if k != "_id"})
    ]
    # logger().error('ingesting %d documents (was %d before filtering)' % (len(docs) / 2, len_first))
    if len(bulk_docs) == 0:
        logger().warning("no document to ingest (flt=%s)" % (flt))
        return 0, 0, []

    # logger().info("ingesting %d docs: %s\n" % (len(bulk_docs) / 2, json.dumps(bulk_docs, indent=2)))

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

    res = await el.bulk(body=bulk_docs, index=index, params=params, headers=headers)
    skipped = 0
    failed = 0
    ingested: list[dict] = []
    if res["errors"]:
        # count skipped
        skipped = sum(1 for r in res["items"] if r["create"]["status"] == 409)
        # count failed
        failed = sum(1 for r in res["items"] if r["create"]["status"] not in [201, 200, 409])
        if failed > 0:
            logger().error(
                "failed to ingest %d documents: %s"
                % (failed, muty.string.make_shorter(str(res["items"]), max_len=10000))
            )

        # take only the documents with no ingest errors
        ingested = _bulk_docs_result_to_ingest_chunk(bulk_docs, res["items"])
    else:
        # no errors, take all documents
        ingested = _bulk_docs_result_to_ingest_chunk(bulk_docs)

    if skipped != 0:
        # NOTE: bulk_docs/2 is because the bulk_docs is a list of tuples (create, doc)
        logger().error(
            "**NOT AN ERROR** total %d skipped, %d failed in this bulk ingestion of %d documents !"
            % (skipped, failed, len(bulk_docs) / 2)
        )
    if failed > 0:
        logger().critical("failed is set, ingestion format needs to be fixed!")
    return skipped, failed, ingested


async def rebase(
    el: AsyncElasticsearch,
    index: str,
    dest_index: str,
    msec_offset: int,
    flt: GulpQueryFilter = None,
) -> dict:
    """
    Rebase documents from one Elasticsearch index to another with a timestamp offset.
    Args:
        el (AsyncElasticsearch): The Elasticsearch client instance.
        index (str): The source index name.
        dest_index (str): The destination index name.
        msec_offset (int): The offset in milliseconds to adjust the '@timestamp' field.
        flt (GulpQueryFilter, optional): A filter to apply to the query. Defaults to None.
    Returns:
        dict: The response from the Elasticsearch reindex operation.

    """
    logger().debug(
        "rebase index %s to %s with offset=%d, flt=%s ..."
        % (index, dest_index, msec_offset, flt)
    )
    q = gulpqueryflt_to_elastic_dsl(flt)
    convert_script = """
        if (ctx._source['@timestamp'] != 0) {
            ctx._source['@timestamp'] += params.nsec_offset;
        }
    """

    body: dict = {
        "source": {"index": index, "query": q["query"]},
        "dest": {"index": dest_index, "op_type": "create"},
        "script": {
            "lang": "painless",
            "source": convert_script,
            "params": {
                "nsec_offset": msec_offset*muty.time.MILLISECONDS_TO_NANOSECONDS,
            },
        },
    }
    params: dict = {"refresh": "true", "wait_for_completion": "true", "timeout": 0}
    headers = {
        "accept": "application/json",
        "content-type": "application/x-ndjson",
    }

    logger().debug("rebase body=%s" % (body))
    res = await el.reindex(body=body, params=params, headers=headers)
    return res


def _parse_elastic_res(
    results: dict,
    include_hits: bool = True,
    include_aggregations: bool = True,
    options = None,
) -> list:
    """
    Parse an Elasticsearch query result.

    Args:
        results (dict): The Elasticsearch query result.
        include_hits (bool, optional): Whether to include the hits in the result. Defaults to True.
        include_aggregations (bool, optional): Whether to include the aggregations in the result. Defaults to True.
        options (GulpQueryOptions, optional): the gulp query options
    Returns:
        list: The parsed result as
        {
            'results': list[dict], # the matched events
            'aggregations': dict,
            'total': int, # total matches
            'search_after': list # search_after for pagination
        }

    Raises:
        ObjectNotFound: If no results are found.
    """
    # logger().warning(results)
    hits = results["hits"]["hits"]
    # logger().debug('hits #=%d, hits node=%s' % (len(hits), hits))
    if len(hits) == 0:
        raise ObjectNotFound("no results found!")

    # logger().debug(json.dumps(results, indent=2))
    n = {}
    if include_hits:
        nres = []
        # return hits
        # logger().debug("hits len: %d ..." % (len(hits)))
        for hit in hits:
            s = hit["_source"]
            s["_id"] = hit["_id"]
            nres.append(s)
        n["results"] = nres

    aggregations = results.get("aggregations", None)
    if aggregations is not None and include_aggregations:
        # return aggregations
        n["aggregations"] = aggregations

    n["total"] = results["hits"]["total"]["value"]
    # logger().debug("hits.total.value: %d ..." % (n["total"]))
    if n.get("results", None) is not None:
        # this is the total hits
        t = n["total"]
        if options is not None and options.limit > 0 and t > options.limit:
            # add search_after to allow pagination
            n["search_after"] = results["hits"]["hits"][-1]["sort"]
    # logger().debug(json.dumps(n, indent=2))
    return n


def _parse_query_max_min(input_dict: dict) -> dict:
    output_dict = {"buckets": [], "total": 0}
    for bucket in input_dict["by_type"]["buckets"]:
        key = bucket["key"]
        doc_count = bucket["doc_count"]
        max_event_code = bucket["max_event.code"]["value"]
        min_timestamp = bucket["min_@timestamp"]["value"]
        max_timestamp = bucket["max_@timestamp"]["value"]
        min_event_code = bucket["min_event.code"]["value"]
        output_dict["buckets"].append(
            {
                key: {
                    "doc_count": doc_count,
                    "max_event.code": max_event_code,
                    "min_@timestamp": min_timestamp,
                    "max_@timestamp": max_timestamp,
                    "min_event.code": min_event_code,
                }
            }
        )
        output_dict["total"] += doc_count
    return output_dict


async def delete_data_by_operation(
    el: AsyncElasticsearch, index: str, operation_id: int, refresh: bool = True
):
    """
    Deletes data from Elasticsearch by operation ID.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to delete data from.
        operation_id (int): The ID of the operation.
        refresh (bool, optional): Whether to refresh the index after deletion. Defaults to True.

    Returns:
        None
    """
    q = {"query": {"term": {"gulp.operation.id": operation_id}}}

    params = None
    if refresh:
        params = {"refresh": "true"}
    headers = {
        "accept": "application/json",
        "content-type": "application/x-ndjson",
    }

    await el.delete_by_query(index=index, body=q, params=params, headers=headers)


async def query_max_min_per_field(
    el: AsyncElasticsearch,
    index: str,
    group_by: str = None,
    flt: GulpQueryFilter = None,
):
    """
    Queries the maximum and minimum @timestamp and event.code in an index, grouping per type if specified.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to query.
        group_by (str): The field to group by (None to consider all fields).
        flt (GulpQueryFilter): The query filter.

    Returns:
        dict: The query result.
    """
    aggregations = {
        "count": {"value_count": {"field": "@timestamp"}},
        "max_@timestamp": {"max": {"field": "@timestamp"}},
        "min_@timestamp": {"min": {"field": "@timestamp"}},
        "max_event.code": {"max": {"field": "gulp.event.code"}},
        "min_event.code": {"min": {"field": "gulp.event.code"}},
    }
    if group_by is not None:
        d = {
            "by_type": {
                "terms": {"field": group_by},
                "aggs": copy.deepcopy(aggregations),
            }
        }
        aggregations = d
        logger().debug(
            "aggregations with group_by=%s: %s"
            % (group_by, json.dumps(aggregations, indent=2))
        )

    q = gulpqueryflt_to_elastic_dsl(flt)
    logger().debug("query_max_min_per_field: q=%s" % (json.dumps(q, indent=2)))
    body = {"track_total_hits": True, "query": q["query"], "aggregations": aggregations}
    headers = {
        "content-type": "application/json",
    }

    res = await el.search(body=body, index=index, headers=headers)
    hits = res["hits"]["total"]["value"]
    if hits == 0:
        raise ObjectNotFound()
    # print(json.dumps(res, indent=2))
    if group_by is not None:
        logger().debug(
            "group_by=%s, res['aggregations']=%s"
            % (group_by, json.dumps(res["aggregations"], indent=2))
        )
        return _parse_query_max_min(res["aggregations"])

    # no group by, standardize the result
    d = {
        "by_type": {
            "buckets": [
                {
                    "key": "*",
                    "doc_count": res["aggregations"]["count"]["value"],
                    "max_event.code": res["aggregations"]["max_event.code"],
                    "min_event.code": res["aggregations"]["min_event.code"],
                    "min_@timestamp": res["aggregations"]["min_@timestamp"],
                    "max_@timestamp": res["aggregations"]["max_@timestamp"],
                }
            ]
        }
    }
    return _parse_query_max_min(d)


async def index_refresh(el: AsyncElasticsearch, index: str) -> None:
    """
    Refresh an index(=make changes available to search) in Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to refresh.

    Returns:
        None
    """
    logger().debug("refreshing index: %s" % (index))
    res = await el.indices.refresh(index=index)
    logger().debug("refreshed index: %s" % (res))


async def query_operations(el: AsyncElasticsearch, index: str):
    """
    Queries the Elasticsearch index for operations and returns the aggregations.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to query

    Returns:
        dict: The aggregations result (WARNING: will return at most "aggregation_max_buckets" hits, which should cover 99,99% of the usage ....).

    Raises:
        ObjectNotFound: If no results are found.
    """
    max_buckets = config.aggregation_max_buckets()
    aggs = {
        "operations": {
            "terms": {"field": "gulp.operation.id", "size": max_buckets},
            "aggs": {
                "context": {
                    "terms": {"field": "gulp.context", "size": max_buckets},
                    "aggs": {
                        "plugin": {
                            "terms": {
                                "field": "agent.type",
                                "size": max_buckets,
                            },
                            "aggs": {
                                "src_file": {
                                    "terms": {
                                        "field": "log.file.path",
                                        "size": max_buckets,
                                    },
                                    "aggs": {
                                        "max_@timestamp": {
                                            "max": {"field": "@timestamp"}
                                        },
                                        "min_@timestamp": {
                                            "min": {"field": "@timestamp"}
                                        },
                                        "max_event.code": {
                                            "max": {"field": "gulp.event.code"}
                                        },
                                        "min_event.code": {
                                            "min": {"field": "gulp.event.code"}
                                        },
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }
    }

    body = {
        "track_total_hits": True,
        "aggregations": aggs,
    }
    headers = {
        "content-type": "application/json",
    }

    res = await el.search(body=body, index=index, headers=headers)
    hits = res["hits"]["total"]["value"]
    if hits == 0:
        logger().warning(
            "no results found, returning empty aggregations (possibly no data on elasticsearch)!"
        )
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

        # raise ObjectNotFound()

    return {"total": hits, "aggregations": res["aggregations"]}


async def query_single_event(el: AsyncElasticsearch, index: str, gulp_id: str) -> dict:
    """
    Get a single event from Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the datastream to query
        gulp_id (str): The ID of the event to retrieve.

    Returns:
        dict: The query result.

    Raises:
        ObjectNotFound: If no results are found.
    """
    # get index name from datastream name
    res = await el.indices.get_data_stream(name=index)
    index_name = res["data_streams"][0]["indices"][0]["index_name"]

    # query index
    res = await el.get(index=index_name, id=gulp_id)
    js = res["_source"]
    js["_id"] = res["_id"]
    return js


async def query_raw(
    el: AsyncElasticsearch, index: str, q: dict, options = None
) -> dict:
    """
    Executes a raw DSL query on OpenSearch

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to query
        q (dict): The DSL query to execute (will be run as "query": q }
        options (GulpQueryOptions, optional): Additional query options. Defaults to None.

    Returns:
        dict: The query result.
    Raises:
        ObjectNotFound: If no results are found.
    """
    from gulp.api.elastic.query_utils import build_elastic_query_options

    opts = build_elastic_query_options(options)
    logger().debug(
        "query_raw: %s, options=%s"
        % (json.dumps(q, indent=2), json.dumps(opts, indent=2))
    )

    body = {"track_total_hits": True, "query": q}
    if opts["source"] is not None:
        body["_source"] = opts["source"]
    if opts["search_after"] is not None:
        body["search_after"] = opts["search_after"]
    if opts["sort"] is not None:
        body["sort"] = opts["sort"]
    if opts["size"] is not None:
        body["size"] = opts["size"]

    headers = {
        "content-type": "application/json",
    }
    res = await el.search(body=body, index=index, headers=headers)

    # logger().debug("query_raw: res=%s" % (json.dumps(res, indent=2)))
    js = _parse_elastic_res(res, options=options)
    return js


async def query_raw_elastic(
    el: AElasticSearch, index: str, q: dict, options = None
) -> dict:
    """
    Executes a raw DSL query on Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to query
        q (dict): The DSL query to execute (will be run as "query": q }
        options (GulpQueryOptions, optional): Additional query options. Defaults to None.

    Returns:
        dict: The query result.
    Raises:
        ObjectNotFound: If no results are found.
    """
    from gulp.api.elastic.query_utils import build_elastic_query_options

    opts = build_elastic_query_options(options)
    logger().debug(
        "query_raw: %s, options=%s"
        % (json.dumps(q, indent=2), json.dumps(opts, indent=2))
    )

    res = await el.search(
        index=index,
        track_total_hits=True,
        query=q,
        sort=opts["sort"],
        size=opts["size"],
        search_after=opts["search_after"],
        source=opts["source"],
    )
    return _parse_elastic_res(res, options=options)


def _get_client() -> AsyncElasticsearch:
    """
    Create and return an Elasticsearch client.

    for https, CA cert is expected to be "elastic-ca.pem" in the certs directory

    Returns:
        AsyncElasticsearch: An instance of the Elasticsearch client.

    """
    url = config.elastic_url()
    verify_certs = config.elastic_verify_certs()

    # split into user:pwd@host:port
    parsed = urlparse(url)

    # url = "%s://%s:***********@%s:%s" % (parsed.scheme, parsed.username, parsed.hostname, parsed.port)
    # logger().debug('%s, elastic hostname=%s, port=%d, user=%s, password=***********' % (url, parsed.hostname, parsed.port, parsed.username))

    host = parsed.scheme + "://" + parsed.hostname + ":" + str(parsed.port)
    ca = None
    certs_dir = config.path_certs()
    if certs_dir is not None and parsed.scheme.lower() == "https":
        # https and certs_dir is set
        ca: str = muty.file.abspath(
            muty.file.safe_path_join(certs_dir, "elastic-ca.pem")
        )

        # check if client certificate exists. if so, it will be used
        client_cert = muty.file.safe_path_join(certs_dir, "elastic.pem")
        client_key = muty.file.safe_path_join(certs_dir, "elastic.key")
        if os.path.exists(client_cert) and os.path.exists(client_key):
            logger().debug(
                "using client certificate: %s, key=%s, ca=%s"
                % (client_cert, client_key, ca)
            )
            return AsyncElasticsearch(
                host,
                use_ssl=True,
                http_auth=(parsed.username, parsed.password),
                ca_certs=ca,
                client_cert=client_cert,
                client_key=client_key,
                verify_certs=verify_certs,
            )
        else:
            logger().debug(
                "no client certificate found, using CA certificate only: %s" % (ca)
            )
            return AsyncElasticsearch(
                host,
                use_ssl=True,
                http_auth=(parsed.username, parsed.password),
                ca_certs=ca,
                verify_certs=verify_certs,
            )

    # no https
    el = AsyncElasticsearch(host, http_auth=(parsed.username, parsed.password))
    logger().debug("created elasticsearch client: %s" % (el))
    return el


async def shutdown_client(el: AsyncElasticsearch) -> None:
    """
    Shutdown the Elasticsearch client.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client to be shutdown.

    Returns:
        None
    """
    if el is not None:
        await el.close()
        logger().debug("elasticsearch client shutdown: %s" % (el))


async def check_alive(el: AsyncElasticsearch) -> None:
    """
    Check if the Elasticsearch client is alive.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.

    Raises:
        Exception: If the client is not reachable

    """
    res = await el.info()
    logger().debug("elasticsearch info: %s" % (res))


def elastic(invalidate: bool = False) -> AsyncElasticsearch:
    """
    Returns the ElasticSearch client instance.

    Args:
        invalidate (bool, optional): Whether to invalidate the current ElasticSearch client instance. Defaults to False.

    Returns:
        The ElasticSearch client instance.
    """
    global _elastic
    if invalidate:
        _elastic = None

    if _elastic is None:
        # create
        _elastic = _get_client()
    return _elastic
