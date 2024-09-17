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
    GulpFieldsFilterType,
    GulpIngestionFilter,
    GulpQueryFilter,
    GulpQueryOptions,
    gulpqueryflt_to_dsl,
)
from gulp.defs import GulpEventFilterResult, ObjectNotFound
from gulp.utils import logger

# to be used in dynamic templates
UNMAPPED_PREFIX = "gulp.unmapped"

# NOTE: this was originally written for Elasticsearch, then ported to OpenSearch. maybe it should be refactored to remove this "as" alias.
from opensearchpy import AsyncOpenSearch as AsyncElasticsearch

_elastic: AsyncElasticsearch = None


def _build_query_options(opt: GulpQueryOptions = None) -> dict:
    """
    Translates GulpQueryOptions into Elasticsearch query options.

    Args:
        opt (GulpQueryOptions, optional): The query options. Defaults to None (default options=sort by @timestamp desc).

    Returns:
        dict: The built query options.
    """
    logger().debug(opt)
    if opt is None:
        # use default
        opt = GulpQueryOptions(fields_filter=GulpFieldsFilterType.DEFAULT)

    n = {}
    if opt.sort is not None:
        # add sort options
        n["sort"] = []
        for k, v in opt.sort.items():
            n["sort"].append({k: {"order": v}})
        # we also add event.hash and event.sequence among sort options to return very distinct results (sometimes, timestamp granularity is not enogh)
        if "event.hash" not in opt.sort:
            n["sort"].append({"event.hash": {"order": "asc"}})
        if "event.sequence" not in opt.sort:
            n["sort"].append({"event.sequence": {"order": "asc"}})
    else:
        # default sort
        n["sort"] = [
            {"@timestamp": {"order": "asc"}},
            # {"event.hash": {"order": "asc"}},
        ]

    if opt.limit is not None:
        # use provided
        n["size"] = opt.limit
    else:
        # default
        n["size"] = 1000

    if opt.search_after is not None:
        # next chunk from this point
        n["search_after"] = opt.search_after
    else:
        n["search_after"] = None

    n["source"] = None
    if opt.fields_filter is not None:
        if opt.fields_filter == GulpFieldsFilterType.ALL:
            # all fields
            n["source"] = None
        elif opt.fields_filter == GulpFieldsFilterType.DEFAULT.name:
            # default set
            opt.fields_filter = GulpFieldsFilterType.DEFAULT.value
        else:
            # only return these fields
            n["source"] = opt.fields_filter.split(",")

    # logger().debug("query options: %s" % (json.dumps(n, indent=2)))
    return n


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


async def datastream_get_mapping(el: AsyncElasticsearch, datastream_name: str) -> dict:
    """
    Get and parse mappings for the given datastream's backing index: it will result in a dict like:
    {
        "field1": "type",
        "field2": "type",
        ...
    }

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        datastream_name (str): The datastream name as passed to "index_create"

    Returns:
        dict: The mapping dict.
    """
    idx = datastream_name
    try:
        res = await el.indices.get_index_template(name=idx)
    except Exception as e:
        logger().warning(
            'no template for datastream/index "%s" found: %s' % (datastream_name, e)
        )
        return {}

    # logger().debug("index_get_mapping: %s" % (json.dumps(res, indent=2)))
    properties = res["index_templates"][0]["index_template"]["template"]["mappings"][
        "properties"
    ]
    return _parse_mappings(properties)


async def index_get_mapping(
    el: AsyncElasticsearch, index_name: str, return_raw_result: bool = False
) -> dict:
    """
    Get and parse mappings for the given index: it will result in a dict (if return_raw_result is not set) like:
    {
        "field1": "type",
        "field2": "type",
        ...
    }

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): an index name
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


def _get_filtered_mapping(docs: list[dict], mapping: dict) -> dict:
    filtered_mapping = {}

    for key in mapping:
        if all(key in doc for doc in docs):
            filtered_mapping[key] = mapping[key]

    return filtered_mapping


async def index_get_mapping_by_src(
    el: AsyncElasticsearch, index_name: str, context: str, src_file: str
) -> dict:
    """
    Get and parse mappings for the given index, considering only "gulp.source.file"=src_file and "gulp.context"=context.

    The return type is the same as index_get_mapping with return_raw_result=False.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index_name (str): an index/datastream to query
        src_file (str): the source file to filter by

    Returns:
        dict: The mapping dict.
    """
    # query first
    options = GulpQueryOptions()
    options.limit = 1000
    options.fields_filter = GulpFieldsFilterType.ALL
    q = {
        "query_string": {
            "query": 'gulp.context: "%s" AND gulp.source.file: "%s"'
            % (context, src_file)
        }
    }

    # get the first 1000 documents as a sample
    docs = await query_raw(el, index_name, q, options)
    docs = docs["results"]

    # get mapping
    mapping = await index_get_mapping(el, index_name)
    js = _get_filtered_mapping(docs, mapping)
    if len(js) == 0:
        raise ObjectNotFound("no mapping found for src_file=%s" % (src_file))

    return js


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
    template_name: str = datastream_name + "-template"
    logger().debug(
        'deleting datastream "%s" and template %s (if existing) ...'
        % (template_name, ds)
    )

    # params = {"ignore_unavailable": "true"}
    headers = {"accept": "application/json"}
    try:
        await el.indices.delete_data_stream(ds, headers=headers)
    except Exception as e:
        logger().error("error deleting datastream: %s" % (e))
        pass
    try:
        await el.indices.delete_index_template(name=template_name, headers=headers)
    except Exception as e:
        logger().error("error deleting index template: %s" % (e))


async def datastream_create(
    el: AsyncElasticsearch,
    datastream_name: str,
    max_total_fields: int = 10000,
    refresh_interval_msec=5000,
    force_date_detection: bool = False,
    event_original_text_analyzer: str = "standard",
) -> None:
    """
    (re)creates the Elasticsearch datastream (with backing index) and associates the index template from configuration (or uses the default).

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        datastream_name(str): The name of the datastream to be (re)created, the index template will be re/created as "<index_name>-template".
        total.
        max_total_fields (int, optional): The maximum number of fields in the index. Defaults to 10000.
        refresh_interval_msec (int, optional): The refresh interval in milliseconds. Defaults to 5000 (5s).
        force_date_detection (bool, optional): Whether to force date detection. Defaults to False.
        event_original_text_analyzer (str, optional): The text analyzer to use for "event.original". Defaults to 'standard'.
    Returns:
        dict
    """
    ds: str = datastream_name
    logger().debug('re/creating datastream "%s" ...' % (ds))
    await datastream_delete(el, datastream_name)

    # minimal template
    # mappings = {"properties": {"event": {"properties": {"original": {}}}}}
    settings = {
        # indices.mapping.dynamic_timeout
        # index.mapping.total_fields.ignore_dynamic_beyond_limit
        "index": {
            "mapping": {
                "total_fields": {
                    "limit": max_total_fields,
                }
            },
            "refresh_interval": muty.time.time_definition_from_milliseconds(
                float(refresh_interval_msec)
            ),
        },
    }

    # enforce date detection ?
    mappings: dict = {}
    mappings["date_detection"] = force_date_detection

    # map gulp fields
    specific_gulp_mapping: dict = {
        "log": {
            "properties": {
                "level": {"type": "keyword"},
            }
        },
        "source": {"properties": {"domain": {"type": "keyword"}}},
        "event": {
            "properties": {
                "hash": {"type": "keyword"},
                "category": {"type": "keyword"},
                "original": {"type": "text", "analyzer": event_original_text_analyzer},
                "duration": {"type": "long"},
                "id": {"type": "keyword"},
                "sequence": {"type": "long"},
                "code": {"type": "keyword"},
            }
        },
        "gulp": {
            "properties": {
                "event": {"properties": {"code": {"type": "long"}}},
                "context": {"type": "keyword"},
                "source": {"properties": {"file": {"type": "keyword"}}},
                "log": {"properties": {"level": {"type": "integer"}}},
            }
        },
        "agent": {"properties": {"type": {"type": "keyword"}}},
        "@timestamp_nsec": {"type": "long"},
        "@timestamp": {"type": "date", "format": "epoch_millis"},
        "operation_id": {"type": "integer"},
    }
    mappings["properties"] = specific_gulp_mapping

    # add dynamic templates for unmapped fields
    dt = [
        {
            # all unmapped fields are mapped to "keyword", to avoid type boundaries issues
            "force_unmapped_to_string": {
                # "match_mapping_type": "long",
                "match": "%s.*" % (UNMAPPED_PREFIX),
                "mapping": {"type": "keyword"},
            }
        }
    ]
    mappings["dynamic_templates"] = dt

    # print(mappings['dynamic_templates'])

    logger().debug("settings: %s" % (json.dumps(settings, indent=2)))
    logger().debug("mappings: %s" % (json.dumps(mappings, indent=2)))
    if not config.elastic_multiple_nodes():
        # optimize for single node
        # this also removes "yellow" node in single node mode
        logger().warning("setting number_of_replicas to 0")
        settings["index"]["number_of_replicas"] = 0

    ###############
    # rebuild template
    ###############
    t = {}
    # use this template just for this index
    t["index_patterns"] = [datastream_name]
    t["data_stream"] = {}
    t["priority"] = 100
    t["template"] = {}
    t["template"]["mappings"] = mappings

    # always use settings
    t["template"]["settings"] = settings

    # create index, template and data stream
    headers = {"accept": "application/json", "content-type": "application/json"}
    # logger().debug(json.dumps(m, indent=2))
    template_name: str = datastream_name + "-template"
    r = await el.indices.put_index_template(name=template_name, body=t, headers=headers)
    logger().debug("template created: %s" % (r))
    # await el.indices.create(index=idx, headers=headers)
    # logger().debug("index created: %s" % (r))
    r = await el.indices.create_data_stream(ds, headers=headers)
    logger().debug("datastream created: %s" % (r))
    return r


def filter_doc_for_ingestion(
    doc: GulpDocument | dict,
    flt: GulpIngestionFilter = None,
    ignore_store_all_documents: bool = False,
) -> GulpEventFilterResult:
    """
    Check if a document is eligible for ingestion based on a filter.

    Args:
        doc (GulpDocument|dict): The document to check.
        flt (GulpIngestionFilter): The filter parameters, if any.
        ignore_store_all_documents (bool, optional): Whether to ignore the store_all_documents flag. Defaults to False.

    Returns:
        GulpEventFilterResult: The result of the filter check.
    """
    # logger().error(flt)
    if (
        flt is None
        or flt.store_all_documents
        and not ignore_store_all_documents
        or (
            flt.category is None
            and flt.event_code is None
            and flt.gulp_log_level is None
            and flt.start_msec is None
            and flt.end_msec is None
            and flt.extra is None
        )
    ):
        # empty filter or store_all_documents is set
        return GulpEventFilterResult.ACCEPT

    is_gulp_doc = isinstance(doc, GulpDocument)
    filter_res = GulpEventFilterResult.SKIP
    if is_gulp_doc:
        # turn to dict
        dd = doc.to_dict()
    else:
        dd = doc

    if flt.category is not None:
        doc_category = set(dd.get("event.category", []))
        if doc_category.intersection(flt.category):
            filter_res = GulpEventFilterResult.ACCEPT
        else:
            return GulpEventFilterResult.SKIP

    if flt.event_code is not None:
        event_code = dd.get("event.code", None)
        if event_code is None:
            event_code = dd.get("gulp.event.code", None)

        res = str(event_code) in flt.event_code
        if res:
            filter_res = GulpEventFilterResult.ACCEPT
        else:
            return GulpEventFilterResult.SKIP

    if flt.gulp_log_level is not None:
        ll = dd.get("gulp.log.level", None)
        if ll is not None:
            res = int(ll) in flt.gulp_log_level
        else:
            # accept
            res = True
        if res:
            filter_res = GulpEventFilterResult.ACCEPT
        else:
            return GulpEventFilterResult.SKIP

    if flt.start_msec is not None:
        res = dd["@timestamp"] >= flt.start_msec
        if res:
            filter_res = GulpEventFilterResult.ACCEPT
        else:
            return GulpEventFilterResult.SKIP

    if flt.end_msec is not None:
        res = dd["@timestamp"] <= flt.end_msec
        if res:
            filter_res = GulpEventFilterResult.ACCEPT
        else:
            return GulpEventFilterResult.SKIP

    if flt.extra and not flt.store_all_documents:
        extra_res = GulpEventFilterResult.SKIP
        for k, v in flt.extra.items():
            if k in dd:
                if dd[k] == v:
                    extra_res = GulpEventFilterResult.ACCEPT

        if extra_res == GulpEventFilterResult.ACCEPT:
            filter_res = GulpEventFilterResult.ACCEPT

    # filtered out
    return filter_res


def _build_bulk_docs(
    docs: list[GulpDocument | dict], flt: GulpIngestionFilter = None
) -> list[dict]:
    """
    Build a list of documents for Elasticsearch bulk indexing.

    Args:
        docs (list[GulpDocument | dict]): The list of documents to be indexed.
        flt (GulpIngestionFilter, optional): The filter parameters to apply. Defaults to None.

    Returns:
        list[dict]: The list of bulk documents ready to be ingested.

    """

    def process_doc(d):
        is_gulp_doc = isinstance(d, GulpDocument)
        if is_gulp_doc:
            # we may have event.code in extra, to override
            if d.extra.get("event.code", None) is not None:
                d.event_code = d.extra["event.code"]

            # print('*******ev_code=%s' % (d.event_code))
            ev_id = muty.crypto.hash_blake2b(
                str(d.event_code)
                + str(d.operation_id)
                + d.context
                + str(d.extra)
                + d.hash[:32]
            )
        else:
            # remove the original id and recalculate
            dd: dict = d
            _id = dd.get("_id", None)
            if _id is not None:
                del dd["_id"]
            ev_id = muty.crypto.hash_blake2b(str(dd.values()))

        return {"create": {"_id": ev_id}}, (d.to_dict() if is_gulp_doc else d)

    return [
        item
        for d in docs
        if filter_doc_for_ingestion(d, flt) == GulpEventFilterResult.ACCEPT
        for item in process_doc(d)
    ]


def _bulk_docs_result_to_ingest_chunk(
    bulk_docs: list[dict], errors: list[dict] = None
) -> list[dict]:
    """
    Extracts the ingested documents from a bulk ingestion result.
    """
    # create a set of _id values from the errors list
    if errors is None:
        # no errors
        errors = []
        error_ids = []
    else:
        error_ids = {error["create"]["_id"] for error in errors}

    result = []

    # iterate through bulk_docs in steps of 2, since bulk_docs is a list of tuples (create, doc)
    for i in range(0, len(bulk_docs), 2):
        create_doc = bulk_docs[i]
        data_doc = bulk_docs[i + 1]

        _id = create_doc["create"]["_id"]

        # check if _id is not in error_ids
        if _id not in error_ids:
            data_doc["_id"] = _id
            result.append(data_doc)

    return result


async def ingest_bulk(
    el: AsyncElasticsearch,
    index: str,
    docs: list[GulpDocument | dict],
    flt: GulpIngestionFilter = None,
    wait_for_refresh: bool = False,
) -> tuple[int, int, list[str], list[dict]]:
    """
    ingests a list of GulpDocument into Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to ingest documents to.
        doc (GulpDocument|dict): The documents to be ingested.
        flt (GulpIngestionFilter, optional): The filter parameters. Defaults to None.
        wait_for_refresh (bool, optional): Whether to wait for the refresh(=refreshed index is available for searching) to complete. Defaults to False.

    Returns:
        tuple:
        - number of skipped (because already existing=duplicated) events
        - number of failed events
        - failed (errors) array: here, failed means "failed ingesting": this means the event format is wrong for ingestion into elasticsearch and should be fixed
        - list of ingested documents
        - list of filtered documents (in case flt.store_all_documents is True), either None
    """

    # filter documents first
    # len_first = len(docs)
    bulk_docs = _build_bulk_docs(docs, flt)
    # logger().error('ingesting %d documents (was %d before filtering)' % (len(docs) / 2, len_first))
    if len(bulk_docs) == 0:
        logger().warning("no document to ingest (flt=%s)" % (flt))
        return 0, 0, [], []

    # logger().info("ingesting %d docs: %s\n" % (len(bulk_docs) / 2, json.dumps(bulk_docs, indent=2)))

    timeout = config.ingestion_request_timeout()

    # opensearch version
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
    failed_ar = []
    ingested: list[dict] = []

    # NOTE: bulk_docs/2 is because the bulk_docs is a list of tuples (create, doc)

    if res["errors"]:
        # logger().error(
        #     "to ingest=%d but errors happened, res=%s"
        #     % (len(docs) / 2, json.dumps(res["items"], indent=2))
        # )
        # print(json.dumps(res["items"], indent=2))

        for r in res["items"]:
            op_result = r["create"]
            error = op_result.get("error", None)
            status = op_result["status"]
            if status == 409:
                # document already exists !
                skipped += 1
                # logger().error(r)
            elif status not in [201, 200]:
                failed += 1
                logger().error(
                    "failed to ingest document: %s, docs=%s"
                    % (r, muty.string.make_shorter(str(bulk_docs), max_len=1000))
                )
                msg = "elastic.bulk.errors.items.create.status=%s" % (op_result)
                if error is not None:
                    msg += ", error=%s" % (error)
                if msg not in failed_ar:
                    # add only once
                    failed_ar.append(msg)

        # take only the documents with no ingest errors
        ingested = _bulk_docs_result_to_ingest_chunk(bulk_docs, res["items"])
    else:
        # no errors, take all documents
        ingested = _bulk_docs_result_to_ingest_chunk(bulk_docs)

    if skipped != 0:
        logger().error(
            "**NOT AN ERROR** total %d skipped, %d failed in this bulk ingestion of %d documents !"
            % (skipped, failed, len(bulk_docs) / 2)
        )
    if failed > 0:
        logger().critical("failed is set, ingestion format needs to be fixed!")
    return skipped, failed, failed_ar, ingested


async def rebase(
    el: AsyncElasticsearch,
    index: str,
    dest_index: str,
    msec_offset: int,
    flt: GulpQueryFilter = None,
) -> dict:
    logger().debug(
        "rebase index %s to %s with offset=%d, flt=%s ..."
        % (index, dest_index, msec_offset, flt)
    )
    q = gulpqueryflt_to_dsl(flt)
    convert_script = """
        if ( ctx._source["@timestamp"] != 0 ) {
            ctx._source["@timestamp"] = ctx._source["@timestamp"] + params.msec_offset;
            long nsec_offset = (long)params.msec_offset * params.multiplier;
            long nsec = ctx._source["@timestamp_nsec"] + nsec_offset;
            ctx._source["@timestamp_nsec"] = (long)nsec;
        }
    """

    body: dict = {
        "source": {"index": index, "query": q["query"]},
        "dest": {"index": dest_index, "op_type": "create"},
        "script": {
            "lang": "painless",
            "source": convert_script,
            "params": {
                "msec_offset": msec_offset,
                "multiplier": muty.time.MILLISECONDS_TO_NANOSECONDS,
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
    options: GulpQueryOptions = None,
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
    q = {"query": {"term": {"operation_id": operation_id}}}

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

    q = gulpqueryflt_to_dsl(flt)
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


async def refresh_index(el: AsyncElasticsearch, index: str) -> None:
    """
    Refresh an index(=make changes available to search) in Elasticsearch.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to refresh.

    Returns:
        None
    """
    logger().debug("refreshing index: %s" % (index))
    await el.indices.refresh(index=index)


async def query_time_histograms(
    el: AsyncElasticsearch,
    index: str,
    interval: str,
    flt: GulpQueryFilter = None,
    options: GulpQueryOptions = None,
    group_by: str = None,
    include_hits: bool = False,
) -> dict:
    """
    Queries Elasticsearch for time histograms based on a given time interval.

    Args:
        el (AsyncElasticsearch): The Elasticsearch client.
        index (str): Name of the index (or datastream) to query.
        interval (str): The interval for the date histogram, i.e 1s.
        flt (GulpQueryFilter, optional): The query filter. Defaults to None.
        options (GulpQueryOptions, optional): The query options. Defaults to None.
        group_by (str, optional): The field to subaggregate by. Defaults to None.
        include_hits: bool, optional: Whether to include hits in the response. Defaults to False.
    Returns:
        dict: The parsed Elasticsearch response.

    """
    q = gulpqueryflt_to_dsl(flt)
    opts = _build_query_options(options)

    # clear options
    aggregations = {
        "histograms": {
            "date_histogram": {
                "field": "@timestamp",
                "fixed_interval": interval,
                "min_doc_count": 1,
                "keyed": False,
            }
        }
    }
    if group_by is not None:
        aggregations["histograms"]["aggs"] = {"by_type": {"terms": {"field": group_by}}}

    body = {
        "track_total_hits": True,
        "query": q["query"],
        "aggregations": aggregations,
    }
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
    # print(json.dumps(body, indent=2))
    res = await el.search(body=body, index=index, headers=headers)
    return _parse_elastic_res(res, include_hits=include_hits, options=options)


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
            "terms": {"field": "operation_id", "size": max_buckets},
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
                                        "field": "gulp.source.file",
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
    el: AsyncElasticsearch, index: str, q: dict, options: GulpQueryOptions = None
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
    opts = _build_query_options(options)
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


def get_client() -> AsyncElasticsearch:
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
    certs_dir = config.certs_directory()
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
        _elastic = get_client()
    return _elastic
