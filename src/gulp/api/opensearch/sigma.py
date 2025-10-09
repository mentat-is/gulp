"""
Utilities for handling Sigma rules in Gulp API.

This module provides functions to convert Sigma rules to GulpQuery objects and to extract tags
from Sigma rules. The module integrates with the Sigma framework for threat detection and
serves as a bridge between Sigma rules and Gulp's search capabilities.
"""

from typing import TYPE_CHECKING

import aiofiles.os
import muty.file
import muty.string
import orjson
import yaml
from muty.log import MutyLogger
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.conversion.base import Backend
from sigma.rule import SigmaRule
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.mapping.models import GulpMapping, GulpMappingFile, GulpSigmaMapping
from gulp.api.ws_api import GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpMappingParameters
from gulp.api.opensearch.structs import GulpQuery


async def _read_sigma_mappings_from_file(
    mapping_file: str,
) -> dict[str, GulpSigmaMapping]:
    """
    read sigma mappings from a mapping file.

    Args:
        mapping_file (str): the mapping file to read
    Returns:
        dict[str, GulpSigmaMapping]: the sigma mappings, or None if not found
    """
    mapping_file_path = GulpConfig.get_instance().build_mapping_file_path(mapping_file)
    file_content = await muty.file.read_file_async(mapping_file_path)
    mapping_data = orjson.loads(file_content)
    mapping_file_obj = GulpMappingFile.model_validate(mapping_data)
    sigma_mappings = mapping_file_obj.sigma_mappings
    if not sigma_mappings:
        MutyLogger.get_instance().warning(
            "mapping file %s has no sigma mappings!", mapping_file
        )
        return None
    return sigma_mappings


async def get_sigma_mappings(
    m: GulpMappingParameters,
) -> dict[str, GulpSigmaMapping]:
    """
    get sigma_mappings from mapping parameters, handling loading from file if needed.

    Args:
        m (GulpMappingParameters): the mapping parameters

    Returns:
        dict[str, GulpSigmaMapping]: the sigma mappings, or None if not found (both in mapping parameters and mapping file, if any)
    """
    if m.sigma_mappings:
        # get it from mapping parameters
        return m.sigma_mappings

    sigma_mappings: dict = {}
    if m.mapping_file:
        # get it from mapping file
        sigma_mappings = await _read_sigma_mappings_from_file(m.mapping_file)

    if m.additional_mapping_files:
        # for each additional mapping file, get the sigma mappings and merge them with sigma_mappings
        for additional_mapping_file, _ in m.additional_mapping_files:
            additional_sigma_mappings = await _read_sigma_mappings_from_file(
                additional_mapping_file
            )

            if additional_sigma_mappings:
                # merge the sigma mappings
                if not sigma_mappings:
                    sigma_mappings = {}
                sigma_mappings.update(additional_sigma_mappings)

    if not sigma_mappings:
        # no sigma mappings found
        MutyLogger.get_instance().warning(
            "no sigma mappings found in mapping parameters or mapping file: %s",
            m.mapping_file,
        )
        return None
    return sigma_mappings


def _use_this_sigma(
    yml: str,
    levels: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
    tags: list[str] = None,
) -> SigmaRule:
    """
    check if the sigma rule is to be used, depending on the filters set (levels, products, categories, services, tags).

    Args:
        yml (str): the sigma rule YAML (must contain a single rule)
        levels (list[str], optional): set of levels to check. Defaults to None.
        products (list[str], optional): set of products to check. Defaults to None.
        categories (list[str], optional): set of categories to check. Defaults to None.
        services (list[str], optional): set of services to check. Defaults to None.
        tags (list[str], optional): set of tags to check. Defaults to None.

    Returns:
        SigmaRule: the parsed SigmaRule if it should be used, None otherwise
    """

    # parse yaml
    r: SigmaRule = SigmaRule.from_yaml(yml)

    # check filters
    if levels:
        if r.level and str(r.level.name).lower() not in levels:
            return None
    if products:
        if (
            r.logsource
            and r.logsource.product
            and r.logsource.product.lower() not in products
        ):
            return None
    if categories:
        if (
            r.logsource
            and r.logsource.category
            and r.logsource.category.lower() not in categories
        ):
            return None
    if services:
        if (
            r.logsource
            and r.logsource.service
            and r.logsource.service.lower() not in services
        ):
            return None
    if tags:
        tag_found: bool = False
        if r.tags:
            tag_names = {t.name.lower() for t in r.tags if t.name}
            if tag_names & tags:
                tag_found = True
        if not tag_found:
            return None

    # return sigma
    return r


def _sigma_rule_to_gulp_query(
    r: SigmaRule, sigma_yml: str, q: dict, tags: list[str] = None
) -> GulpQuery:
    """
    convert a Sigma rule to a GulpQuery object.

    NOTE: levels (severity_), logsource.service(service_), logsource.product (product_), and logsource.category (category_) are set in GulpQuery.tags together with sigma's own tags (if any).
    Args:
        r (SigmaRule): the sigma rule object
        q (dict): the converted query
        tags (list[str], optional): the (additional) tags to set on the query
    Returns:
        GulpQuery: represents the converted sigma rule
    """
    rule_id: str = str(r.id) or muty.string.generate_unique()
    rule_name: str = r.name or r.title or "sigma_%s" % (rule_id)
    rule_tags: list[str] = [t.name for t in (r.tags or []) if t]
    if r.level:
        # add severity tag
        rule_tags.append(f"severity_{r.level.name.lower()}")
    if r.logsource:
        if r.logsource.product:
            # add product tag
            rule_tags.append(f"product_{r.logsource.product.lower()}")
        if r.logsource.service:
            # add service tag
            rule_tags.append(f"service_{r.logsource.service.lower()}")
        if r.logsource.category:
            # add category tag
            rule_tags.append(f"category_{r.logsource.category.lower()}")

    if tags:
        # additional tags
        for t in tags:
            if t not in rule_tags:
                rule_tags.append(t)

    qq: GulpQuery = GulpQuery(
        name=rule_name,
        sigma_yml=sigma_yml,
        sigma_id=rule_id,
        tags=rule_tags,
        q=q,
    )
    MutyLogger.get_instance().debug(
        "converted sigma rule: name=%s, id=%s, q=%s"
        % (rule_name, rule_id, muty.string.make_shorter(str(qq), 260))
    )
    return qq


def _map_sigma_fields_to_ecs(sigma_yaml: str, mapping: GulpMapping) -> str:
    """
    Replace field names in a sigma rule YAML with their ECS mappings from a GulpMapping.

    This function takes a sigma rule in YAML format and transforms field names based on
    the ECS mappings defined in the provided GulpMapping object. For each field in the
    sigma rule's detection section, if a mapping exists, the field is replaced with its
    corresponding ECS field(s).

    Args:
        sigma_yaml (str): The sigma rule in YAML format
        mapping (GulpMapping): The mapping containing field-to-ECS transformations

    Returns:
        str: The modified sigma rule YAML with fields replaced according to ECS mappings

    Throws:
        yaml.YAMLError: If the input YAML is not valid
    """

    def _map_field(field: str) -> list[str]:
        """
        map a field name to its ecs equivalent, preserving any modifiers.

        Args:
            field (str): original field name possibly containing modifiers

        Returns:
            list[str]: list of mapped field names with preserved modifiers
        """
        # split the field name and any modifiers (e.g., "field|modifier")
        if "|" in field:
            field_name, modifier = field.split("|", 1)
            modifier_suffix = f"|{modifier}"
        else:
            field_name = field
            modifier_suffix = ""

        gulp_document_default_fields: list[str] = [
            "_id",
            "@timestamp",
            "gulp.timestamp",
            "gulp.timestamp_invalid",
            "gulp.operation_id",
            "gulp.context_id",
            "gulp.source_id",
            "log.file.path",
            "agent.type",
            "event.original",
            "event.sequence",
            "event.code",
            "gulp.event_code",
            "event.duration",
        ]

        # check if the base field name has a mapping
        if field_name in field_mappings:
            # map to one or more ecs fields
            return [
                f"{ecs_field}{modifier_suffix}"
                for ecs_field in field_mappings[field_name]
            ]
        elif field_name in gulp_document_default_fields:
            # if the field is one of the default fields, return it as is with modifier
            return [f"{field_name}{modifier_suffix}"]
        else:
            # return "gulp.unmapped" field
            return [f"{GulpPluginBase.build_unmapped_key(field_name)}{modifier_suffix}"]

    def _patch_re_selections(detection: dict) -> dict:
        """
        patch "|re" selections to be compatible with elasticsearch/opensearch.

        TODO: submit a patch to pysigma elasticsearch/opensearch backends to handle this automatically.

        Args:
            detection (dict): the detection section of the sigma rule
        """

        def _process_selection_value(d: dict, k: str, v: str):
            flags: str = None
            if v.startswith("(?"):
                # regex has global flags, split regex and flags
                flag_end: int = v.find(")")
                flags = v[2:flag_end]
                v = v[flag_end + 1 :]

            if v.startswith("^"):
                # elastic doesn't like the ^ at the beginning
                v = v[1:]
            else:
                v = ".*" + v
            if v.endswith("$"):
                # elastic doesn't like the $ at the end
                v = v[:-1]
            else:
                v = v + ".*"

            # update the value
            if flags:
                # add flags back
                v = f"(?{flags}){v}"
            d[k] = v

        for _, value in detection.items():
            if isinstance(value, dict):
                # selection is a dict, i.e.
                #
                # 'detection': {
                #    'selection': {
                #        'Contents|re': 'http[s]?://[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}',
                #        ...
                #    },
                for k, v in value.items():
                    if k.endswith("|re"):
                        # we're interested to patch regex only ...
                        _process_selection_value(value, k, v)

            elif isinstance(value, list):
                # selection is a list of dicts, i.e.
                #
                # 'detection': {
                #     'selection_img': [{
                #         'Image|endswith': ['\\powershell.exe', '\\pwsh.exe']
                #     }, {
                #         'OriginalFileName': ['PowerShell.EXE', 'pwsh.dll']
                #     }],
                #     'selection_re': [{
                #         'CommandLine|re': '\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+.*\\+'
                #     }, ...
                for item in value:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            if k.endswith("|re"):
                                # we're interested to patch regex only ...
                                _process_selection_value(item, k, v)

        return detection

    def _process_field_conditions(conditions: dict) -> dict:
        """
        process a dictionary of field conditions, replacing field names with ecs mappings.

        Args:
            conditions (dict): dictionary of field conditions

        Returns:
            dict: updated dictionary with mapped field names
        """
        new_conditions = {}
        for field, value in conditions.items():
            mapped_fields = _map_field(field)
            for mapped_field in mapped_fields:
                new_conditions[mapped_field] = value
        return new_conditions

    # parse the sigma yaml
    try:
        sigma_dict = yaml.safe_load(sigma_yaml)
    except yaml.YAMLError as e:
        MutyLogger.get_instance().error("failed to parse sigma yaml: %s", e)
        raise

    # create field mapping dictionary from source yml fields to mapped fields
    field_mappings = {}
    for source_field, mapping_field in mapping.fields.items():
        if mapping_field.ecs:
            # handle both string and list formats for ecs field mapping
            if isinstance(mapping_field.ecs, str):
                field_mappings[source_field] = [mapping_field.ecs]
            else:  # it's a list
                field_mappings[source_field] = mapping_field.ecs

    # process detection section where field names are used
    if "detection" not in sigma_dict:
        MutyLogger.get_instance().warning(
            "sigma rule has no detection section:\n%s", sigma_yaml
        )
        return sigma_yaml

    # fix detection part for elasticsearch/opensearch
    detection = sigma_dict["detection"]
    detection = _patch_re_selections(detection)

    # process each key in the detection section
    for key, value in list(detection.items()):
        if key == "condition":
            # skip the condition, it refers to section names, not field names
            continue

        if isinstance(value, dict):
            # direct field-to-value mapping
            detection[key] = _process_field_conditions(value)
        elif isinstance(value, list):
            # list of conditions (could be dicts or strings)
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    # replace field names in this dict
                    value[i] = _process_field_conditions(item)
                # leave strings unchanged

    # convert back to yaml
    s = yaml.dump(sigma_dict, sort_keys=False)
    # MutyLogger.get_instance().debug("mapped sigma:\n%s", s)
    return s


async def sigma_to_tags(sigma: str) -> list[str]:
    """
    get tags from a sigma rule.

    Args:
        sess (AsyncSession): the database session
        path (str): path to the sigma yml file
        mapping_parameters_cache (dict[str, GulpMappingParameters]): source_id->mapping_parameters cache
        src_ids (list[str], optional): list of source ids to use. Defaults to None (use all sources in cache)
        levels (list[str], optional): list of levels to filter by. Defaults to None (use all levels)
        tags (list[str], optional): list of tags to filter by. Defaults to None (use all tags)
        products (list[str], optional): list of products to filter by. Defaults to None (use all products)
        categories (list[str], optional): list of categories to filter by. Defaults to None (use all categories)
        services (list[str], optional): list of services to filter by. Defaults to None (use all services)

    Returns:
        list[str]: the tags extracted from the sigma rule
    """
    # just load the sigma and extract the tags
    r: SigmaRule = SigmaRule.from_yaml(sigma)
    q: GulpQuery = _sigma_rule_to_gulp_query(r, sigma, q=None)
    MutyLogger.get_instance().debug("extracted tags from sigma rule:\n%s", q.tags)
    return q.tags


async def sigmas_to_queries(
    sess: AsyncSession,
    user_id: str,
    operation_id: str,
    sigmas: list[str],
    src_ids: list[str] = None,
    levels: list[str] = None,
    products: list[str] = None,
    services: list[str] = None,
    categories: list[str] = None,
    tags: list[str] = None,
    paths: bool = False,
    req_id: str = None,
    ws_id: str = None,
) -> list["GulpQuery"]:
    """
    convert a list of sigma rules to GulpQuery objects.

    for each sigma rule, we check if it should be used, depending on the filters set (levels, products, categories, services, tags).
    if the rule should be used, we convert it to a GulpQuery object using the mapping parameters of the source.

    if no sources are provided, we get all available sources

    Args:
        sess (AsyncSession): the database session
        user_id (str): the user id
        operation_id (str): the operation id
        sigmas (list[str]): the list of sigma rules in YAML format (or paths to files if paths=True)
        src_ids (list[str], optional): list of source ids to use. Defaults to None (get all sources).
        levels (list[str], optional): list of levels to check. Defaults to None.
        products (list[str], optional): list of products to check. Defaults to None.
        services (list[str], optional): list of services to check. Defaults to None.
        categories (list[str], optional): list of categories to check. Defaults to None.
        tags (list[str], optional): list of tags to check. Defaults to None.
        paths (bool, optional): if True, sigmas are paths to files (will be read). Defaults to False.
        req_id (str, optional): the request id. Defaults to None.
        ws_id (str, optional): the websocket id to stream progress updates. Defaults to None.
    Returns:
        list[GulpQuery]: the list of GulpQuery objects
    """

    backend: Backend = OpensearchLuceneBackend()
    output_format: str = "dsl_lucene"

    srcs: list[GulpSource] = []
    if not src_ids:
        # get all source ids, if not provided
        flt: GulpCollabFilter = GulpCollabFilter(operation_ids=[operation_id])
        srcs = await GulpSource.get_by_filter(sess, flt=flt, user_id=user_id)
        MutyLogger.get_instance().warning(
            "using all sources for user %s: %s", user_id, src_ids
        )
    else:
        # get the given sources
        srcs = await GulpSource.get_by_ids(sess, src_ids)
        found_ids = {s.id for s in srcs if s}
        for src_id in src_ids:
            if src_id not in found_ids:
                MutyLogger.get_instance().warning(
                    "source %s not found for user %s, skipping", src_id, user_id
                )
    src_ids = [s.id for s in srcs]
    MutyLogger.get_instance().debug(f"src_ids= {src_ids}")
    # precompute source_id->mapping_parameters map
    mp_by_source_id: list = []
    for src in srcs:
        mp_by_source_id.append(
            GulpMappingParameters.model_validate(src.mapping_parameters)
            if src.mapping_parameters
            else GulpMappingParameters()
        )

    # get unique mapping parameters sets
    my_mapping = set(mp_by_source_id)

    # get content for every rules passed, reading from files if they're paths
    yamls: list[str] = []
    if paths:
        # batch read all files
        for sigma in sigmas:
            is_file = await aiofiles.os.path.isfile(sigma)
            if is_file and (
                sigma.lower().endswith(".yml") or sigma.lower().endswith(".yaml")
            ):
                # read from file
                b = await muty.file.read_file_async(sigma)
                yamls.append(b.decode())
    else:
        # take yamls as is
        yamls = sigmas

    gulp_queries: list[GulpQuery] = []

    count: int = 0
    total: int = len(yamls)

    for yml in yamls:
        count += 1

        # check if the rule has to be used
        rule: SigmaRule = _use_this_sigma(
            yml,
            levels=levels,
            products=products,
            categories=categories,
            services=services,
            tags=tags,
        )
        if not rule:
            continue

        should_part: list[dict] = []
        for mapping_parameters in my_mapping:
            # loop for every unique mapping parameters set
            mappings: dict[str, GulpMapping]
            mapping_id: str
            mappings, mapping_id = await GulpPluginBase.mapping_parameters_to_mapping(
                mapping_parameters
            )

            # check if we need to process this mapping for this rule
            use_sigma_mapping: bool = True
            if mapping_parameters.sigma_mappings:
                if (
                    not rule.logsource
                    or not rule.logsource.service
                    or (rule.logsource.service not in mapping_parameters.sigma_mappings)
                ):
                    use_sigma_mapping = False

            # transform the sigma rule considering source mappings: i.e. if the sigma rule have EventId in the conditions, we want it
            # to be "event.code" in ECS, so we need to apply the mapping first
            final_sigma_yml: str = _map_sigma_fields_to_ecs(yml, mappings[mapping_id])
            # MutyLogger.get_instance().debug(
            #     "sigma_convert_default, final sigma rule yml:\n%s", final_sigma_yml
            # )
            try:
                qs: list[dict] = backend.convert_rule(
                    SigmaRule.from_yaml(final_sigma_yml), output_format=output_format
                )
            except Exception as ex:
                MutyLogger.get_instance().exception(
                    "failed to convert sigma %s ***ORIGINAL***:\n\n%s\n***PATCHED***:%s"
                    % (rule.title or rule.name, yml, final_sigma_yml)
                )
                raise ex
            for q in qs:
                q_should_query_part: list[dict] = []

                if use_sigma_mapping:
                    # add the logsource specific part
                    sigma_mapping_to_use: GulpSigmaMapping = (
                        mapping_parameters.sigma_mappings[rule.logsource.service]
                    )

                    new_query_string: str = ""
                    for v in sigma_mapping_to_use.service_values:
                        # escape special characters
                        v = v.replace(" ", "\\ ")
                        v = v.replace("/", "\\/")
                        new_query_string += (
                            f"{sigma_mapping_to_use.service_field}: *{v}* OR "
                        )
                    new_query_string = new_query_string[:-4]

                    # build the new query using a further query_string
                    new_q: dict = {
                        # this is the new part of the query, to handle sigma mappings (restrict to specific service values)
                        "query_string": {
                            "query": new_query_string,
                        }
                    }
                    # store back the patched query
                    q_should_query_part.append(new_q)
            should_dict = q["query"]  # existing query (from pysigma conversion)

            if q_should_query_part:
                should_dict["bool"]["should"] = q_should_query_part

            should_part.append(should_dict)

        # mapping loop done, build the final query
        if len(should_part) == 1:
            mapping_should_query: dict = {"query": should_part[0]}
        elif len(should_part) > 1:
            mapping_should_query: dict = {"query": {"bool": {"should": should_part}}}

        # build the final GulpQuery
        final_query: GulpQuery = _sigma_rule_to_gulp_query(
            rule, yml, mapping_should_query, tags=tags
        )
        gulp_queries.append(final_query)
        MutyLogger.get_instance().info(
            '******* converted sigma rule "%s": current=%d, total=%d *******'
            % (rule.name or rule.title, len(gulp_queries), total)
        )

        # for query in gulp_queries:
        #     print(query)

        if count % 100 == 0 and req_id:
            # check if the request is cancelled
            canceled = await GulpRequestStats.is_canceled(sess, req_id)
            if canceled:
                raise Exception("request canceled")

            if ws_id:
                # HANDLE THIS
                # send progress packet to the websocket
                # p = GwlpProgressPacket(
                #     total=total,
                #     current=count,
                #     generated_q=len(gulp_queries),
                #     msg="sigma_conversion",
                # )
                # wsq = GulpWsSharedQueue.get_instance()
                # await wsq.put(
                #     t="TOREMOVEprogress"
                #     ws_id=ws_id,
                #     user_id=user_id,
                #     req_id=req_id,
                #     d=p.model_dump(exclude_none=True),
                # )
                pass

    return gulp_queries
