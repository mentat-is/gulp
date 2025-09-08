"""
Utilities for handling Sigma rules in Gulp API.

This module provides functions to convert Sigma rules to GulpQuery objects and to extract tags
from Sigma rules. The module integrates with the Sigma framework for threat detection and
serves as a bridge between Sigma rules and Gulp's search capabilities.

The module includes:
- `sigma_to_tags`: Function to extract tags from a Sigma rule
- `to_gulp_query_struct`: Function to convert a Sigma rule to a GulpQuery object

These utilities facilitate the use of Sigma rules for security monitoring and threat detection
using the Gulp framework.
"""

import asyncio
import os
from typing import TYPE_CHECKING

import aiofiles.os
import muty.file
import muty.string
import orjson
import yaml
from muty.log import MutyLogger
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.collection import SigmaCollection
from sigma.conversion.base import Backend
from sigma.rule import SigmaRule
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.mapping.models import GulpMapping, GulpMappingFile, GulpSigmaMapping
from gulp.api.rest_api import GulpRestServer
from gulp.api.ws_api import WSDATA_PROGRESS, GulpProgressPacket, GulpWsSharedQueue
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpMappingParameters

if TYPE_CHECKING:
    from gulp.api.opensearch.query import GulpQuery


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


def _apply_mappings_to_sigma_detections(sigma_yaml: str, mapping: GulpMapping) -> str:
    """
    Replace detection field names in a sigma rule YAML with their ECS mappings from a GulpMapping.

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
            d[k] = v

        for field, value in detection.items():
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
        plugin (str): the plugin to use
        sigma (str): the sigma rule YAML
        plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.

    Returns:
        list[str]: the tags extracted from the sigma rule
    """
    # just load the sigma and extract the tags
    q: list[GulpQuery] = _to_gulp_query(
        sigma,
    )
    tags: list[str] = []
    for qq in q:
        if qq.tags:
            tags.extend(qq.tags)
    MutyLogger.get_instance().debug("extracted tags from sigma rule:\n%s", tags)
    return tags


async def build_source_mapping_cache(
    sess: AsyncSession,
    operation_id: str,
    user_id: str,
    cache: dict[str, tuple[GulpMappingParameters, dict[str, GulpSigmaMapping]]] = None,
    src_ids: list[str] = None,
) -> dict[str, GulpMappingParameters]:
    """
    build a cache of mapping parameters and sigma mappings for the provided sources.

    if no source ids are provided, all sources for the operation/user are used.

    Args:
        sess (AsyncSession): the database session
        operation_id (str): the operation id
        user_id (str): the user id
        cache (dict[str, GulpMappingParameters]): the cache to add elements on. if None, a new empty cache is created.
        src_ids (list[str], optional): list of source ids to use. Defaults to None
    Returns:
        dict[str, GulpMappingParameters]: the (updated) dictionary mapping source ids to their mapping parameters
    """
    srcs: list[GulpSource] = []
    if src_ids:
        # get just the provided sources
        srcs = await GulpSource.get_by_ids_ordered(src_ids)
    else:
        # get all sources for the operation/user
        MutyLogger.get_instance().warning(
            "no source provided, using all sources for operation %s and user %s"
            % (operation_id, user_id)
        )
        srcs = await GulpSource.get_by_filter(
            sess,
            collab_filter=GulpCollabFilter(operation_ids=[operation_id]),
            user_id=user_id,
        )

    if cache:
        # use provided cache
        source_cache = cache
    else:
        # empty
        source_cache: dict[str, GulpMappingParameters] = {}

    for src in srcs:
        if not src:
            continue
        if src.id in source_cache:
            # already in cache
            # MutyLogger.get_instance().warning("source %s already in cache, skipping" % src.id)
            continue

        mp = src.mapping_parameters or {}
        mapping_parameters = GulpMappingParameters.model_validate(mp)
        if mapping_parameters.is_empty():
            mapping_parameters = None
        else:
            sigma_mappings = await get_sigma_mappings(mapping_parameters)
            if not sigma_mappings:
                mapping_parameters.sigma_mappings = {}
            else:
                mapping_parameters.sigma_mappings = sigma_mappings

        # set mappings
        source_cache[src.id] = mapping_parameters

    MutyLogger.get_instance().debug(
        "source/mapping cache size: %d sources" % (len(source_cache))
    )
    return source_cache


def _use_this(
    yml: bytes | str,
    levels: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
    tags: list[str] = None,
) -> SigmaRule:
    """
    check wether to use this sigma rule based on the provided filters.

    NOTE: an yml may contain multiple sigma rules (that's why we return a list).

    Args:

        yml (bytes|str): the sigma rule YAML
        levels (list[str], optional): list of levels to check. Defaults to None.
        products (list[str], optional): list of products to check. Defaults to None.
        categories (list[str], optional): list of categories to check. Defaults to None.
        services (list[str], optional): list of services to check. Defaults to None.
        tags (list[str], optional): list of tags to check. Defaults to None.
    Returns:
        SigmaRule: the sigma rule if it should be used, None otherwise
    """

    r = SigmaRule.from_yaml(yml)
    # optimization: use set membership which is O(1) instead of list membership which is O(n)
    if levels:
        if r.level and str(r.level.name).lower() not in levels:
            MutyLogger.get_instance().warning(
                'rule "%s" did not pass the filter (levels)' % (r.title)
            )
            return None
    if products:
        if (
            r.logsource
            and r.logsource.product
            and r.logsource.product.lower() not in products
        ):
            MutyLogger.get_instance().warning(
                'rule "%s" did not pass the filter (products)' % (r.title)
            )
            return None
    if categories:
        if (
            r.logsource
            and r.logsource.category
            and r.logsource.category.lower() not in categories
        ):
            MutyLogger.get_instance().warning(
                'rule "%s" did not pass the filter (categories)' % (r.title)
            )
            return None
    if services:
        if (
            r.logsource
            and r.logsource.service
            and r.logsource.service.lower() not in services
        ):
            MutyLogger.get_instance().warning(
                'rule "%s" did not pass the filter (services)' % (r.title)
            )
            return None
    if tags:
        tag_found: bool = False
        if r.tags:
            tag_names = {t.name.lower() for t in r.tags if t.name}
            if tag_names & tags:
                tag_found = True
        if not tag_found:
            MutyLogger.get_instance().warning(
                'rule "%s" did not pass the filter (tags)' % (r.title)
            )
            return None

    # rule passed
    MutyLogger.get_instance().debug('rule "%s" passed the filters' % (r.title))

    return r


def _to_gulp_query(
    yml: str | bytes,
    backend: Backend = None,
    output_format: str = None,
    tags: list[str] = None,
    q_groups: list[str] = None,
    return_dicts: bool = False,
) -> list["GulpQuery" | dict]:
    """
    convert a Sigma rule to one or more GulpQuery objects.

    Args:
        r (SigmaRule): the sigma rule
        backend (Backend, optional): the backend to use. Defaults to OpensearchLuceneBackend.
        output_format (str, optional): the output format to use. Defaults to "dsl_lucene".
        tags (list[str], optional): the (additional) tags to set on the query
        groups (list[str], optional): the "query group"/s to set on the query
        return_dicts (bool, optional): whether to return a list of dicts instead of GulpQuery objects. Defaults to False.
    Returns:
        list[GulpQuery|dict]: one or more queries in the format specified by backend/pipeline/output
    """
    from gulp.api.opensearch.query import GulpQuery

    if not backend:
        backend = OpensearchLuceneBackend()
    if not output_format:
        output_format = "dsl_lucene"

    r = SigmaRule.from_yaml(yml)

    # a single sigma may originate multiple queries
    converted_sigmas: list[GulpQuery | dict] = []
    q = backend.convert_rule(r, output_format=output_format)
    for qq in q:
        # generate a GulpQuery for each
        rule_id = str(r.id) or muty.string.generate_unique()
        rule_name = r.name or r.title or "sigma_%s" % (rule_id)
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

        # create gulpquery
        sigma_str = yml if isinstance(yml, str) else yml.decode(errors="ignore")
        converted = GulpQuery(
            name=rule_name,
            sigma_id=rule_id,
            sigma_yml=sigma_str,
            tags=rule_tags,
            q=qq,
            groups=q_groups or [],
        )
        if return_dicts:
            converted_sigmas.append(converted.model_dump(exclude_none=True))
        else:
            converted_sigmas.append(converted)

        MutyLogger.get_instance().debug(
            "converted sigma rule: name=%s, id=%s, q=%s"
            % (rule_name, rule_id, muty.string.make_shorter(str(qq), 260))
        )
    return converted_sigmas


async def _convert_internal(
    r: SigmaRule, yml: str | bytes, mapping_parameters: GulpMappingParameters = None, return_dicts: bool=True
) -> list[dict]:
    """
    internal function to convert a sigma rule to one or more GulpQuery dicts, applying
    mappings and sigma mappings as needed.

    Args:
        r (SigmaRule): the sigma rule
        yml (str|bytes): the sigma rule YAML
        mapping_parameters (GulpMappingParameters, optional): the mapping parameters to use for conversion.
        return_dicts (bool, optional): whether to return a list of dicts instead of GulpQuery objects. Defaults to True.
    Returns:
        list[dict|GulpQuery]: one or more GulpQuery/dict
    """
    # get mappings
    mappings, mapping_id = await GulpPluginBase.mapping_parameters_to_mapping(
        mapping_parameters or GulpMappingParameters()
    )

    # transform the sigma rule using ECS field mappings
    mapped_yml = _apply_mappings_to_sigma_detections(yml, mappings[mapping_id])
    # MutyLogger.get_instance().debug(
    #     "sigma_convert_default, mapped sigma rule:\n%s", mapped_sigma
    # )
    list_queries: list[dict] = _to_gulp_query(mapped_yml, return_dicts=return_dicts)
    if not mapping_parameters or not mapping_parameters.sigma_mappings:
        # no sigma mappings, just return the queries
        MutyLogger.get_instance().warning(
            "sigma_convert_default, no sigma mappings, returning %d queries",
            len(list_queries),
        )
        return list_queries

    # walk the generated queries and add constraints if sigma mappings are set
    for q in list_queries:
        if not r.logsource or not r.logsource.service:
            # use q as is
            continue
        svc = r.logsource.service.lower()
        if not svc in mapping_parameters.sigma_mappings:
            # use q as is
            continue

        # get the mapping and build the new query adding a constraints (service must match)
        relevant_mapping: GulpSigmaMapping = mapping_parameters.sigma_mappings[svc]
        new_query_string: str = ""
        for v in relevant_mapping.service_values:
            # escape special characters
            v = v.replace(" ", "\\ ")
            v = v.replace("/", "\\/")
            new_query_string += f"{relevant_mapping.service_field}: *{v}* OR "
        new_query_string = new_query_string[:-4]

        # build the new query using a further query_string
        new_q: dict = {
            "query": {
                "bool": {
                    "must": [
                        q["q"]["query"],
                        {
                            # this is the new part of the query, to handle sigma mappings (restrict to specific service values)
                            "query_string": {
                                "query": new_query_string,
                            }
                        },
                    ]
                }
            }
        }
        q["q"] = new_q

    # done!
    # count: int=0
    # for q in list_queries:
    #     MutyLogger.get_instance().debug(
    #         "q[%d]:\n%s" % (count, orjson.dumps(q["q"], option=orjson.OPT_INDENT_2).decode())
    #     )
    #     count += 1
    return list_queries


async def sigma_yml_to_queries(
    sess: AsyncSession,
    yml: str | bytes,
    mapping_parameters_cache: dict[str, GulpMappingParameters],
    src_ids: list[str] = None,
    levels: list[str] = None,
    tags: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
    return_dicts: bool = True,
) -> list[dict|GulpQuery]:
    if not mapping_parameters_cache:
        raise ValueError("sigma_yml_to_queries: source mapping cache is empty")

    queries: list[dict] = []
    r = _use_this(
        yml,
        levels=levels,
        products=products,
        categories=categories,
        services=services,
        tags=tags,
    )
    if not r:
        # filters not passed
        return []

    # for every rule, check the sources to which it must be applied
    conversion_cache: dict[str, list[dict]] = {}
    for src_id in mapping_parameters_cache:
        # check if this rule is to be used for this source (based on sigma mappings, if any)
        mapping_parameters = mapping_parameters_cache[src_id]
        if mapping_parameters and mapping_parameters.sigma_mappings:
            if r.logsource and r.logsource.service:
                # check if this sigma rule is for this src_id, based on its defined sigma mappings
                if r.logsource.service.lower() not in mapping_parameters.sigma_mappings:
                    # not for this mapping
                    MutyLogger.get_instance().warning(
                        'rule "%s" not for source %s (service %s not in sigma mappings)'
                        % (r.title, src_id, r.logsource.service.lower())
                    )
                    continue

        # convert base rule with pysigma:
        # use a cache to speedup conversion, avoiding converting the same rule multiple times
        # for the same source
        if r.id in conversion_cache:
            # already converted
            converted = conversion_cache[r.id]
        else:
            # convert and add to cache
            converted = await _convert_internal(r, yml, mapping_parameters, return_dicts=return_dicts)
            converted[r.id] = converted

        for c in converted:
            # add source_id constraint to the base query (GulpQuery.q)
            base_query: dict = c["q"] if return_dicts else c.q
            targeted_q: dict = {
                "query": {
                    "bool": {
                        "must": [
                            base_query["query"],
                            {
                                "query_string": {
                                    "query": "gulp.source_id: %s" % src_id,
                                }
                            },
                        ]
                    }
                }
            }
            queries.append(targeted_q)

    # done
    return queries


async def sigma_file_to_queries(
    sess: AsyncSession,
    path: str,
    mapping_parameters_cache: dict[str, GulpMappingParameters],
    src_ids: list[str] = None,
    levels: list[str] = None,
    tags: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
) -> list[dict]:
    """
    convert a sigma yml file to one or more GulpQuery dictionaries.

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
        list[dict]: one or more GulpQUery dictionaries
    """
    is_file = await aiofiles.os.path.isfile(path)
    if not is_file or not (
        path.lower().endswith(".yml") or path.lower().endswith(".yaml")
    ):
        raise ValueError(
            "sigma_file_to_queries: path is not a valid sigma yml file: %s" % path
        )

    yml: bytes = await muty.file.read_file_async(path)
    MutyLogger.get_instance().info(
        "read sigma yml file %s (%d bytes)" % (path, len(yml))
    )
    return await sigma_yml_to_queries(
        sess,
        yml,
        mapping_parameters_cache,
        src_ids=src_ids,
        levels=levels,
        tags=tags,
        products=products,
        categories=categories,
        services=services,
    )


async def sigma_file_list_to_queries(
    sess: AsyncSession,
    paths: str,
    mapping_parameters_cache: dict[str, GulpMappingParameters],
    src_ids: list[str] = None,
    levels: list[str] = None,
    tags: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
) -> list[dict]:
    """
    convert a list of sigma yml files to one or more GulpQuery dictionaries.

    Args:
        sess (AsyncSession): the database session
        paths (list[str]): list of paths to sigma yml files
        mapping_parameters_cache (dict[str, GulpMappingParameters]): source_id->mapping_parameters cache
        src_ids (list[str], optional): list of source ids to use. Defaults to None (use all sources in cache)
        levels (list[str], optional): list of levels to filter by. Defaults to None (use all levels)
        tags (list[str], optional): list of tags to filter by. Defaults to None (use all tags)
        products (list[str], optional): list of products to filter by. Defaults to None (use all products)
        categories (list[str], optional): list of categories to filter by. Defaults to None (use all categories)
        services (list[str], optional): list of services to filter by. Defaults to None (use all services)
    Returns:
        list[dict]: one or more GulpQUery dictionaries
    """
    queries: list[GulpQuery] = []
    yield_count: int = 0
    for path in paths:
        qs = await sigma_file_to_queries(
            sess,
            path,
            mapping_parameters_cache,
            src_ids=src_ids,
            levels=levels,
            tags=tags,
            products=products,
            categories=categories,
            services=services,
        )
        queries.extend(qs)
        yield_count += 1
        if yield_count == 10:
            # yield to the event loop every 10 files converted
            await asyncio.sleep(0)
            yield_count = 0
    return queries


async def sigma_yml_list_to_queries(
    sess: AsyncSession,
    ymls: list[str | bytes],
    mapping_parameters_cache: dict[str, GulpMappingParameters],
    src_ids: list[str] = None,
    levels: list[str] = None,
    tags: list[str] = None,
    products: list[str] = None,
    categories: list[str] = None,
    services: list[str] = None,
) -> list[dict]:
    """
    convert a list of sigma yml strings to one or more GulpQuery dictionaries.

    Args:
        sess (AsyncSession): the database session
        ymls (list[str|bytes]): list of sigma yml strings
        mapping_parameters_cache (dict[str, GulpMappingParameters]): source_id->mapping_parameters cache
        src_ids (list[str], optional): list of source ids to use. Defaults to None (use all sources in cache)
        levels (list[str], optional): list of levels to filter by. Defaults to None (use all levels)
        tags (list[str], optional): list of tags to filter by. Defaults to None (use all tags)
        products (list[str], optional): list of products to filter by. Defaults to None (use all products)
        categories (list[str], optional): list of categories to filter by. Defaults to None (use all categories)
        services (list[str], optional): list of services to filter by. Defaults to None (use all services)
    Returns:
        list[dict]: one or more GulpQUery dictionaries
    """
    l: list[GulpQuery] = []
    for yml in ymls:
        qs = await sigma_yml_to_queries(
            sess,
            yml,
            mapping_parameters_cache,
            src_ids=src_ids,
            levels=levels,
            tags=tags,
            products=products,
            categories=categories,
            services=services,
        )
        l.extend(qs)
    return []
