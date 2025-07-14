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
import orjson
import os
from typing import TYPE_CHECKING

import muty.file
import muty.string
import yaml
from muty.log import MutyLogger
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.collection import SigmaCollection
from sigma.conversion.base import Backend
from sigma.rule import SigmaRule
from sqlalchemy.ext.asyncio import AsyncSession
import aiofiles.os

from gulp.api.collab.source import GulpSource
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.mapping.models import GulpMapping, GulpMappingFile, GulpSigmaMapping
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
    get the sigma mappings from the mapping parameters.

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


async def sigmas_to_queries(
    sess: AsyncSession,
    user_id: str,
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
    if no sources are provided, we get all sources for the user.

    Args:
        sess (AsyncSession): the database session
        user_id (str): the user id
        sigmas (list[str]): the list of sigma rules in YAML format
        src_ids (list[str], optional): list of source ids to use. Defaults to None (get all sources).
        levels (list[str], optional): list of levels to check. Defaults to None.
        products (list[str], optional): list of products to check. Defaults to None.
        services (list[str], optional): list of services to check. Defaults to None.
        categories (list[str], optional): list of categories to check. Defaults to None.
        tags (list[str], optional): list of tags to check. Defaults to None.
        paths (bool, optional): if True, sigmas are paths to files (will be read). Defaults to False.
        req_id (str, optional): the request id. Defaults to None.
        ws_id (str, optional): the websocket id. Defaults to None.
    Returns:
        list[GulpQuery]: the list of GulpQuery objects
    """
    # optimization: precompute filter lists for faster comparison
    levels_set = set(levels) if levels else None
    products_set = set(products) if products else None
    categories_set = set(categories) if categories else None
    services_set = set(services) if services else None
    tags_set = set(tags) if tags else None

    # optimization: batch database operations
    srcs: list[GulpSource] = []
    if not src_ids:
        # get all source ids, if not provided
        srcs = await GulpSource.get_by_filter(sess, user_id=user_id)
        MutyLogger.get_instance().warning(
            "using all sources for user %s: %s", user_id, src_ids
        )
    else:
        # optimization: batch get sources instead of individual queries
        srcs = await GulpSource.get_by_ids(sess, src_ids)
        # filter out None sources
        found_ids = {s.id for s in srcs if s}
        for src_id in src_ids:
            if src_id not in found_ids:
                MutyLogger.get_instance().warning(
                    "source %s not found for user %s, skipping", src_id, user_id
                )

    # get all source ids and preload a dict with mapping parameters
    src_ids = [s.id for s in srcs]
    mp_by_source_id: dict = {}
    for src in srcs:
        mp_by_source_id[src.id] = src.mapping_parameters

    # optimization: preload sigma mappings cache for all sources
    mapping_params_cache: dict[str, GulpMappingParameters] = {}
    sigma_mappings_cache: dict[str, dict[str, GulpSigmaMapping]] = {}

    # optimization: group sources by their mapping parameters to reduce redundant conversions
    mapping_groups: dict[str, list[str]] = {}  # mapping_key -> list of src_ids

    for src_id in src_ids:
        mp = mp_by_source_id[src_id] or {}
        mapping_parameters = GulpMappingParameters.model_validate(mp)
        mapping_params_cache[src_id] = mapping_parameters

        # preload sigma mappings for this source
        sigma_mappings = await get_sigma_mappings(mapping_parameters)
        sigma_mappings_cache[src_id] = sigma_mappings

        # create a unique key for this mapping configuration
        mapping_key = _create_mapping_key(mapping_parameters)
        if mapping_key not in mapping_groups:
            mapping_groups[mapping_key] = []
        mapping_groups[mapping_key].append(src_id)

    # optimization: batch file reading for path mode
    rule_contents: list[str] = []
    if paths:
        # batch read all files
        for sigma in sigmas:
            is_file = await aiofiles.os.path.isfile(sigma)
            if is_file and (
                sigma.lower().endswith(".yml") or sigma.lower().endswith(".yaml")
            ):
                rule_content: bytes = await muty.file.read_file_async(sigma)
                rule_contents.append(rule_content.decode())
            else:
                rule_contents.append(None)  # placeholder for invalid files
    else:
        rule_contents = sigmas

    # convert sigma rule/s using pysigma
    queries: list[GulpQuery] = []
    count: int = 0
    used: int = 0
    generated_q: int = 0
    total: int = len(sigmas)

    # optimization: cache base conversions per mapping configuration
    base_conversion_cache: dict[str, dict[str, list["GulpQuery"]]] = (
        {}
    )  # mapping_key -> rule_content -> base_queries

    for idx, rule_content in enumerate(rule_contents):
        if count % 100 == 0 and req_id:
            # check if the request is cancelled
            canceled = await GulpRequestStats.is_canceled(sess, req_id)
            if canceled:
                raise Exception("request canceled")

            if ws_id:
                # send progress packet to the websocket (this may be a lenghty operation)
                p = GulpProgressPacket(
                    total=total,
                    current=count,
                    used=used,
                    generated_q=generated_q,
                    msg="converting sigma rules...",
                )
                wsq = GulpWsSharedQueue.get_instance()
                await wsq.put(
                    type=WSDATA_PROGRESS,
                    ws_id=ws_id,
                    user_id=user_id,
                    req_id=req_id,
                    data=p.model_dump(exclude_none=True),
                )

            print("processed %d sigma rules, total=%d, used=%d" % (count, total, used))

        count += 1

        # skip invalid files in path mode
        if paths and rule_content is None:
            continue

        # optimization: pass precomputed sets to use_this_sigma
        use, sigma_service_names = use_this_sigma(
            rule_content,
            levels_set=levels_set,
            products_set=products_set,
            categories_set=categories_set,
            services_set=services_set,
            tags_set=tags_set,
        )

        if not use:
            continue

        used += 1

        # optimization: process each unique mapping configuration only once
        for mapping_key, source_ids in mapping_groups.items():
            # get a representative source for this mapping group
            representative_src_id = source_ids[0]
            mapping_parameters = mapping_params_cache[representative_src_id]
            mapping_parameters.sigma_mappings = sigma_mappings_cache[
                representative_src_id
            ]

            # check if we need to process this mapping for this rule
            if mapping_parameters.sigma_mappings and sigma_service_names:
                # check if this sigma rule is for this mapping group
                service_match_found: bool = False
                for service_name in sigma_service_names:
                    if service_name in mapping_parameters.sigma_mappings:
                        service_match_found = True
                        break
                if not service_match_found:
                    # this sigma rule is not for this mapping group
                    continue

            # check cache for base conversion
            if mapping_key not in base_conversion_cache:
                base_conversion_cache[mapping_key] = {}

            if rule_content not in base_conversion_cache[mapping_key]:
                # perform base conversion only once per mapping configuration
                use_sigma_mappings = True
                if not sigma_service_names:
                    # no service names found in the rule, we don't need to use sigma mappings
                    use_sigma_mappings = False

                try:
                    base_queries: list[GulpQuery] = await sigma_convert_default(
                        rule_content,
                        mapping_parameters,
                        use_sigma_mappings=use_sigma_mappings,
                        sigma_service_names=sigma_service_names,
                    )
                    base_conversion_cache[mapping_key][rule_content] = base_queries
                except Exception as ex:
                    # error converting sigma
                    MutyLogger.get_instance().error(
                        "%s, rule:\n%s"
                        % (
                            str(ex),
                            muty.string.make_shorter(rule_content, max_len=100),
                        ),
                    )
                    base_conversion_cache[mapping_key][rule_content] = []
                    continue

            # get cached base queries
            base_queries = base_conversion_cache[mapping_key][rule_content]

            # optimization: apply source-specific modifications to cached queries
            for src_id in source_ids:
                for base_q in base_queries:
                    # create a deep copy of the base query
                    import copy

                    qq = copy.deepcopy(base_q)
                    q_dict: dict = qq.q

                    # tie this query to the source id
                    new_q: dict = {
                        "query": {
                            "bool": {
                                "must": [
                                    q_dict["query"],
                                    {
                                        "query_string": {
                                            "query": "gulp.source_id: %s" % src_id,
                                        }
                                    },
                                ]
                            }
                        }
                    }
                    qq.q = new_q
                    queries.append(qq)
                    generated_q += 1

    # log the number of queries generated
    print(
        "******* converted sigma rules (total=%d, used=%d) to %d GulpQuery objects *******"
        % (total, used, len(queries))
    )

    if ws_id and req_id:
        # send progress packet to the websocket (this may be a lenghty operation)
        p = GulpProgressPacket(
            total=total,
            current=total,
            used=used,
            generated_q=generated_q,
            done=True,
            msg="sigma rules conversion done!",
        )
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            type=WSDATA_PROGRESS,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            data=p.model_dump(exclude_none=True),
        )

    if not queries:
        # no queries generated
        raise ValueError(
            "no queries generated from the provided sigma rules, check log/input!"
        )

    # DEBUG PRINTS
    dbg_count: int = 0
    for q in queries:
        # dump each query
        MutyLogger.get_instance().debug(
            "query[%d]: %s" % (dbg_count, orjson.dumps(q.q, option=orjson.OPT_INDENT_2).decode())
        )
        dbg_count += 1

    return queries


def _create_mapping_key(mapping_parameters: GulpMappingParameters) -> str:
    """
    create a unique key for mapping parameters to group sources with identical configurations.

    Args:
        mapping_parameters (GulpMappingParameters): the mapping parameters

    Returns:
        str: unique key representing the mapping configuration
    """
    import hashlib

    # create a deterministic representation of the mapping parameters
    key_components = []

    if mapping_parameters.mapping_file:
        key_components.append(f"mapping_file:{mapping_parameters.mapping_file}")

    if mapping_parameters.additional_mapping_files:
        sorted_files = sorted(
            [f[0] for f in mapping_parameters.additional_mapping_files]
        )
        key_components.append(f"additional_files:{','.join(sorted_files)}")

    if mapping_parameters.sigma_mappings:
        # create a deterministic representation of sigma mappings
        sigma_keys = sorted(mapping_parameters.sigma_mappings.keys())
        sigma_repr = []
        for key in sigma_keys:
            mapping = mapping_parameters.sigma_mappings[key]
            sigma_repr.append(
                f"{key}:{mapping.service_field}:{','.join(sorted(mapping.service_values or []))}"
            )
        key_components.append(f"sigma_mappings:{';'.join(sigma_repr)}")

    # create hash of the combined components
    key_string = "|".join(key_components)
    return hashlib.md5(key_string.encode()).hexdigest()


def use_this_sigma(
    yml: str,
    levels_set: set[str] = None,
    products_set: set[str] = None,
    categories_set: set[str] = None,
    services_set: set[str] = None,
    tags_set: set[str] = None,
) -> tuple[bool, list[str]]:
    """
    optimized version of use_this_sigma that uses sets for faster lookups

    Args:
        yml (str): the sigma rule YAML
        levels_set (set[str], optional): set of levels to check. Defaults to None.
        products_set (set[str], optional): set of products to check. Defaults to None.
        categories_set (set[str], optional): set of categories to check. Defaults to None.
        services_set (set[str], optional): set of services to check. Defaults to None.
        tags_set (set[str], optional): set of tags to check. Defaults to None.

    Returns:
        tuple[bool, list[str]]: True if the rule should be used, False otherwise. Also returns a list of service names found in the rule (logsource.service) if the sigma must be used.
    """
    sc = SigmaCollection.from_yaml(yml)
    l: int = len(sc)
    passed: int = 0
    sigma_service_names: list[str] = []

    for r in sc:
        r: SigmaRule

        # optimization: use set membership which is O(1) instead of list membership which is O(n)
        if levels_set:
            if r.level and str(r.level.name).lower() not in levels_set:
                continue
        if products_set:
            if (
                r.logsource
                and r.logsource.product
                and r.logsource.product.lower() not in products_set
            ):
                continue
        if categories_set:
            if (
                r.logsource
                and r.logsource.category
                and r.logsource.category.lower() not in categories_set
            ):
                continue
        if services_set:
            if (
                r.logsource
                and r.logsource.service
                and r.logsource.service.lower() not in services_set
            ):
                continue
        if tags_set:
            # optimization: check if any tag is in tags_set using set intersection
            tag_found: bool = False
            if r.tags:
                tag_names = {t.name.lower() for t in r.tags if t.name}
                if tag_names & tags_set:  # set intersection
                    tag_found = True
            if not tag_found:
                continue

        # passed
        passed += 1
        if r.logsource.service:
            # add service name to the list
            sigma_service_names.append(r.logsource.service.lower())

    if passed == l:
        # all rules passed
        return True, sigma_service_names

    # do not use this sigma
    return False, []


def to_gulp_query_struct(
    sigma: str, backend: Backend, output_format: str = None, tags: list[str] = None
) -> list["GulpQuery"]:
    """
    convert a Sigma rule to a GulpQuery object.

    NOTE: levels (severity_), logsource.service(service_), logsource.product (product_), and logsource.category (category_) are set in GulpQuery.tags together with sigma tags (if any).

    Args:
        sigma (str): the sigma rule YAML
        backend (Backend): the backend to use
        output_format (str, optional): the output format to use. Defaults to None (use backend's default)
        tags (list[str], optional): the (additional) tags to set on the query

    Returns:
        list[GulpConvertedSigma]: one or more queries in the format specified by backend/pipeline/output_format.
    """
    from gulp.api.opensearch.query import GulpQuery

    converted_sigmas: list[GulpQuery] = []
    sc: list[SigmaRule] = SigmaCollection.from_yaml(sigma)
    for r in sc:
        # a single sigma may originate multiple queries
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

            converted = GulpQuery(
                name=rule_name,
                sigma_id=rule_id,
                sigma_yml=sigma,
                tags=rule_tags,
                q=qq,
            )
            converted_sigmas.append(converted)
    MutyLogger.get_instance().debug(
        "converted %d sigma rules to GulpQuery",
        len(converted_sigmas),
    )
    return converted_sigmas


def map_sigma_fields_to_ecs(sigma_yaml: str, mapping: GulpMapping) -> str:
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

    # create field mapping dictionary from source fields to ecs fields
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


async def sigma_convert_default(
    sigma: str,
    mapping_parameters: GulpMappingParameters = None,
    use_sigma_mappings: bool = True,
    sigma_service_names: list[str] = None,
    **kwargs,
) -> list["GulpQuery"]:
    """
    convert a sigma rule to one or more GulpQuery objects targeting gulp's opensearch/elasticsearch backend.

    Args:
        sigma (str): the sigma rule YAML
        mapping_parameters (GulpMappingParameters, optional): the mapping parameters to use for conversion. if not set, the default (empty) mapping will be used.
        use_sigma_mappings (bool, optional): whether to process (if present) sigma mappings to build the query. Defaults to True.
        sigma_service_names (list[str], optional): list of service names from the sigma rule. Defaults to None.
        **kwargs: additional parameters to pass to the conversion function
            backend (Backend): the backend to use for conversion. Defaults to OpensearchLuceneBackend.
    Returns:
        list[GulpQuery]: one or more queries in the format specified by backend/pipeline/output_format.
    """
    if not mapping_parameters:
        # MutyLogger.get_instance().warning("mapping parameters are empty, using default mapping")
        # use default mapping
        mapping_parameters = GulpMappingParameters()
    mappings, mapping_id = await GulpPluginBase.mapping_parameters_to_mapping(
        mapping_parameters
    )

    # transform the sigma rule using ECS field mappings
    mapped_sigma = map_sigma_fields_to_ecs(sigma, mappings[mapping_id])
    MutyLogger.get_instance().debug(
        "sigma_convert_default, mapped sigma rule:\n%s", mapped_sigma
    )

    # convert the transformed sigma rule to gulp queries
    backend = kwargs.get("backend", OpensearchLuceneBackend())
    output_format = "dsl_lucene"
    qs: list[GulpQuery] = to_gulp_query_struct(
        mapped_sigma, backend=backend, output_format=output_format
    )
    if not use_sigma_mappings:
        # no sigma mappings, just return the queries
        MutyLogger.get_instance().warning(
            "sigma_convert_default, no sigma mappings, returning %d queries",
            len(qs),
        )
        # count: int =0
        # for q in qs:
        #     MutyLogger.get_instance().debug(
        #         "sigma_convert_default, q[%d]:\n%s" % (count, orjson.dumps(q.q, option=orjson.OPT_INDENT_2).decode())
        #     )
        #     count += 1
        return qs

    # finally we add constraints if sigma mappings are set (and we're told to use them)
    for q in qs:
        if not mapping_parameters.sigma_mappings:
            continue

        # find the relevant sigma mapping based on service name
        relevant_mapping: GulpSigmaMapping = None
        if sigma_service_names:
            for service_name in sigma_service_names:
                if service_name in mapping_parameters.sigma_mappings:
                    relevant_mapping = mapping_parameters.sigma_mappings[service_name]
                    break

        if not relevant_mapping or not relevant_mapping.service_values:
            continue

        # add service values to the query
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
                        q.q["query"],
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

        # store back the patched query
        q.q = new_q

    # done!
    # count: int=0
    # for q in qs:
    #     MutyLogger.get_instance().debug(
    #         "sigma_convert_default, q[%d]:\n%s" % (count, orjson.dumps(q.q, option=orjson.OPT_INDENT_2).decode())
    #     )
    #     count += 1
    return qs


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
    q: list[GulpQuery] = to_gulp_query_struct(
        sigma,
        backend=OpensearchLuceneBackend(),
        output_format="dsl_lucene",
    )
    tags: list[str] = []
    for qq in q:
        if qq.tags:
            tags.extend(qq.tags)
    MutyLogger.get_instance().debug("extracted tags from sigma rule:\n%s", tags)
    return tags
