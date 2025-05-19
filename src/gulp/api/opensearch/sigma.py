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

from typing import TYPE_CHECKING

import muty.string
import yaml
from muty.log import MutyLogger
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.collection import SigmaCollection
from sigma.conversion.base import Backend
from sigma.rule import SigmaRule

from gulp.api.mapping.models import GulpMapping
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpMappingParameters, GulpPluginParameters

if TYPE_CHECKING:
    from gulp.api.opensearch.query import GulpQuery


def to_gulp_query_struct(
    sigma: str, backend: Backend, output_format: str = None, tags: list[str] = None
) -> list["GulpQuery"]:
    """
    convert a Sigma rule to a GulpQuery object.

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
        "converted %d sigma rules to GulpQuery:\n%s",
        len(converted_sigmas),
        converted_sigmas,
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

        # check if the base field name has a mapping
        if field_name in field_mappings:
            # map to one or more ecs fields
            return [
                f"{ecs_field}{modifier_suffix}"
                for ecs_field in field_mappings[field_name]
            ]
        else:
            # keep original field if no mapping exists
            return [field]

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
        MutyLogger.get_instance().warning("sigma rule has no detection section")
        return sigma_yaml

    detection = sigma_dict["detection"]

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
    MutyLogger.get_instance().debug("mapped sigma:\n%s", s)
    return s


async def sigma_convert_default(
    sigma: str,
    mapping_parameters: GulpMappingParameters = None,
    **kwargs,
) -> list["GulpQuery"]:
    """
    convert a sigma rule to one or more GulpQuery objects targeting gulp's opensearch backend.

    Args:
        sigma (str): the sigma rule YAML
        mapping_parameters (GulpMappingParameters, optional): the mapping parameters to use for conversion. if not set, the default (empty) mapping will be used.
        **kwargs: additional parameters to pass to the conversion function
            backend (Backend): the backend to use for conversion. Defaults to OpensearchLuceneBackend.
            output_format (str): the output format to use. Defaults to "dsl_lucene".
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

    # convert the transformed sigma rule to gulp queries
    backend = kwargs.get("backend", OpensearchLuceneBackend())
    output_format = kwargs.get("output_format", "dsl_lucene")
    return to_gulp_query_struct(
        mapped_sigma, backend=backend, output_format=output_format
    )


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
