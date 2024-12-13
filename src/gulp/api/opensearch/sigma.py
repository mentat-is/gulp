"""
sigma rules tools
"""

import json
from muty.log import MutyLogger
from sigma.collection import SigmaCollection
from sigma.rule import (
    SigmaRule,
    SigmaDetection,
    SigmaDetections,
)
from sigma.conversion.base import Backend
import yaml

import muty.string

from gulp.api.opensearch.query import GulpQuery, GulpQueryAdditionalParameters


def to_gulp_query_struct(
    sigma: str, backend: Backend, output_format: str = None, tags: list[str] = None
) -> list[GulpQuery]:
    """
    convert a Sigma rule to a GulpConvertedSigma object.

    Args:
        sigma (str): the sigma rule YAML
        backend (Backend): the backend to use
        output_format (str, optional): the output format to use. Defaults to None.
        tags (list[str], optional): the (additional) tags to set on the query

    Returns:
        list[GulpConvertedSigma]: one or more queries in the format specified by backend/pipeline/output_format.
    """
    converted_sigmas: list[GulpQuery] = []
    sc: list[SigmaRule] = SigmaCollection.from_yaml(sigma)
    for r in sc:
        # a single sigma may originate multiple queries
        q = backend.convert_rule(r, output_format=output_format)
        for qq in q:
            # generate a GulpConvertedSigma for each
            rule_id = str(r.id) or muty.string.generate_unique()
            rule_name = r.name or r.title or "sigma_%s" % (rule_id)
            rule_tags = r.tags or []
            if tags:
                # additional tags
                [rule_tags.append(t) for t in tags if t not in rule_tags]

            converted = GulpQuery(
                name=rule_name,
                sigma_id=rule_id,
                tags=rule_tags,
                q=qq,
            )
            converted_sigmas.append(converted)
    return converted_sigmas


def _rule_to_yaml(rule: SigmaRule) -> str:
    """
    convert a SigmaRule to YAML string representation.

    Args:
        rule (SigmaRule): The rule to convert to YAML

    Returns:
        str: YAML representation of the rule
    """

    # Convert rule to dict
    rule_dict = {
        "title": rule.title,
        "id": str(rule.id),
        "name": rule.name,
        "status": str(rule.status) if rule.status else None,
        "description": rule.description,
        "logsource": {
            "category": rule.logsource.category,
            "product": rule.logsource.product,
            "service": rule.logsource.service,
        },
        "detection": {
            name: {
                item.field: [str(v) for v in item.value]
                for item in detection.detection_items
            }
            for name, detection in rule.detection.detections.items()
        },
        "condition": rule.detection.condition,
        "tags": [str(tag) for tag in rule.tags] if rule.tags else None,
        "level": str(rule.level) if rule.level else None,
    }

    # remove None values
    rule_dict = {k: v for k, v in rule_dict.items() if v is not None}

    # convert to YAML with proper formatting
    return yaml.dump(
        rule_dict, sort_keys=False, allow_unicode=True, default_flow_style=False
    )


def _merge_rules(
    rules: list[str], name: str = None, tags: list[str] = None
) -> SigmaRule:
    """
    merge multiple Sigma rules from a list.

    Args:
        rules(list[str]): list of sigma rule YAMLs to be merged
        name(str, optional): only for multiple rules, name to set on the merged rule
        tags(list[str], optional): only for multiple rules, tags to set on the merged rule

    Returns:
        SigmaRule: A merged rule combining all input rules
    """
    if not rules:
        raise ValueError("No rules provided")

    if len(rules) == 1:
        return SigmaCollection.from_yaml(rules[0]).rules[0]

    # load all rules
    rule_objects: list[SigmaRule] = []
    for rule_path in rules:
        collection = SigmaCollection.from_yaml(rule_path)
        for rule in collection.rules:
            rule_objects.append(rule)

    # merge detections
    merged_detections = {}
    for rule in rule_objects:
        for detection_name, detection in rule.detection.detections.items():
            if detection_name in merged_detections:
                # get existing detection items
                existing_items = merged_detections[detection_name].detection_items

                # get new detection items, filtering duplicates
                new_items = [
                    item
                    for item in detection.detection_items
                    if not any(
                        existing_item.field == item.field
                        and existing_item.value == item.value
                        and existing_item.modifiers == item.modifiers
                        for existing_item in existing_items
                    )
                ]

                # only add non-duplicate items
                if new_items:
                    merged_items = existing_items + new_items
                    merged_detections[detection_name] = SigmaDetection(
                        detection_items=merged_items,
                        source=detection.source,
                        item_linking=detection.item_linking,
                    )
            else:
                merged_detections[detection_name] = detection

    if not tags:
        # merge tags from all rules, if not provided
        tags = list(
            {str(tag) for rule in rule_objects if rule.tags for tag in rule.tags}
        )

    # create merged conditions
    conditions = []
    for rule in rule_objects:
        for condition in rule.detection.condition:
            if condition not in conditions:
                conditions.append(condition)

    merged_condition = [" or ".join(f"({condition})" for condition in conditions)]

    # create merged detection object
    merged_detection = SigmaDetections(
        detections=merged_detections, condition=merged_condition
    )

    # create merged rule
    rule_id = muty.string.generate_unique()
    rule_name = name or f"merged_{rule_id}"
    merged_rule = SigmaRule(
        title=rule_name,
        id=rule_id,
        name=rule_name,
        tags=tags,
        logsource=rule_objects[0].logsource,
        detection=merged_detection,
    )
    MutyLogger.get_instance().debug(
        "merged rule YAML:\n%s" % (_rule_to_yaml(merged_rule))
    )
    return merged_rule


def merge_and_convert(
    sigmas: list[str],
    backend: Backend,
    name: str = None,
    tags: list[str] = None,
    output_format: str = None,
) -> list[GulpQuery]:
    """
    merge multiple Sigma rules from a list and convert them to the specified backend and output format.

    Args:
        sigmas (str): the main sigma rule YAML
        backend (Backend): the backend to use
        name (str): the name to set on the query (only used if sigmas contains multiple rules)
        tags (list[str]): the tags to set on the query (only used if sigmas contains multiple rules)
        referenced_sigmas (list[str], optional): a list of referenced sigma rules YAMLs. Defaults to None.
        output_format (str): the output format to use
    Returns:
        list[GulpConvertedSigma]: one or more queries in the format specified by backend/pipeline/output_format.
    """
    # build sigma including references
    merged_rule: SigmaRule = _merge_rules(sigmas, name, tags)
    MutyLogger.get_instance().debug("merged rule:\n%s" % (merged_rule))

    q = backend.convert_rule(merged_rule, output_format=output_format)
    MutyLogger.get_instance().debug("converted query:\n%s" % (json.dumps(q)))

    # a single sigma may originate multiple queries
    l = []
    for qq in q:
        converted = GulpQuery(
            name=merged_rule.name or "rule_" + str(merged_rule.id),
            q=qq,
            tags=[str(t) for t in merged_rule.tags] if merged_rule.tags else [],
            sigma_id=str(merged_rule.id),
        )
        l.append(converted)
    return l
