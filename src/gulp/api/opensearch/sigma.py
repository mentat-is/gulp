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
from muty.log import MutyLogger
from sigma.collection import SigmaCollection
from sigma.conversion.base import Backend
from sigma.rule import SigmaRule

from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters

if TYPE_CHECKING:
    from gulp.api.opensearch.query import GulpQuery


async def sigma_to_tags(
    plugin: str, sigma: str, plugin_params: GulpPluginParameters = None
) -> list[str]:
    """
    get tags from a sigma rule.

    Args:
        plugin (str): the plugin to use
        sigma (str): the sigma rule YAML
        plugin_params (GulpPluginParameters, optional): the plugin parameters. Defaults to None.

    Returns:
        list[str]: the tags extracted from the sigma rule
    """
    mod: GulpPluginBase = None
    tags: list[str] = []
    try:
        mod = await GulpPluginBase.load(plugin)
        q: list[GulpQuery] = mod.sigma_convert(sigma, plugin_params)
        for qq in q:
            if qq.tags:
                tags.extend(qq.tags)
    finally:
        if mod:
            mod.unload()
    MutyLogger.get_instance().debug("extracted tags from sigma rule:\n%s", tags)
    return tags


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
            rule_tags: list[str] = [t.name for t in r.tags if t]
            if r.level:
                # add severity tag
                rule_tags.append(f"severity-{r.level.name.lower()}")
            if tags:
                # additional tags
                for t in tags:
                    if t not in rule_tags:
                        rule_tags.append(t)

            converted = GulpQuery(
                name=rule_name,
                sigma_id=rule_id,
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
