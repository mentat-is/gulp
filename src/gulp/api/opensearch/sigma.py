"""
sigma rules tools
"""

from typing import TYPE_CHECKING
from sigma.collection import SigmaCollection
from sigma.rule import (
    SigmaRule,
)
from sigma.conversion.base import Backend

import muty.string

from muty.log import MutyLogger


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
            rule_tags: list[str] = [t.name for t in r.tags if t]
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
    MutyLogger.get_instance().debug(
        "converted %d sigma rules to GulpQuery:\n%s",
        len(converted_sigmas),
        converted_sigmas,
    )
    return converted_sigmas
