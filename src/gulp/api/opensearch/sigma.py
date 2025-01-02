"""
sigma rules tools
"""

from typing import TYPE_CHECKING
from pydantic import BaseModel, ConfigDict, Field
from sigma.collection import SigmaCollection
from sigma.rule import (
    SigmaRule,
)
from sigma.conversion.base import Backend

import muty.string

from muty.log import MutyLogger

from gulp.structs import GulpNameDescriptionEntry

if TYPE_CHECKING:
    from gulp.api.opensearch.query import GulpQuery


class GulpQuerySigmaParameters(BaseModel):
    """
    represents options for a sigma query, to customize automatic note creation or to customize
    the conversion using specific backend/pipeline/output format.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "pipeline": None,
                    "backend": None,
                    "output_format": None,
                }
            ]
        }
    )
    plugin: str = Field(
        None,
        description="""
the plugin to be used to convert the sigma rule.

- must implement `backend`, `pipeline`, `output_format` if they are set (either, it is assumed that the plugin implements at least one backend, pipeline, output_format).
""",
    )
    pipeline: str = Field(
        None,
        description="the pipeline to use when converting the sigma rule, defaults to None (use plugin's default)",
    )
    backend: str = Field(
        None,
        description="this is only used for `external` queries: the backend to use when converting the sigma rule, defaults to None (use plugin's default)",
    )
    output_format: str = Field(
        None,
        description="this is only used for `external` queries: the output format to use when converting the sigma rule, defaults to None (use plugin's default)",
    )


class GulpPluginSigmaSupport(BaseModel):
    """
    indicates the sigma support for a plugin, to be returned by the plugin.sigma_support() method.

    refer to [sigma-cli](https://github.com/SigmaHQ/sigma-cli) for parameters (backend=-t, pipeline=-p, output=-f).
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "backends": [
                        {
                            "name": "opensearch",
                            "description": "OpenSearch.",
                        }
                    ],
                    "pipelines": [
                        {
                            "name": "ecs_windows",
                            "description": "ECS Mapping for windows event logs ingested with Winlogbeat or Gulp.",
                        }
                    ],
                    "output_formats": [
                        {
                            "name": "dsl_lucene",
                            "description": "DSL with embedded Lucene queries.",
                        }
                    ],
                }
            ]
        }
    )
    backends: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more pysigma backend supported by the plugin: `opensearch` is the one to use to query Gulp.",
    )
    pipelines: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more pysigma pipelines supported by the plugin.",
    )
    output_formats: list[GulpNameDescriptionEntry] = Field(
        ...,
        description="one or more output formats supported by the plugin.",
    )


def to_gulp_query_struct(
    sigma: str, backend: Backend, output_format: str = None, tags: list[str] = None
) -> list["GulpQuery"]:
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
    from gulp.api.opensearch.query import GulpQuery

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
    MutyLogger.get_instance().debug(
        "converted %d sigma rules to GulpQuery:\n%s",
        len(converted_sigmas),
        converted_sigmas,
    )
    return converted_sigmas
