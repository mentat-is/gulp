from typing import Any, override
import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from elasticsearch import AsyncElasticsearch
from muty.log import MutyLogger
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.query import (
    GulpQuery,
    GulpQueryParameters,
    GulpQueryHelpers,
    GulpQuerySigmaParameters,
)
from gulp.api.opensearch.sigma import to_gulp_query_struct
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import (
    GulpDocument,
)
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpNameDescriptionEntry,
    GulpPluginCustomParameter,
    GulpPluginParameters,
    GulpPluginSigmaSupport,
)

muty.os.check_and_install_package("pysigma-backend-elasticsearch", ">=1.1.5, <2")
muty.os.check_and_install_package("elasticsearch", ">=8.1.5, <9")

from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.pipelines.elasticsearch.windows import ecs_windows, ecs_windows_old
from sigma.backends.elasticsearch.elasticsearch_lucene import LuceneBackend
from sigma.pipelines.elasticsearch.zeek import (
    ecs_zeek_beats,
    ecs_zeek_corelight,
    zeek_raw,
)


class Plugin(GulpPluginBase):
    """
    query plugin for opensearch/elasticsearch.

    TODO: outdated. must be reworked, based on the splunk plugin

    example flt and plugin_params:

     {
        "flt": { "start_msec": 1475730263242, "end_msec": 1475830263242},
        "plugin_params": {
            "extra": {
                "url": "localhost:9200",
                "username": "admin",
                "password": "Gulp1234!",
                "index": "testidx",
                "timestamp_is_string": false,
                "timestamp_unit": "ms",
                "is_elasticsearch": false
            }
        }
    }
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTERNAL

    @override
    def desc(self) -> str:
        return "Query data from elasticsearch."

    def display_name(self) -> str:
        return "query_elasticsearch"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="offset_msec",
                type="int",
                desc="""
                    offset in milliseconds to be added to documents timestamp.

                    - to subtract, use a negative offset.
                    """,
                default_value=0,
            ),
            GulpPluginCustomParameter(
                name="timestamp_field",
                type="str",
                desc="the main timestamp field in the documents.",
                default_value="@timestamp",
            ),
            GulpPluginCustomParameter(
                name="is_elasticsearch",
                type="bool",
                desc="if True, connect to elasticsearch, otherwise connect to opensearch.",
                default_value=True,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, data: Any = None
    ) -> GulpDocument:
        # record is a dict
        doc: dict = record

        offset_msec: int = self._custom_params.get("offset_msec", 0)
        timestamp_field: str = self._custom_params.get("timestamp_field", "@timestamp")

        # convert timestamp to nanoseconds
        mapping = self.selected_mapping()
        _, ts_nsec, _ = GulpDocument.ensure_timestamp(
            doc[timestamp_field],
            dayfirst=mapping.timestamp_dayfirst if mapping else None,
            yearfirst=mapping.timestamp_yearfirst if mapping else None,
            fuzzy=mapping.timestamp_fuzzy if mapping else None,
        )
        # strip timestamp
        doc.pop(timestamp_field, None)

        # map any other field
        d = {}
        for k, v in doc.items():
            # do not
            mapped = self._process_key(k, v)
            d.update(mapped)

        """
        MutyLogger.get_instance().debug(
            "operation_id=%s, context_id=%s, source_id=%s, doc=\n%s"
            % (
                self._operation_id,
                self._context_id,
                self._source_id,
                json.dumps(d, indent=2),
            )
        )"""

        # create a gulp document
        return GulpDocument(
            self,
            timestamp=str(
                ts_nsec + (offset_msec * muty.time.MILLISECONDS_TO_NANOSECONDS)
            ),
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=str(doc),
            event_sequence=record_idx,
            **d,
        )

    async def query_external(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: Any,
        q: Any,
        q_options: GulpQueryParameters,
        flt: GulpQueryFilter = None,
    ) -> tuple[int, int]:

        await super().query_external(
            sess, user_id, req_id, ws_id, index, q, q_options, flt
        )
        if q_options.external_parameters.plugin_params.is_empty():
            # use default
            q_options.external_parameters.plugin_params = GulpPluginParameters(
                mappings={"default": GulpMapping(fields={})},
                # these may be set, keep it
                additional_mapping_files=q_options.external_parameters.plugin_params.additional_mapping_files,
            )

        # load any mapping set
        await self._initialize(q_options.external_parameters.plugin_params)

        # connect
        is_elasticsearch = self._custom_params.get("is_elasticsearch")
        uri = q_options.external_parameters.uri
        user = q_options.external_parameters.username
        password = q_options.external_parameters.password
        MutyLogger.get_instance().info(
            "connecting to %s, is_elasticsearch=%r, user=%s"
            % (uri, is_elasticsearch, user)
        )
        try:
            if is_elasticsearch:
                # elastic
                cl: AsyncElasticsearch = AsyncElasticsearch(
                    uri,
                    basic_auth=(user, password),
                    verify_certs=False,
                )
            else:
                # opensearch
                cl: AsyncOpenSearch = AsyncOpenSearch(
                    uri,
                    http_auth=(user, password),
                    verify_certs=False,
                )
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            await self._query_external_done(q_options.name, 0, ex)
            return 0, 0

        # query
        query_error = None
        total_count = 0
        try:
            total_count, processed = await GulpQueryHelpers.query_raw(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q=q,
                index=index,
                flt=flt,
                q_options=q_options,
                el=cl,
                callback=self.process_record,
            )
            if total_count == 0:
                MutyLogger.get_instance().warning("no results!")
                await self._query_external_done(
                    q_options.name,
                    0,
                )
                return 0, 0

            MutyLogger.get_instance().debug(
                "elasticsearch/opensearch query done, total=%d, processed=%d!"
                % (total_count, processed)
            )
            return total_count, processed

        except Exception as ex:
            # error during query
            MutyLogger.get_instance().exception(ex)
            query_error = ex
            return 0, 0

        finally:
            # last flush
            await self._source_done()

            # and signal websocket
            await self._query_external_done(
                q_options.name,
                total_count,
                error=query_error,
            )
            MutyLogger.get_instance().debug("closing client ...")
            await cl.close()

    def sigma_support(self) -> list[GulpPluginSigmaSupport]:
        return [
            GulpPluginSigmaSupport(
                backends=[
                    GulpNameDescriptionEntry(
                        name="elasticsearch_lucene",
                        description="Lucene backend for pySigma",
                    ),
                    GulpNameDescriptionEntry(
                        name="opensearch",
                        description="OpenSearch backend for pySigma",
                    ),
                ],
                pipelines=[
                    GulpNameDescriptionEntry(
                        name="ecs_windows",
                        description="ECS mapping for Windows event logs ingested with Winlogbeat.",
                    ),
                    GulpNameDescriptionEntry(
                        name="ecs_windows_old",
                        description="ECS mapping for Windows event logs ingested with Winlogbeat <= 6.x.",
                    ),
                    GulpNameDescriptionEntry(
                        name="ecs_zeek_beats",
                        description=" Zeek ECS mapping from Elastic.",
                    ),
                    GulpNameDescriptionEntry(
                        name="ecs_zeek_corelight",
                        description="Zeek ECS mapping from Corelight.",
                    ),
                    GulpNameDescriptionEntry(
                        name="zeek_raw",
                        description="Zeek raw JSON log fields.",
                    ),
                ],
                output_formats=[
                    GulpNameDescriptionEntry(
                        name="dsl_lucene",
                        description="DSL with embedded Lucene queries.",
                    )
                ],
            )
        ]

    def sigma_convert(
        self,
        sigma: str,
        s_options: GulpQuerySigmaParameters,
    ) -> list[GulpQuery]:

        # select pipeline, backend and output format to use
        backend, pipeline, output_format = self._check_sigma_support(s_options)

        if pipeline == "ecs_windows":
            pipeline = ecs_windows()
        elif pipeline == "ecs_windows_old":
            pipeline = ecs_windows_old()
        elif pipeline == "ecs_zeek_beats":
            pipeline = ecs_zeek_beats()
        elif pipeline == "ecs_zeek_corelight":
            pipeline = ecs_zeek_corelight()
        elif pipeline == "zeek_raw":
            pipeline = zeek_raw()

        if backend == "opensearch":
            # this is not guaranteed to work with all pipelines, though...
            backend = OpensearchLuceneBackend(processing_pipeline=pipeline)
        else:
            backend = LuceneBackend(processing_pipeline=pipeline)
        return to_gulp_query_struct(sigma, backend, output_format=output_format)
