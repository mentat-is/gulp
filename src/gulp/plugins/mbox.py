import mailbox

from gulp import plugin
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.plugin import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginSpecificParam, GulpPluginParameters
from muty.log import MutyLogger


class Plugin(GulpPluginBase):
    def __init__(
        self,
        path: str,
        **kwargs,
    ) -> None:
        super().__init__(path, **kwargs)
        self._eml_parser = None

    def desc(self) -> str:
        return """generic MBOX file processor"""

    def display_name(self) -> str:
        return "mbox"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        return [
            GulpPluginSpecificParam(
                "decode", "bool", "attempt to decode messages wherever possible", True
            )
        ]

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:
        # Load eml plugin
        self._eml_parser = plugin.load_plugin("eml")

        fs = TmpIngestStats(source)
        # initialize mapping
        index_type_mapping, custom_mapping = await self._initialize()(
            index, source, plugin_params=plugin_params
        )

        MutyLogger.get_logger().debug("custom_mapping=%s" % (custom_mapping))

        # get options
        # attempt_decode = plugin_params.extra.get("decode", True)

        # reuse the eml.py plugin record_to_gulp_document to parse the object
        # (this can be done because mailbox.Message is a subclass of email.Message)
        # _eml_parser = eml.Plugin(self.path)

        ev_idx = 0
        try:
            mbox = mailbox.mbox(source)
            for message in mbox.itervalues():
                try:
                    fs, must_break = await self.process_record(
                        index,
                        message,
                        ev_idx,
                        self._eml_parser.record_to_gulp_document,
                        ws_id,
                        req_id,
                        operation_id,
                        client_id,
                        context,
                        source,
                        fs,
                        custom_mapping=custom_mapping,
                        index_type_mapping=index_type_mapping,
                        plugin=self.display_name(),
                        plugin_params=plugin_params,
                        flt=flt,
                        **kwargs,
                    )
                    ev_idx += 1
                    if must_break:
                        break

                except Exception as ex:
                    fs = self._record_failed(fs, message, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs, flt
        )
