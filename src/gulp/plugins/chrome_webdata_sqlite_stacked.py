import datetime
import os
from copy import deepcopy
from enum import Enum

import muty.os
import muty.string
import muty.time
import muty.xml

import gulp.plugin as gulp_plugin
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    chrome based browser web data plugin stacked over the SQLITE plugin
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return """chrome based browser web data sqlite stacked plugin"""

    def name(self) -> str:
        return "chrome_webdata_sqlite_stacked"

    def version(self) -> str:
        return "1.0"

    class ChromeWebdataTables(Enum):
        autofill = 0
        autofill_model_type_state = 1
        autofill_sync_metadata = 2
        benefit_merchant_domains = 3
        contact_info = 4
        contact_info_type_tokens = 5
        credit_cards = 6
        generic_payment_instruments = 7
        keywords = 8
        local_addresses = 9
        local_addresses_type_tokens = 10
        local_ibans = 11
        local_stored_cvc = 12
        masked_bank_accounts = 13
        masked_bank_accounts_metadata = 14
        masked_credit_card_benefits = 15
        masked_credit_cards = 16
        masked_ibans_metadata = 17
        meta = 18
        offer_data = 19
        offer_eligible_instrument = 20
        offer_merchant_domain = 21
        payment_method_manifest = 22
        payments_customer_data = 23
        plus_address_sync_entity_metadata = 24
        plus_address_sync_model_type_state = 25
        plus_addresses = 26
        secure_payment_confirmation_instrument = 27
        server_card_cloud_token_data = 28
        server_card_metadata = 29
        server_stored_cvc = 30
        token_service = 31
        virtual_card_usage_data = 32
        web_app_manifest_section = 33

    async def record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParams = None,
        **kwargs,
    ) -> list[GulpDocument]:

        #TODO: handle special cases from webdata file in here, e.g. calculate duration, etc
        """"
        for r in record:
            event: GulpDocument = r

            # replace gulp event code with a value of the table
            extra = kwargs["extra"]
            event.event_code = self.ChromeWebdataTables[
                extra["gulp.sqlite.db.table.name"]
            ].name
            event.gulp_event_code = self.ChromeWebdataTables[
                extra["gulp.sqlite.db.table.name"]
            ].value

            fme: list[FieldMappingEntry] = []
            for k,v in event.extra.items():
                e = self._map_source_key(
                    plugin_params,
                    custom_mapping,
                    k,
                    v,
                    index_type_mapping=index_type_mapping,
                    **kwargs
                )
                if e is not None:
                    fme.extend(e)

            # coming from the sqlite plugin, the event_code field contains the table name, we use it here to handle special cases
            # and override it with a more meaningful value
            # if event.event_code == "autofill":
            #    pass
            # TODO:
        """
        return record

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        if plugin_params is None:
            plugin_params = GulpPluginParams()
        fs = TmpIngestStats(source)

        # initialize mapping
        try:
            await self.ingest_plugin_initialize(
                index, source, skip_mapping=True)
            mod = gulp_plugin.load_plugin("sqlite", **kwargs)  
        except Exception as ex:
            fs=self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)
        
        plugin_params.record_to_gulp_document_fun.append(self.record_to_gulp_document)
        plugin_params.mapping_file = "chrome_webdata.json"
        return await mod.ingest(
            index,
            req_id,
            client_id,
            operation_id,
            context,
            source,
            ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )
