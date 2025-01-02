from enum import Enum
from typing import Any, override

from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    chrome based browser web data plugin stacked over the SQLITE plugin

    ./test_scripts/ingest.py --plugin chrome_webdata_sqlite_stacked --path ~/Downloads/webdata.sqlite
    """

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return """chrome based browser web data sqlite stacked plugin"""

    def display_name(self) -> str:
        return "chrome_webdata_sqlite_stacked"

    @override
    def depends_on(self) -> list[str]:
        return ["sqlite"]

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

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, data: Any
    ) -> dict:
        # do nothing ...
        return record

    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:

        # set as stacked
        try:
            lower = await self.setup_stacked_plugin("sqlite")
        except Exception as ex:
            await self._source_failed(ex)
            return GulpRequestStatus.FAILED

        if not plugin_params:
            plugin_params = GulpPluginParameters()
        plugin_params.mapping_file = "chrome_webdata.json"

        # call lower plugin, which in turn will call our record_to_gulp_document after its own processing
        res = await lower.ingest_file(
            sess=sess,
            stats=stats,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            file_path=file_path,
            original_file_path=original_file_path,
            plugin_params=plugin_params,
            flt=flt,
        )
        await lower.unload()
        return res
