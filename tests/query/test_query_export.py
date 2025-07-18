#!/usr/bin/env python3
import os

import pytest
from muty.log import MutyLogger

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryParameters
from gulp_client.common import GulpAPICommon
from gulp_client.query import GulpAPIQuery
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)


@pytest.mark.asyncio
async def test_query_export():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    current_dir = os.path.dirname(os.path.realpath(__file__))
    out_path = os.path.join(current_dir, "export.json")

    try:
        q_options: GulpQueryParameters = GulpQueryParameters()
        # q_options.limit = 500
        # q_options.total_limit = 500

        flt: GulpQueryFilter = GulpQueryFilter()

        path = await GulpAPIQuery.query_gulp_export_json(
            guest_token,
            TEST_OPERATION_ID,
            output_file_path=out_path,
            flt=flt,
            q_options=q_options,
        )
        assert path == out_path
        MutyLogger.get_instance().info(test_query_export.__name__ + " succeeded!")

    finally:
        pass
        # muty.file.delete_file_or_dir(out_path)
