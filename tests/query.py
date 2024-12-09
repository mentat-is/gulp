import pprint
import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_HOST,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)
from tests.common import GulpAPICommon


@pytest.mark.asyncio
async def test_windows():
    gulp_api = GulpAPICommon(host=TEST_HOST, req_id=TEST_REQ_ID, ws_id=TEST_WS_ID)

    # reset first
    await gulp_api.reset_gulp_collab()

    # login admin
    admin_token = await gulp_api.login("admin", "admin")
    guest_token = await gulp_api.login("guest", "guest")


    assert admin_token