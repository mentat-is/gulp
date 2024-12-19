import pytest
from muty.log import MutyLogger
from gulp.api.rest.test_values import TEST_HOST, TEST_REQ_ID, TEST_WS_ID
from tests.common import GulpAPICommon

@pytest.mark.asyncio
async def test():
    gulp_api = GulpAPICommon(host=TEST_HOST, req_id=TEST_REQ_ID, ws_id=TEST_WS_ID)
    
    # reset first
    await gulp_api.reset_gulp_collab()

    # login admin, guest
    admin_token = await gulp_api.login("admin", "admin")
    assert admin_token

    guest_token = await gulp_api.login("guest", "guest")
    assert guest_token

    l = await gulp_api.plugin_list(admin_token)
    assert l

    l = await gulp_api.plugin_list(guest_token)
    assert l

    l = await gulp_api.plugin_get(admin_token, "csv.py")
    assert l

    l = await gulp_api.plugin_get(guest_token, "csv.py")
    assert l

    MutyLogger.get_instance().info("all UTILITY tests succeeded!")
