import os
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import COLLABTYPE_GLYPH, GulpCollabFilter
from gulp_client.common import _ensure_test_operation
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import TEST_CONTEXT_ID, TEST_OPERATION_ID
from gulp_client.storage import GulpAPIStorage
from gulp.structs import GulpPluginParameters


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_storage():
    # ingest file
    from tests.ingest.test_ingest import test_win_evtx
    await test_win_evtx(plugin_params=GulpPluginParameters(store_file=True))

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    # list and delete files
    d = await GulpAPIStorage.storage_list_files(token=guest_token, operation_id=TEST_OPERATION_ID, expected_status=401)
    d = await GulpAPIStorage.storage_list_files(token=edit_token, operation_id=TEST_OPERATION_ID)
    assert len(d["objects"]) == 1
    d = await GulpAPIStorage.storage_list_files(token=edit_token, operation_id="notexist", expected_status=404)
    d = await GulpAPIStorage.storage_delete_by_tags(token=edit_token, operation_id=TEST_OPERATION_ID, context_id=TEST_CONTEXT_ID)
    assert d["num_deleted"] == 1
    d = await GulpAPIStorage.storage_delete_by_tags(token=edit_token, operation_id=TEST_OPERATION_ID, context_id=TEST_CONTEXT_ID)
    assert d["num_deleted"] == 0
    
