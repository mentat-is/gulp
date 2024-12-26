import pytest
import shutil, os
import pathlib
import tempfile
from muty.log import MutyLogger
from gulp.api.collab.structs import MissingPermission
from gulp.config import GulpConfig
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from tests.api.common import GulpAPICommon
from tests.api.db import GulpAPIDb
from tests.api.user import GulpAPIUser
from tests.api.utility import GulpAPIUtility


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # create temp dir for tests
    tmp_dir = tempfile.mkdtemp("gulp_test")

    # login admin, guest
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    l = await GulpAPIUtility.plugin_list(admin_token)
    assert l

    l = await GulpAPIUtility.plugin_list(guest_token)
    assert l

    p = await GulpAPIUtility.plugin_get(admin_token, "csv.py")
    assert p

    p = await GulpAPIUtility.plugin_get(guest_token, "csv.py")
    assert p

    # create copy of plugin to get deleted
    csv_plugin = pathlib.Path(GulpConfig.get_instance().path_plugins()) / "csv.py"
    to_be_deleted = (
        pathlib.Path(GulpConfig.get_instance().path_plugins())
        / "utility_test_delete_me.py"
    )
    shutil.copyfile(csv_plugin, to_be_deleted)

    d = await GulpAPIUtility.plugin_delete(
        guest_token, "utility_test_delete_me.py", expected_status=401
    )
    assert os.path.exists(to_be_deleted)

    d = await GulpAPIUtility.plugin_delete(admin_token, "utility_test_delete_me.py")
    assert not os.path.exists(to_be_deleted)

    # create copy of plugin to get deleted
    csv_plugin = pathlib.Path(GulpConfig.get_instance().path_plugins()) / "csv.py"
    to_be_uploaded = str(
        pathlib.Path(tmp_dir) / "upload_me.py"
    )
    shutil.copy(csv_plugin, to_be_uploaded)

    u = await GulpAPIUtility.plugin_upload(guest_token, to_be_uploaded, expected_status=401)
    assert not pathlib.Path(pathlib.Path(GulpConfig.get_instance().path_plugins()) / "upload_me.py").exists()

    u = await GulpAPIUtility.plugin_upload(admin_token, to_be_uploaded)
    assert pathlib.Path(pathlib.Path(GulpConfig.get_instance().path_plugins()) / "upload_me.py").exists()

    await GulpAPIUtility.plugin_delete(admin_token, "upload_me.py")

    t = await GulpAPIUtility.plugin_tags(admin_token, "csv.py")
    assert len(t) == 0

    v = await GulpAPIUtility.version(admin_token)
    assert v

    # clear temp_dir
    shutil.rmtree(tmp_dir)

    MutyLogger.get_instance().info("all UTILITY tests succeeded!")
