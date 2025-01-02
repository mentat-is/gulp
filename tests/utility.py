import pytest
import shutil
import os
import pathlib
import tempfile
from muty.log import MutyLogger
import muty.file
from gulp.api.collab.structs import MissingPermission
from gulp.config import GulpConfig
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from gulp.plugin import GulpPluginBase
from tests.api.common import GulpAPICommon
from tests.api.db import GulpAPIDb
from tests.api.user import GulpAPIUser
from tests.api.utility import GulpAPIUtility
import os


@pytest.mark.asyncio
async def test():
    async def _test_plugins():
        # reset first
        await GulpAPIDb.reset_collab_as_admin()
        if not os.environ.get("PATH_PLUGINS_EXTRA"):
            raise ValueError("PATH_PLUGINS_EXTRA not set")

        # ensure clean
        test_plugin = "csv.py"
        test_plugin_path = os.path.join(
            GulpConfig.get_instance().path_plugins_extra(), test_plugin
        )
        await muty.file.delete_file_or_dir_async(test_plugin_path)

        # login admin, guest
        admin_token = await GulpAPIUser.login("admin", "admin")
        assert admin_token

        guest_token = await GulpAPIUser.login("guest", "guest")
        assert guest_token

        # guest can list plugins
        l = await GulpAPIUtility.plugin_list(admin_token)
        assert l

        # path should be the default path
        found = False
        for plugin in l:
            if plugin["filename"] == test_plugin:
                assert plugin["path"] == os.path.join(
                    GulpConfig.get_instance().path_plugins_default(), test_plugin
                )
                found = True
        assert found

        # get and upload plugin to extra path
        # (guest cannot download a plugin)
        await GulpAPIUtility.plugin_get(guest_token, test_plugin, expected_status=401)
        p = await GulpAPIUtility.plugin_get(admin_token, test_plugin)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_plugins_default(), test_plugin
        )
        to_be_uploaded = GulpPluginBase.path_from_plugin(test_plugin)

        await GulpAPIUtility.plugin_upload(
            guest_token, to_be_uploaded, expected_status=401
        )
        p = await GulpAPIUtility.plugin_upload(admin_token, to_be_uploaded)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_plugins_extra(), test_plugin
        )

        # list plugin again
        # csv plugin should be in the list, but its path should be the extra path now (precedence)
        l = await GulpAPIUtility.plugin_list(guest_token)
        p = await GulpAPIUtility.plugin_get(admin_token, test_plugin)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_plugins_extra(), test_plugin
        )
        found = False
        for plugin in l:
            if plugin["filename"] == test_plugin:
                assert plugin["path"] == os.path.join(
                    GulpConfig.get_instance().path_plugins_extra(), test_plugin
                )
                found = True
        assert found

        # when deleting a plugin, it should be removed from the extra path but not from the main path
        # (guest cannot delete plugins)
        await GulpAPIUtility.plugin_delete(
            guest_token, plugin=test_plugin, expected_status=401
        )
        d = await GulpAPIUtility.plugin_delete(admin_token, plugin=test_plugin)
        assert d["path"] == os.path.join(
            GulpConfig.get_instance().path_plugins_extra(), test_plugin
        )
        l = await GulpAPIUtility.plugin_list(guest_token)
        found = False
        for plugin in l:
            if plugin["filename"] == test_plugin:
                assert plugin["path"] == os.path.join(
                    GulpConfig.get_instance().path_plugins_default(), test_plugin
                )
                found = True
        assert found

    async def _test_mapping_files():
        # reset first
        await GulpAPIDb.reset_collab_as_admin()
        if not os.environ.get("PATH_MAPPING_FILES_EXTRA"):
            raise ValueError("PATH_MAPPING_FILES_EXTRA not set")

        # ensure clean
        test_mapping_file = "chrome_history.json"
        test_mapping_file_path = os.path.join(
            GulpConfig.get_instance().path_mapping_files_extra(), test_mapping_file
        )
        await muty.file.delete_file_or_dir_async(test_mapping_file_path)

        # login admin, guest
        admin_token = await GulpAPIUser.login("admin", "admin")
        assert admin_token

        guest_token = await GulpAPIUser.login("guest", "guest")
        assert guest_token

        # guest can list and get mapping file
        l = await GulpAPIUtility.mapping_file_list(guest_token)
        assert l

        # path should be the default path
        found = False
        for mf in l:
            if mf["filename"] == test_mapping_file:
                assert mf["path"] == os.path.join(
                    GulpConfig.get_instance().path_mapping_files_default(),
                    test_mapping_file,
                )
                found = True
        assert found

        # get and upload mapping file to extra path
        # (guest cannot download a mapping file)
        await GulpAPIUtility.mapping_file_get(
            guest_token, test_mapping_file, expected_status=401
        )
        p = await GulpAPIUtility.mapping_file_get(admin_token, test_mapping_file)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_mapping_files_default(), test_mapping_file
        )
        to_be_uploaded = GulpConfig.get_instance().build_mapping_file_path(
            test_mapping_file
        )
        await GulpAPIUtility.mapping_file_upload(
            guest_token, to_be_uploaded, expected_status=401
        )
        p = await GulpAPIUtility.mapping_file_upload(admin_token, to_be_uploaded)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_mapping_files_extra(), test_mapping_file
        )

        # list mapping files again
        # mapping file should be in the list, but its path should be the extra path now (precedence)
        l = await GulpAPIUtility.mapping_file_list(guest_token)
        p = await GulpAPIUtility.mapping_file_get(admin_token, test_mapping_file)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_mapping_files_extra(), test_mapping_file
        )
        found = False
        for mf in l:
            if mf["filename"] == test_mapping_file:
                assert mf["path"] == os.path.join(
                    GulpConfig.get_instance().path_mapping_files_extra(),
                    test_mapping_file,
                )
                found = True
        assert found

        # when deleting a mapping file, it should be removed from the extra path but not from the main path
        # (guest cannot delete mapping files)
        await GulpAPIUtility.mapping_file_delete(
            guest_token, mapping_file=test_mapping_file, expected_status=401
        )
        d = await GulpAPIUtility.mapping_file_delete(
            admin_token, mapping_file=test_mapping_file
        )
        assert d["path"] == os.path.join(
            GulpConfig.get_instance().path_mapping_files_extra(), test_mapping_file
        )
        l = await GulpAPIUtility.mapping_file_list(guest_token)
        found = False
        for mf in l:
            if mf["filename"] == test_mapping_file:
                assert mf["path"] == os.path.join(
                    GulpConfig.get_instance().path_mapping_files_default(),
                    test_mapping_file,
                )
                found = True
        assert found

    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test version
    v = await GulpAPIUtility.version(guest_token)
    assert v

    # test mapping files api
    await _test_mapping_files()

    # test plugin api
    await _test_plugins()

    MutyLogger.get_instance().info("all tests succeeded!")
