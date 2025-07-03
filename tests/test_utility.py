import os

import muty.file
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.rest.client.common import GulpAPICommon, _ensure_test_operation
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.client.utility import GulpAPIUtility
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_utility():
    async def _test_plugins():
        # reset first
        if not os.environ.get("PATH_PLUGINS_EXTRA"):
            raise ValueError("PATH_PLUGINS_EXTRA not set")
        MutyLogger.get_instance().info(
            "PATH_PLUGINS_EXTRA: " + os.environ.get("PATH_PLUGINS_EXTRA")
        )

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

        l = await GulpAPIUtility.plugin_list(admin_token)
        assert l

        l_ui = await GulpAPIUtility.ui_plugin_list() # tokenless
        assert l_ui and len(l_ui) >= 1

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
        # (guest cannot upload a plugin)
        await GulpAPIUtility.plugin_get(guest_token, test_plugin, expected_status=401)
        p = await GulpAPIUtility.plugin_get(admin_token, test_plugin)
        assert p["path"] == os.path.join(
            GulpConfig.get_instance().path_plugins_default(), test_plugin
        )

        # get the sample test ui plugin
        tsx = await GulpAPIUtility.ui_plugin_get("example_ui_plugin.tsx")
        assert tsx and isinstance(tsx["content"], str) # base64 encoded string, the plugin TSX content
        assert tsx["filename"] == "example_ui_plugin.tsx"

    async def _test_mapping_files():
        if not os.environ.get("PATH_MAPPING_FILES_EXTRA"):
            raise ValueError("PATH_MAPPING_FILES_EXTRA not set")
        MutyLogger.get_instance().info(
            "PATH_MAPPING_FILES_EXTRA: " +
            os.environ.get("PATH_MAPPING_FILES_EXTRA")
        )

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

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test version
    v = await GulpAPIUtility.version(guest_token)
    assert v

    # check env
    current_dir = os.path.dirname(os.path.realpath(__file__))
    if not os.environ.get("PATH_MAPPING_FILES_EXTRA"):
        # probably not in the dev container, gulp-paid-plugins must be at the same level of gulp directory
        os.environ["PATH_MAPPING_FILES_EXTRA"] = os.path.join(
            current_dir, "../../gulp-paid-plugins/src/gulp-paid-plugins/mapping_files"
        )
        os.environ["PATH_PLUGINS_EXTRA"] = os.path.join(
            current_dir, "../../gulp-paid-plugins/src/gulp-paid-plugins/plugins"
        )
    MutyLogger.get_instance().info(
        "PATH_MAPPING_FILES_EXTRA: " + os.environ["PATH_MAPPING_FILES_EXTRA"]
    )
    MutyLogger.get_instance().info("PATH_PLUGINS_EXTRA: " +
                                   os.environ["PATH_PLUGINS_EXTRA"])
    # assert os.path.exists(os.environ["PATH_MAPPING_FILES_EXTRA"])
    # assert os.path.exists(os.environ["PATH_PLUGINS_EXTRA"])

    # test mapping files api
    await _test_mapping_files()

    # test plugin api
    await _test_plugins()

    MutyLogger.get_instance().info(test_utility.__name__ + " passed")
