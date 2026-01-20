import os

import muty.file
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp_client.common import (
    GulpAPICommon,
    _ensure_test_operation,
    _cleanup_test_operation,
)
from gulp_client.db import GulpAPIDb
from gulp_client.user import GulpAPIUser
from gulp_client.utility import GulpAPIUtility
from gulp_client.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase
import os


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    # not needed here
    if os.getenv("SKIP_RESET", "0") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def test_mapping_files_api():
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    test_mapping_file = "chrome_history.json"

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
    MutyLogger.get_instance().info(test_mapping_files_api.__name__ + " passed")


@pytest.mark.asyncio
async def test_plugins_api():
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    test_plugin = "csv.py"
    test_plugin_path = os.path.join(
        GulpConfig.get_instance().path_plugins_extra(), test_plugin
    )
    await muty.file.delete_file_or_dir_async(test_plugin_path)

    l = await GulpAPIUtility.plugin_list(guest_token)
    assert l

    l_ui = await GulpAPIUtility.ui_plugin_list()  # tokenless
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

    # get the sample test ui plugin
    tsx = await GulpAPIUtility.ui_plugin_get("example_ui_plugin.tsx")
    assert tsx and isinstance(
        tsx["content"], str
    )  # base64 encoded string, the plugin TSX content
    assert tsx["filename"] == "example_ui_plugin.tsx"
    MutyLogger.get_instance().info(test_plugins_api.__name__ + " passed")


@pytest.mark.asyncio
async def test_utility_api():

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # test version
    v = await GulpAPIUtility.version(guest_token)
    assert v

    MutyLogger.get_instance().info(test_utility_api.__name__ + " passed")
