import pytest
import pytest_asyncio
from muty.log import MutyLogger
import os
from gulp.api.collab.structs import COLLABTYPE_HIGHLIGHT, GulpCollabFilter
from gulp_client.common import _ensure_test_operation, _cleanup_test_operation
from gulp_client.highlight import GulpAPIHighlight
from gulp_client.object_acl import GulpAPIObjectACL
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    if os.getenv("SKIP_RESET", "0") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def test_highlight():
    # ingest some data
    if os.getenv("SKIP_RESET", "0") != "1":
        from tests.ingest.test_ingest import test_win_evtx

        await test_win_evtx()

    source_id = "64e7c3a4013ae243aa13151b5449aac884e36081"

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    h = await GulpAPIHighlight.highlight_create(
        guest_token,
        operation_id=TEST_OPERATION_ID,
        time_range=[1000000, 2000000],
        expected_status=401,
    )

    h = await GulpAPIHighlight.highlight_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        time_range=[1000000, 2000000],
    )
    assert h

    # doc filter
    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(operation_ids=[TEST_OPERATION_ID]),
    )
    assert len(l) == 1
    assert l[0]["id"] == h["id"]

    # update
    await GulpAPIHighlight.highlight_update(
        edit_token, h["id"], color="black", time_range=[2000000, 3000000]
    )
    h = await GulpAPIHighlight.highlight_get_by_id(guest_token, h["id"])
    assert h["color"] == "black"
    assert h["time_range"] == [2000000, 3000000]

    # make private
    h_id = h["id"]
    await GulpAPIObjectACL.object_make_private(
        edit_token, h["id"], COLLABTYPE_HIGHLIGHT
    )
    h = await GulpAPIHighlight.highlight_get_by_id(
        guest_token, h_id, expected_status=401
    )
    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(edit_token, h_id, COLLABTYPE_HIGHLIGHT)
    h = await GulpAPIHighlight.highlight_get_by_id(guest_token, h_id)
    assert h

    # delete
    await GulpAPIHighlight.highlight_delete(edit_token, h_id)
    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_highlight.__name__ + " passed")
