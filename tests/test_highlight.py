import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import COLLABTYPE_HIGHLIGHT, GulpCollabFilter
from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.highlight import GulpAPIHighlight
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


@pytest.mark.asyncio
async def test_highlight():
    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()
    source_id = "64e7c3a4013ae243aa13151b5449aac884e36081"
    doc_id = "c8869c95f8e92be5e86d6b1f03a50252"

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    h = await GulpAPIHighlight.highlight_create(
        guest_token,
        operation_id=TEST_OPERATION_ID,
        source_id=source_id,
        time_range=[1000000, 2000000],
        expected_status=401,
    )

    h = await GulpAPIHighlight.highlight_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        source_id=source_id,
        time_range=[1000000, 2000000],
    )
    assert h
    assert h["source_id"] == source_id

    # doc filter
    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        GulpCollabFilter(source_ids=[source_id], operation_ids=[TEST_OPERATION_ID]),
    )
    assert len(l) == 1
    assert l[0]["id"] == h["id"]

    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        GulpCollabFilter(operation_ids=[TEST_OPERATION_ID], source_ids=["aaaa"]),
    )
    assert not l  # 0 len

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
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(
        edit_token, h_id, COLLABTYPE_HIGHLIGHT
    )
    h = await GulpAPIHighlight.highlight_get_by_id(guest_token, h_id)
    assert h

    # delete
    await GulpAPIHighlight.highlight_delete(edit_token, h_id)
    l = await GulpAPIHighlight.highlight_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_highlight.__name__ + " passed")
