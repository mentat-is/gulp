import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.rest.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_SOURCE_ID_2,
    TEST_WS_ID,
)
from tests.api.common import GulpAPICommon
from tests.api.user import GulpAPIUser
from tests.api.highlight import GulpAPIHighlight
from tests.api.object_acl import GulpAPIObjectACL
from tests.api.db import GulpAPIDb


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # login editor, admin, guest, power
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    # create another editor user
    editor2_user_id = "editor2"
    editor2_pwd = "MyPassword1234!"
    editor2_user = await GulpAPIUser.user_create(
        admin_token,
        "editor2",
        editor2_pwd,
        ["edit", "read"],
        email="editor2@localhost.com",
    )
    assert editor2_user["id"] == editor2_user_id

    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    editor2_token = await GulpAPIUser.login("editor2", editor2_pwd)
    assert editor2_token

    # create a highlight by editor
    highlight = await GulpAPIHighlight.highlight_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        ws_id=TEST_WS_ID,
        source_id=TEST_SOURCE_ID_2,
        time_range=(1000000, 1500000),
        name="test_highlight",
        tags=["test"],
        color="blue",
    )
    assert highlight["name"] == "test_highlight"

    # guest can see the highlight
    h = await GulpAPIHighlight.highlight_get_by_id(
        guest_token, highlight["id"], expected_status=200
    )
    assert h["id"] == highlight["id"]

    # guest cannot edit the highlight
    await GulpAPIHighlight.highlight_update(
        guest_token,
        highlight["id"],
        ws_id=TEST_WS_ID,
        time_range=(1000000, 2000000),
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor2 cannot delete the highlight
    await GulpAPIHighlight.highlight_delete(
        editor2_token, highlight["id"], TEST_WS_ID, expected_status=401
    )

    # editor can edit the highlight
    updated = await GulpAPIHighlight.highlight_update(
        editor_token,
        highlight["id"],
        ws_id=TEST_WS_ID,
        time_range=(1000000, 2000000),
        tags=["test", "updated"],
    )
    assert updated["time_range"] == [1000000, 2000000]
    assert updated["tags"] == ["test", "updated"]

    # create another highlight
    highlight2 = await GulpAPIHighlight.highlight_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        ws_id=TEST_WS_ID,
        source_id=TEST_SOURCE_ID_2,
        time_range=(2000000, 2500000),
        name="test_highlight_2",
        tags=["test2"],
        color="red",
    )
    assert highlight2["name"] == "test_highlight_2"

    # filter by name/tags
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        source_ids=[TEST_SOURCE_ID_2],
        names=["test_highlight_2"],
        tags=["test2"],
    )
    highlights = await GulpAPIHighlight.highlight_list(
        guest_token,
        flt=flt,
    )
    assert len(highlights) == 1
    assert highlights[0]["id"] == highlight2["id"]

    # make highlight 1 private
    await GulpAPIObjectACL.object_make_private(
        editor_token,
        highlight["id"],
        GulpCollabType.HIGHLIGHT,
    )

    # guest cannot see private highlight
    highlights = await GulpAPIHighlight.highlight_list(
        guest_token,
    )
    assert len(highlights) == 1
    assert highlights[0]["id"] == highlight2["id"]
    
    # editor can delete the highlight
    d = await GulpAPIHighlight.highlight_delete(
        editor_token, highlight["id"], TEST_WS_ID
    )
    assert d["id"] == highlight["id"]

    MutyLogger.get_instance().info("all HIGHLIGHT tests succeeded!")
