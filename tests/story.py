import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument
from tests.api.common import GulpAPICommon
from tests.api.object_acl import GulpAPIObjectACL
from tests.api.user_group import GulpAPIUserGroup
from tests.api.user import GulpAPIUser
from tests.api.story import GulpAPIStory
from tests.api.db import GulpAPIDb
from gulp.api.rest.test_values import (
    TEST_OPERATION_ID,
    TEST_HOST,
    TEST_INDEX,
    TEST_REQ_ID,
    TEST_WS_ID,
)


@pytest.mark.asyncio
async def test():
    """
    test stories and ACL
    """
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
    power_token = await GulpAPIUser.login("power", "power")
    assert power_token
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    editor2_token = await GulpAPIUser.login("editor2", editor2_pwd)
    assert editor2_token

    # Test document data
    docs = [
        "doc1",
        "doc2",
        "doc3",
    ]

    # create a story by editor
    story = await GulpAPIStory.story_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        doc_ids=docs,
        name="test_story",
        tags=["test"],
        color="blue",
    )
    assert story["name"] == "test_story"
    assert len(story["doc_ids"]) == 3

    # guest can see the story
    s = await GulpAPIStory.story_get_by_id(
        guest_token, story["id"], expected_status=200
    )
    assert s["id"] == story["id"]

    # get by filters
    flt = GulpCollabFilter(doc_ids=["doc2"])
    s = await GulpAPIStory.story_list(guest_token, flt)
    assert s and len(s) == 1 and s[0]["id"] == story["id"]

    # non-existing doc
    flt = GulpCollabFilter(doc_ids=["doc15"])
    l = await GulpAPIStory.story_list(guest_token, flt)
    assert not l

    # guest cannot edit the story
    _ = await GulpAPIStory.story_update(
        guest_token,
        story["id"],
        doc_ids=["doc4"],
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor2 cannot delete the story
    await GulpAPIStory.story_delete(editor2_token, story["id"], expected_status=401)

    # editor can edit the story
    updated = await GulpAPIStory.story_update(
        editor_token,
        story["id"],
        doc_ids=["doc4", "doc5"],
        tags=["test", "updated"],
    )
    assert len(updated["doc_ids"]) == 2
    assert "updated" in updated["tags"]

    # editor can make story private
    updated = await GulpAPIObjectACL.object_make_private(
        editor_token,
        story["id"],
        GulpCollabType.STORY,
    )
    assert updated["granted_user_ids"] == ["editor"]

    # guest cannot see story anymore
    _ = await GulpAPIStory.story_get_by_id(
        guest_token, story["id"], expected_status=401
    )

    # editor can add guest to object grants
    updated = await GulpAPIObjectACL.object_add_granted_user(
        editor_token,
        story["id"],
        GulpCollabType.STORY,
        "guest",
    )
    assert "guest" in updated["granted_user_ids"]

    # guest can see story again
    s = await GulpAPIStory.story_get_by_id(
        guest_token, story["id"], expected_status=200
    )
    assert s["id"] == story["id"]

    # editor can delete the story
    d = await GulpAPIStory.story_delete(editor_token, story["id"])
    assert d["id"] == story["id"]

    MutyLogger.get_instance().info("all STORY/ACL tests succeeded!")
