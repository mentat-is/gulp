import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import COLLABTYPE_STORY, GulpCollabFilter
from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.story import GulpAPIStory
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


@pytest.mark.asyncio
async def test_story():
    target_doc_ids = [
        "9d6f4d014b7dd9f5f65ce43f3c142749",
        "7090d29202d7cd8b57c30fa14202ac37",
    ]

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    st = await GulpAPIStory.story_create(
        guest_token,
        operation_id=TEST_OPERATION_ID,
        doc_ids=target_doc_ids,
        name="story",
        expected_status=401,
    )

    st = await GulpAPIStory.story_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        name="story",
        doc_ids=target_doc_ids,
    )
    assert st
    assert st["doc_ids"][0] == target_doc_ids[0]
    assert st["doc_ids"][1] == target_doc_ids[1]
    assert st["name"] == "story"

    # doc filter
    l = await GulpAPIStory.story_list(
        guest_token,
        GulpCollabFilter(
            doc_ids=[target_doc_ids[0]],
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == st["id"]

    l = await GulpAPIStory.story_list(
        guest_token,
        GulpCollabFilter(operation_ids=[TEST_OPERATION_ID], doc_ids=["aaaaa"]),
    )
    assert not l  # 0 len

    # update
    await GulpAPIStory.story_update(
        edit_token, st["id"], doc_ids=["aaaaa"], color="black"
    )
    st = await GulpAPIStory.story_get_by_id(guest_token, st["id"])
    assert st["color"] == "black"
    assert st["doc_ids"] == ["aaaaa"]
    assert st["name"] == "story"

    # make story private
    st_id = st["id"]
    await GulpAPIObjectACL.object_make_private(
        edit_token, st["id"], COLLABTYPE_STORY,
    )
    st = await GulpAPIStory.story_get_by_id(guest_token, st["id"], expected_status=401)
    l = await GulpAPIStory.story_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(edit_token, st_id, COLLABTYPE_STORY)
    st = await GulpAPIStory.story_get_by_id(guest_token, st_id)
    assert st

    # delete
    await GulpAPIStory.story_delete(edit_token, st["id"])
    l = await GulpAPIStory.story_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_story.__name__ + " passed")
