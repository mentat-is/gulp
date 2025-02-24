import pprint

import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.rest.client.common import GulpAPICommon, _test_init
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.note import GulpAPINote
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.client.user_group import GulpAPIUserGroup
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


@pytest.mark.asyncio
async def test_note():
    """
    test notes and ACL
    """

    # ingest some data
    from tests.ingest.test_ingest import test_win_evtx

    await test_win_evtx()
    source_id = "64e7c3a4013ae243aa13151b5449aac884e36081"
    doc_id = "c8869c95f8e92be5e86d6b1f03a50252"

    # create note
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    note1 = await GulpAPINote.note_create(
        guest_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=source_id,
        text="pinned note 1",
        time_pin=1000000,
        name="test_pinned_note",
        tags=["test"],
        color="blue",
        expected_status=401,
    )

    note1 = await GulpAPINote.note_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=source_id,
        text="pinned note 1",
        time_pin=1000000,
        name="test_pinned_note",
        tags=["test"],
        color="blue",
    )

    note2 = await GulpAPINote.note_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=source_id,
        text="pinned note 2",
        time_pin=1100000,
        name="test_pinned_note_2",
        tags=["test"],
        color="blue",
    )

    note3 = await GulpAPINote.note_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=source_id,
        text="pinned note 3",
        docs=[
            {
                "_id": doc_id,
                "@timestamp": "2021-01-01T00:00:00Z",
                "gulp.timestamp": 1609459200000000000,
                "gulp.operation_id": TEST_OPERATION_ID,
                "gulp.context_id": TEST_CONTEXT_ID,
                "gulp.source_id": source_id,
            }
        ],
        name="test_pinned_note_3",
        tags=["test"],
        color="blue",
    )

    # doc filter
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            doc_ids=[doc_id],
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == note3["id"]

    # doc time range filter
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            doc_time_range=(1609459100000000000, 1609459300000000000),
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == note3["id"]

    # time pin filter
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
            time_pin_range=(1000000, 1100000),
        ),
    )
    assert len(l) == 2

    # update
    await GulpAPINote.note_update(edit_token, note1["id"], text="modified")
    note1 = await GulpAPINote.note_get_by_id(guest_token, note1["id"])
    assert note1["text"] == "modified"
    assert note1["edits"]

    # delete
    await GulpAPINote.note_delete(edit_token, note1["id"])

    # list without filter, 2 notes (was 3, 1 deleted)
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
        ),
    )
    assert len(l) == 2

    # make note2 private
    await GulpAPIObjectACL.object_make_private(
        guest_token, note2["id"], GulpCollabType.NOTE, expected_status=401
    )
    await GulpAPIObjectACL.object_make_private(
        edit_token,
        note2["id"],
        GulpCollabType.NOTE,
    )
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
        ),
    )

    # only note 3 is visible to guest
    assert len(l) == 1
    assert l[0]["id"] == note3["id"]

    # make public again
    await GulpAPIObjectACL.object_make_public(
        edit_token,
        note2["id"],
        GulpCollabType.NOTE,
    )
    l = await GulpAPINote.note_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
            context_ids=[TEST_CONTEXT_ID],
            source_ids=[source_id],
        ),
    )
    assert len(l) == 2
    MutyLogger.get_instance().info(test_note.__name__ + " passed")
