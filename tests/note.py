import pprint
import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument
from tests.api.common import GulpAPICommon
from tests.api.object_acl import GulpAPIObjectACL
from tests.api.user_group import GulpAPIUserGroup
from tests.api.user import GulpAPIUser
from tests.api.note import GulpAPINote
from tests.api.db import GulpAPIDb
from gulp.api.rest.test_values import (
    TEST_CONTEXT_ID,
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_SOURCE_ID,
    TEST_WS_ID,
)


@pytest.mark.asyncio
async def test():
    """
    test notes and ACL
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

    # create a note by editor
    pinned_note = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test pinned note",
        time_pin=1000000,
        name="test_pinned_note",
        tags=["test"],
        color="blue",
    )
    assert pinned_note["text"] == "Test pinned note"

    # guest can see the note
    note = await GulpAPINote.note_get_by_id(
        guest_token, pinned_note["id"], expected_status=200
    )
    assert note["id"] == pinned_note["id"]

    # guest cannot edit the note
    _ = await GulpAPINote.note_update(
        guest_token,
        pinned_note["id"],
        text="Updated note",
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor2 cannot o the note
    await GulpAPINote.note_delete(
        editor2_token, pinned_note["id"], expected_status=401
    )

    # editor can edit the note
    updated = await GulpAPINote.note_update(
        editor_token,
        pinned_note["id"],
        text="Updated note",
        tags=["test", "updated"],
    )
    assert updated["text"] == "Updated note"
    assert len(updated["edits"]) == 1
    assert updated["edits"][0]["text"] == "Updated note"
    assert updated["edits"][0]["user_id"] == "editor"
    assert updated["last_editor_id"] == "editor"

    # further edit by admin
    updated = await GulpAPINote.note_update(
        admin_token,
        pinned_note["id"],
        text="Updated note again",
        tags=["test", "updated", "again"],
    )
    assert updated["text"] == "Updated note again"
    assert len(updated["edits"]) == 2
    assert updated["edits"][0]["text"] == "Updated note"
    assert updated["edits"][1]["text"] == "Updated note again"
    assert updated["edits"][0]["user_id"] == "editor"
    assert updated["edits"][1]["user_id"] == "admin"
    assert updated["last_editor_id"] == "admin"

    # editor can delete the note
    d = await GulpAPINote.note_delete(
        editor_token, pinned_note["id"]
    )
    assert d["id"] == pinned_note["id"]

    # create couple of notes
    pinned_note = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test pinned note",
        time_pin=1000000,
        name="test_pinned_note",
        tags=["test"],
        color="blue",
    )
    assert pinned_note["text"] == "Test pinned note"

    docs = [
        GulpBasicDocument(
            id="test_doc",
            timestamp="2019-01-01T00:00:00Z",
            gulp_timestamp=1000000,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
        GulpBasicDocument(
            id="test_doc2",
            timestamp="2019-01-01T00:00:01Z",
            gulp_timestamp=1000001,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
        GulpBasicDocument(
            id="test_doc3",
            timestamp="2019-01-01T00:00:03Z",
            gulp_timestamp=1000002,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
    ]
    note_with_docs_1 = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test note with docs 1",
        docs=docs,
        name="test_note_with_docs_1",
        tags=["test"],
        color="purple",
    )
    assert len(note_with_docs_1["docs"]) == 3

    note_with_docs_2 = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test note with docs 2",
        docs=[
            GulpBasicDocument(
                id="test_doc4",
                timestamp="2019-01-01T01:00:00Z",
                gulp_timestamp=1000008,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
            ).model_dump(by_alias=True, exclude_none=True),
            GulpBasicDocument(
                id="test_doc5",
                timestamp="2019-01-01T02:00:01Z",
                gulp_timestamp=1000009,
                operation_id=TEST_OPERATION_ID,
                context_id=TEST_CONTEXT_ID,
                source_id=TEST_SOURCE_ID,
            ).model_dump(by_alias=True, exclude_none=True),
        ],
        name="test_note_with_docs_2",
        tags=["test"],
        color="blue",
    )
    assert len(note_with_docs_2["docs"]) == 2

    pinned_note_2 = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test pinned note 2",
        time_pin=2000000,
        name="test_pinned_note_2",
        tags=["test"],
        color="blue",
    )
    assert pinned_note_2["time_pin"] == 2000000

    # cannot create note with both data and pin
    n = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test note",
        time_pin=1000000,
        docs=docs,
        name="Test Note",
        tags=["test"],
        color="blue",
        expected_status=400,
    )
    assert not n

    # cannot create note with no docs and no pin
    n = await GulpAPINote.note_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        context_id=TEST_CONTEXT_ID,
        source_id=TEST_SOURCE_ID,
        text="Test note",
        name="Test Note",
        tags=["test"],
        color="blue",
        expected_status=400,
    )
    assert not n

    # guest can see note
    n = await GulpAPINote.note_get_by_id(
        guest_token, pinned_note["id"], expected_status=200
    )
    assert n["id"] == pinned_note["id"]

    # guest can also list notes (by doc)
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        context_ids=[TEST_CONTEXT_ID],
        source_ids=[TEST_SOURCE_ID],
        doc_ids=["test_doc2"],
    )
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    assert notes and len(notes) == 1 and notes[0]["id"] == note_with_docs_1["id"]

    # or by time
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        context_ids=[TEST_CONTEXT_ID],
        source_ids=[TEST_SOURCE_ID],
        doc_time_range=(1000007, 1000009),
    )
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    assert notes and len(notes) == 1 and notes[0]["id"] == note_with_docs_2["id"]

    # editor2 cannot make note private (not owner)
    _ = await GulpAPIObjectACL.object_make_private(
        editor2_token,
        note_with_docs_2["id"],
        GulpCollabType.NOTE,
        expected_status=401,
    )

    # editor can make note private
    updated = await GulpAPIObjectACL.object_make_private(
        editor_token,
        note_with_docs_2["id"],
        GulpCollabType.NOTE,
    )
    assert updated["granted_user_ids"] == ["editor"]

    # guest cannot see note_with_docs_2 anymore, sees pinned_note to the timepin range
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        context_ids=[TEST_CONTEXT_ID],
        source_ids=[TEST_SOURCE_ID],
        doc_time_range=(1000007, 1000009),
    )
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        context_ids=[TEST_CONTEXT_ID],
        source_ids=[TEST_SOURCE_ID],
        time_pin_range=(1900000, 2000009),
    )
    nn = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    notes.extend(nn)
    pprint.pprint(notes)
    assert notes and len(notes) == 1 and notes[0]["id"] == pinned_note_2["id"]

    # editor can add guest to object grants
    updated = await GulpAPIObjectACL.object_add_granted_user(
        editor_token,
        note_with_docs_2["id"],
        GulpCollabType.NOTE,
        "guest",
    )
    assert "guest" in updated["granted_user_ids"]

    # guest can see note_with_docs_2 again
    flt = GulpCollabFilter(
        operation_ids=[TEST_OPERATION_ID],
        context_ids=[TEST_CONTEXT_ID],
        source_ids=[TEST_SOURCE_ID],
        doc_time_range=(1000007, 1000009),
    )
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    assert notes and len(notes) == 1 and notes[0]["id"] == note_with_docs_2["id"]

    # editor can remove guest from object grants
    updated = await GulpAPIObjectACL.object_remove_granted_user(
        editor_token,
        note_with_docs_2["id"],
        GulpCollabType.NOTE,
        "guest",
    )
    assert "guest" not in updated["granted_user_ids"]

    # guest can't see the note again
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    assert not notes

    # guest cannot add itself to admin group
    _ = await GulpAPIUserGroup.usergroup_add_user(
        guest_token,
        "guest",
        "administrators",
        expected_status=401,
    )

    # admin can add guest to admin group
    updated = await GulpAPIUserGroup.usergroup_add_user(
        admin_token,
        "guest",
        "administrators",
    )
    # verify
    users = updated["users"]
    found = False
    for u in users:
        if u["id"] == "guest":
            found = True
    assert found

    # guest can now see note again
    notes = await GulpAPINote.note_list(
        guest_token,
        flt=flt,
    )
    assert notes and len(notes) == 1 and notes[0]["id"] == note_with_docs_2["id"]

    # verify again guest is an admin
    flt = GulpCollabFilter(
        ids=["guest"],
    )

    # this should not fail since guest is an admin (user_list is admin-only api)
    users = await GulpAPIUser.user_list(guest_token)
    assert users

    guest_user: dict = None
    for user in users:
        if user["id"] == "guest":
            guest_user = user
            break
    assert guest_user

    groups = guest_user.get("groups", [])
    assert groups

    is_admin = False
    for g in groups:
        if g["id"] == "administrators":
            is_admin = True
    assert is_admin
    MutyLogger.get_instance().info("all NOTES/ACL tests succeeded!")
