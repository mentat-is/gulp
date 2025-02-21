import pprint

import pytest
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.rest.client.common import GulpAPICommon
from gulp.api.rest.client.db import GulpAPIDb
from gulp.api.rest.client.link import GulpAPILink
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


@pytest.mark.asyncio
async def test():
    """
    test links and ACL
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

    # create test documents
    docs = [
        GulpBasicDocument(
            id="test_doc_from",
            timestamp="2019-01-01T00:00:00Z",
            gulp_timestamp=1000000,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
        GulpBasicDocument(
            id="test_doc_to1",
            timestamp="2019-01-01T00:00:01Z",
            gulp_timestamp=1000001,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
        GulpBasicDocument(
            id="test_doc_to2",
            timestamp="2019-01-01T00:00:02Z",
            gulp_timestamp=1000002,
            operation_id=TEST_OPERATION_ID,
            context_id=TEST_CONTEXT_ID,
            source_id=TEST_SOURCE_ID,
        ).model_dump(by_alias=True, exclude_none=True),
    ]

    # create a link by editor
    link1 = await GulpAPILink.link_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        doc_id_from=docs[0]["_id"],
        doc_ids=[docs[1]["_id"]],
        name="test_link1",
        tags=["test"],
        color="blue",
    )
    assert link1["doc_id_from"] == docs[0]["_id"]
    assert docs[1]["_id"] in link1["doc_ids"]

    # guest can see the link
    link = await GulpAPILink.link_get_by_id(
        guest_token, link1["id"], expected_status=200
    )
    assert link["id"] == link1["id"]

    # guest cannot edit the link
    _ = await GulpAPILink.link_update(
        guest_token,
        link1["id"],
        doc_ids=[docs[2]["_id"]],
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor2 cannot delete the link
    await GulpAPILink.link_delete(editor2_token, link1["id"], expected_status=401)

    # editor can edit the link
    updated = await GulpAPILink.link_update(
        editor_token,
        link1["id"],
        doc_ids=[docs[2]["_id"]],
        tags=["test", "updated"],
    )
    assert docs[2]["_id"] in updated["doc_ids"]
    assert len(updated["tags"]) == 2
    assert "updated" in updated["tags"]

    # editor can delete the link
    d = await GulpAPILink.link_delete(editor_token, link1["id"])
    assert d["id"] == link1["id"]

    # create more links for testing
    link2 = await GulpAPILink.link_create(
        editor_token,
        operation_id=TEST_OPERATION_ID,
        doc_id_from=docs[0]["_id"],
        doc_ids=[docs[1]["_id"], docs[2]["_id"]],
        name="test_link2",
        tags=["test"],
        color="red",
    )
    assert len(link2["doc_ids"]) == 2

    # editor can make link private
    updated = await GulpAPIObjectACL.object_make_private(
        editor_token,
        link2["id"],
        GulpCollabType.LINK,
    )
    assert updated["granted_user_ids"] == ["editor"]

    # guest cannot see private link
    _ = await GulpAPILink.link_get_by_id(guest_token, link2["id"], expected_status=401)

    # editor can add guest to object grants
    updated = await GulpAPIObjectACL.object_add_granted_user(
        editor_token,
        link2["id"],
        GulpCollabType.LINK,
        "guest",
    )
    assert "guest" in updated["granted_user_ids"]

    # guest can now see the link
    link = await GulpAPILink.link_get_by_id(
        guest_token, link2["id"], expected_status=200
    )
    assert link["id"] == link2["id"]

    MutyLogger.get_instance().info("all LINK/ACL tests succeeded!")
