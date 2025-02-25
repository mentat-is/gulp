import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.link import GulpAPILink
from gulp.api.rest.client.object_acl import GulpAPIObjectACL
from gulp.api.rest.client.user import GulpAPIUser
from gulp.api.rest.test_values import TEST_CONTEXT_ID, TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


@pytest.mark.asyncio
async def test_link():
    doc_id = "c8869c95f8e92be5e86d6b1f03a50252"
    target_doc_ids = [
        "9d6f4d014b7dd9f5f65ce43f3c142749",
        "7090d29202d7cd8b57c30fa14202ac37",
    ]

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token

    lnk = await GulpAPILink.link_create(
        guest_token,
        operation_id=TEST_OPERATION_ID,
        doc_id_from=doc_id,
        doc_ids=target_doc_ids,
        expected_status=401,
    )

    lnk = await GulpAPILink.link_create(
        edit_token,
        operation_id=TEST_OPERATION_ID,
        doc_id_from=doc_id,
        doc_ids=target_doc_ids,
    )
    assert lnk
    assert lnk["doc_id_from"] == doc_id

    # doc filter
    l = await GulpAPILink.link_list(
        guest_token,
        GulpCollabFilter(
            doc_ids=[target_doc_ids[0]],
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == lnk["id"]

    l = await GulpAPILink.link_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID], doc_ids=["aaaaa"]
        ),
    )
    assert not l  # 0 len

    # update
    await GulpAPILink.link_update(
        edit_token, lnk["id"], doc_ids=["aaaaa"], color="black"
    )
    lnk = await GulpAPILink.link_get_by_id(guest_token, lnk["id"])
    assert lnk["color"] == "black"
    assert lnk["doc_ids"] == ["aaaaa"]

    # make link private
    lnk_id=lnk["id"]
    await GulpAPIObjectACL.object_make_private(
        edit_token, lnk["id"], GulpCollabType.LINK
    )
    lnk = await GulpAPILink.link_get_by_id(guest_token, lnk["id"], expected_status=401)
    l = await GulpAPILink.link_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(
        edit_token, lnk_id, GulpCollabType.LINK
    )
    lnk = await GulpAPILink.link_get_by_id(guest_token, lnk_id)
    assert lnk

    # delete
    await GulpAPILink.link_delete(edit_token, lnk["id"])
    l = await GulpAPILink.link_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_link.__name__ + " passed")
