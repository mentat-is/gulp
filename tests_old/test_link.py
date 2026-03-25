import pytest
import pytest_asyncio
from muty.log import MutyLogger
import os
from gulp.api.collab.structs import COLLABTYPE_LINK, GulpCollabFilter
from gulp_client.common import _ensure_test_operation, _cleanup_test_operation
from gulp_client.link import GulpAPILink
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
async def test_link():
    if os.getenv("SKIP_RESET", "0") != "1":
        # ingest some data
        from tests.ingest.test_ingest import test_win_evtx

        await test_win_evtx()

    doc_id = "4905967cfcaf2abe0e28322ff085619d"
    target_doc_ids = [
        "a20b2f881e11f5f61cb3ba007aa662f6",
        "02ea5d4d3962ba8e5867e630146c30c4",
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
        TEST_OPERATION_ID,
        GulpCollabFilter(
            doc_ids=[target_doc_ids[0]],
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == lnk["id"]

    l = await GulpAPILink.link_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(operation_ids=[TEST_OPERATION_ID], doc_ids=["aaaaa"]),
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
    lnk_id = lnk["id"]
    await GulpAPIObjectACL.object_make_private(edit_token, lnk["id"], COLLABTYPE_LINK)
    lnk = await GulpAPILink.link_get_by_id(guest_token, lnk["id"], expected_status=401)
    l = await GulpAPILink.link_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(edit_token, lnk_id, COLLABTYPE_LINK)
    lnk = await GulpAPILink.link_get_by_id(guest_token, lnk_id)
    assert lnk

    # delete
    await GulpAPILink.link_delete(edit_token, lnk["id"])
    l = await GulpAPILink.link_list(
        guest_token,
        TEST_OPERATION_ID,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_link.__name__ + " passed")
