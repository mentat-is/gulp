import pprint
import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.opensearch.query import GulpQuerySigmaParameters
from gulp.api.opensearch.structs import GulpBasicDocument
from tests.api.common import GulpAPICommon
from tests.api.object_acl import GulpAPIObjectACL
from tests.api.stored_query import GulpAPIStoredQuery
from tests.api.user_group import GulpAPIUserGroup
from tests.api.user import GulpAPIUser
from tests.api.link import GulpAPILink
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
    test stored query
    """
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # Reset database
    await GulpAPIDb.reset_collab_as_admin()

    # Login users
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # guest cannot create stored query
    await GulpAPIStoredQuery.stored_query_create(
        guest_token,
        name="Test Query",
        q="search source=* | table timestamp, message",
        tags=["test"],
        description="Test stored query",
        expected_status=401,
    )

    # create stored query
    query = await GulpAPIStoredQuery.stored_query_create(
        editor_token,
        name="Test Query",
        q="search source=* | table timestamp, message",
        q_groups=["query_group_1"],
        tags=["test"],
        description="Test stored query",
    )
    assert query["name"] == "Test Query"

    # guest can view the query
    q = await GulpAPIStoredQuery.stored_query_get_by_id(
        guest_token, query["id"], expected_status=200
    )
    assert q["id"] == query["id"]

    # guest cannot edit the query
    _ = await GulpAPIStoredQuery.stored_query_update(
        guest_token,
        query["id"],
        q=["updated query"],
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor can edit the query
    updated = await GulpAPIStoredQuery.stored_query_update(
        editor_token,
        query["id"],
        q=["updated query"],
        tags=["test", "updated"],
        s_options=GulpQuerySigmaParameters(),
    )
    assert "updated query" in updated["q"]

    # filter queries (6 queries in total, 5 are the default queries)
    flt = GulpCollabFilter(tags=["test"])
    queries = await GulpAPIStoredQuery.stored_query_list(guest_token, flt=flt)
    assert len(queries) == 1

    queries = await GulpAPIStoredQuery.stored_query_list(guest_token)
    assert len(queries) == 6

    flt = GulpCollabFilter(q_groups=["query_group_1"])
    queries = await GulpAPIStoredQuery.stored_query_list(guest_token, flt=flt)
    assert len(queries) == 1
    assert queries[0]["id"] == query["id"]

    flt = GulpCollabFilter(q_groups=["q_group_12345"])
    queries = await GulpAPIStoredQuery.stored_query_list(guest_token, flt=flt)
    assert len(queries) == 0

    # editor can delete query
    d = await GulpAPIStoredQuery.stored_query_delete(editor_token, query["id"])
    assert d["id"] == query["id"]

    # verify deleted
    queries = await GulpAPIStoredQuery.stored_query_list(guest_token, flt=flt)
    assert len(queries) == 0
    MutyLogger.get_instance().info("all tests succeeded!")
