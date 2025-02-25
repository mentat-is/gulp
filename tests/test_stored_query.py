import json
import os

import muty.file
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import GulpCollabFilter
from gulp.api.rest.client.common import _test_init
from gulp.api.rest.client.stored_query import GulpAPIStoredQuery
from gulp.api.rest.client.user import GulpAPIUser
from gulp.structs import GulpPluginParameters


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    await _test_init(recreate=True)


@pytest.mark.asyncio
async def test_stored_query():
    # login users
    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    # delete any existing stored queries
    queries = await GulpAPIStoredQuery.stored_query_list(admin_token)
    for q in queries:
        await GulpAPIStoredQuery.stored_query_delete(admin_token, q["id"])

    # read sigma
    sigma_q = muty.file.read_file("./query/sigma/match_all.yaml")

    sigma_q: str = sigma_q.decode("utf-8")
    raw_q: str = json.dumps({"query": {"match_all": {}}})

    # guest cannot create stored query
    await GulpAPIStoredQuery.stored_query_create(
        guest_token,
        name="test_query_sigma",
        q=sigma_q,
        tags=["test"],
        description="Test stored sigma query",
        plugin="win_evtx",
        plugin_params=GulpPluginParameters(
            custom_parameters={"test": "test"}
        ).model_dump(),
        expected_status=401,
    )

    query_sigma = await GulpAPIStoredQuery.stored_query_create(
        editor_token,
        name="test_query_sigma",
        q=sigma_q,
        tags=["test", "sigma"],
        plugin="win_evtx",
        plugin_params=GulpPluginParameters(
            custom_parameters={"test": "test"}
        ).model_dump(),
        description="Test stored sigma query",
    )
    assert query_sigma["name"] == "test_query_sigma"
    assert query_sigma["q"] == sigma_q

    query_raw = await GulpAPIStoredQuery.stored_query_create(
        editor_token,
        name="test_query_raw",
        q=raw_q,
        tags=["test", "raw"],
        q_groups=["query_group_1"],
        description="Test stored query",
    )
    assert query_raw["name"] == "test_query_raw"
    assert query_raw["q"] == raw_q

    # filter by tags
    queries = await GulpAPIStoredQuery.stored_query_list(
        guest_token, GulpCollabFilter(tags=["test"])
    )
    assert len(queries) == 2
    queries = await GulpAPIStoredQuery.stored_query_list(
        guest_token, GulpCollabFilter(tags=["sigma"])
    )
    assert len(queries) == 1

    # filter by q_groups
    queries = await GulpAPIStoredQuery.stored_query_list(
        guest_token, GulpCollabFilter(q_groups=["query_group_1"])
    )
    assert len(queries) == 1
    assert queries[0]["id"] == query_raw["id"]

    # guest cannot edit the query
    _ = await GulpAPIStoredQuery.stored_query_update(
        guest_token,
        query_sigma["id"],
        q="updated query",
        tags=["test", "updated"],
        expected_status=401,
    )

    # editor can edit the query
    updated = await GulpAPIStoredQuery.stored_query_update(
        editor_token,
        query_sigma["id"],
        q="updated query",
        tags=["test", "updated"],
    )
    assert "updated query" in updated["q"]
    assert "updated" in updated["tags"]

    # editor can delete query
    d = await GulpAPIStoredQuery.stored_query_delete(editor_token, query_sigma["id"])
    assert d["id"] == query_sigma["id"]
    d = await GulpAPIStoredQuery.stored_query_delete(editor_token, query_raw["id"])
    assert d["id"] == query_raw["id"]

    # verify deleted
    queries = await GulpAPIStoredQuery.stored_query_list(guest_token)
    assert len(queries) == 0
    MutyLogger.get_instance().info(test_stored_query.__name__ + " passed")
