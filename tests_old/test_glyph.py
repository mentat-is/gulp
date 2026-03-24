import os
import pytest
import pytest_asyncio
from muty.log import MutyLogger

from gulp.api.collab.structs import COLLABTYPE_GLYPH, GulpCollabFilter
from gulp_client.common import _ensure_test_operation
from gulp_client.glyph import GulpAPIGlyph
from gulp_client.object_acl import GulpAPIObjectACL
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import TEST_OPERATION_ID


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    await _ensure_test_operation()


@pytest.mark.asyncio
async def test_glyph():
    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token
    edit_token = await GulpAPIUser.login("editor", "editor")
    assert edit_token
    # power user can delete glyphs, editor not
    power_token = await GulpAPIUser.login("power", "power")
    assert power_token

    pwd = os.path.dirname(os.path.abspath(__file__))

    # guest cannot create
    st = await GulpAPIGlyph.glyph_create(
        guest_token,
        img_path=os.path.join(pwd, "./user.png"),
        name="user_test_glyph",
        expected_status=401,
    )

    st = await GulpAPIGlyph.glyph_create(
        edit_token,
        img_path=os.path.join(pwd, "./user.png"),
        name="user_test_glyph",
    )
    assert st
    assert st["name"] == "user_test_glyph"

    # name filter
    l = await GulpAPIGlyph.glyph_list(
        guest_token,
        GulpCollabFilter(
            names=["*ser_test_gly*"],
        ),
    )
    assert len(l) == 1
    assert l[0]["id"] == st["id"]

    l = await GulpAPIGlyph.glyph_list(
        guest_token,
        GulpCollabFilter(names=["aaaaa*"]),
    )
    assert not l  # 0 len

    # update
    await GulpAPIGlyph.glyph_update(
        edit_token, st["id"], img_path=os.path.join(pwd, "./user.png"), name="testglyph"
    )
    st = await GulpAPIGlyph.glyph_get_by_id(guest_token, st["id"])
    assert st["name"] == "testglyph"

    # make glyph private
    st_id = st["id"]
    await GulpAPIObjectACL.object_make_private(edit_token, st["id"], COLLABTYPE_GLYPH)
    st = await GulpAPIGlyph.glyph_get_by_id(guest_token, st["id"], expected_status=401)
    l = await GulpAPIGlyph.glyph_list(
        guest_token,
        GulpCollabFilter(
            names=["testglyph"],
        ),
    )
    assert not l

    await GulpAPIObjectACL.object_make_public(edit_token, st_id, COLLABTYPE_GLYPH)
    st = await GulpAPIGlyph.glyph_get_by_id(guest_token, st_id)
    assert st
    l = await GulpAPIGlyph.glyph_list(
        guest_token,
        GulpCollabFilter(
            names=["testglyph"],
        ),
    )
    assert len(l) == 1

    # delete
    await GulpAPIGlyph.glyph_delete(power_token, st["id"])
    l = await GulpAPIGlyph.glyph_list(
        guest_token,
        GulpCollabFilter(
            operation_ids=[TEST_OPERATION_ID],
            names=["testglyph"],
        ),
    )
    assert not l

    MutyLogger.get_instance().info(test_glyph.__name__ + " passed")
