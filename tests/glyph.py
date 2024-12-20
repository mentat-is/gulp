import os
import pytest
from muty.log import MutyLogger
from gulp.api.collab.structs import GulpCollabFilter, GulpCollabType
from gulp.api.rest.test_values import TEST_HOST, TEST_INDEX, TEST_REQ_ID, TEST_WS_ID
from tests.api.common import GulpAPICommon
from tests.api.user import GulpAPIUser
from tests.api.glyph import GulpAPIGlyph
from tests.api.db import GulpAPIDb
from tests.api.object_acl import GulpAPIObjectACL


@pytest.mark.asyncio
async def test():
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )

    # reset first
    await GulpAPIDb.reset_collab_as_admin()

    # login editor, guest, power
    editor_token = await GulpAPIUser.login("editor", "editor")
    assert editor_token

    # power user can delete too
    power_token = await GulpAPIUser.login("power", "power")
    assert power_token

    guest_token = await GulpAPIUser.login("guest", "guest")
    assert guest_token

    admin_token = await GulpAPIUser.login("admin", "admin")
    assert admin_token

    current_dir = os.path.dirname(os.path.realpath(__file__))
    glyph_1_path = os.path.join(current_dir, "../src/gulp/api/collab/assets/user.png")
    glyph_2_path = os.path.join(
        current_dir, "../src/gulp/api/collab/assets/operation.png"
    )

    # guest user cannot create glyph
    await GulpAPIGlyph.glyph_create(
        guest_token, glyph_1_path, "test_glyph", expected_status=401
    )

    # editor user can create glyph
    glyph = await GulpAPIGlyph.glyph_create(editor_token, glyph_1_path, "test_glyph")
    assert glyph.get("name") == "test_glyph"
    img1 = glyph.get("img")
    assert img1

    # editor can update glyph
    updated = await GulpAPIGlyph.glyph_update(
        editor_token, glyph["id"], glyph_2_path, "updated_glyph"
    )
    assert updated.get("name") == "updated_glyph"
    img2 = updated.get("img")
    assert img2 and img1 != img2

    # guest cannot delete glyph
    await GulpAPIGlyph.glyph_delete(guest_token, updated["id"], expected_status=401)

    # having just reset and create a glyph, 3 glyphs should be present
    glyphs = await GulpAPIGlyph.glyph_list(guest_token)
    assert glyphs and len(glyphs) == 3

    # glyph filter by name ("user_icon" glyph is one of the default glyphs)
    glyphs = await GulpAPIGlyph.glyph_list(
        guest_token, GulpCollabFilter(names=["user_icon"])
    )
    assert glyphs and len(glyphs) == 1

    # make glyph private, only editor can see it
    await GulpAPIObjectACL.object_make_private(
        editor_token, updated["id"], GulpCollabType.GLYPH
    )

    # guest sees 2 glyphs
    glyphs = await GulpAPIGlyph.glyph_list(guest_token)
    assert glyphs and len(glyphs) == 2

    # power user sees 2 glyphs
    glyphs = await GulpAPIGlyph.glyph_list(power_token)
    assert glyphs and len(glyphs) == 2

    # editor sees 3 glyphs
    glyphs = await GulpAPIGlyph.glyph_list(editor_token)
    assert glyphs and len(glyphs) == 3

    # admin sees 3 glyphs
    glyphs = await GulpAPIGlyph.glyph_list(admin_token)
    assert glyphs and len(glyphs) == 3

    # power user can't delete glyph due to private
    await GulpAPIGlyph.glyph_delete(power_token, updated["id"], expected_status=401)

    # editor can delete glyph
    d = await GulpAPIGlyph.glyph_delete(
        editor_token, updated["id"]
    )
    assert d["id"] == updated["id"]

    MutyLogger.get_instance().info("all USER tests succeeded!")
