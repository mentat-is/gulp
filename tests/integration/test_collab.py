"""
Integration tests for collaboration API — notes, links, highlights, glyphs.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_collab.py -m integration
"""

import pytest
import uuid


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _setup_operation(client) -> str:
    """Create a throwaway operation and return its ID."""
    op = await client.operations.create(_unique("collab_test_op"))
    return op.id


async def _setup_context(client, operation_id: str) -> str:
    """Create a context inside an operation and return its ID."""
    ctx = await client.operations.context_create(
        operation_id=operation_id,
        context_name=_unique("ctx"),
    )
    return ctx["id"]


async def _setup_source(client, operation_id: str, context_id: str) -> str:
    """Create a source inside a context and return its ID."""
    src = await client.operations.source_create(
        operation_id=operation_id,
        context_id=context_id,
        source_name=_unique("src"),
    )
    return src["id"]


async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Notes
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_note_create_list_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a note, list it, then delete it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ctx_id = await _setup_context(client, op_id)
            src_id = await _setup_source(client, op_id, ctx_id)
            note = await client.collab.note_create(
                operation_id=op_id,
                context_id=ctx_id,
                source_id=src_id,
                name=_unique("note"),
                text="Integration test note",
                time_pin=1_000_000_000,
            )
            assert note.get("id") is not None
            note_id = note["id"]

            # List notes
            notes = await client.collab.note_list(
                flt={"operation_ids": [op_id]}
            )
            assert any(n.get("id") == note_id for n in notes)

            # Delete note
            result = await client.collab.note_delete(note_id)
            assert result.get("id") == note_id
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_note_get_by_id(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a note, retrieve it by ID."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ctx_id = await _setup_context(client, op_id)
            src_id = await _setup_source(client, op_id, ctx_id)
            note_name = _unique("note")
            note = await client.collab.note_create(
                operation_id=op_id,
                context_id=ctx_id,
                source_id=src_id,
                name=note_name,
                text="Fetch by ID test",
                time_pin=2_000_000_000,
            )
            note_id = note["id"]

            fetched = await client.collab.note_get_by_id(note_id)
            assert fetched.get("id") == note_id
            assert fetched.get("name") == note_name
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_note_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a note and update its text."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ctx_id = await _setup_context(client, op_id)
            src_id = await _setup_source(client, op_id, ctx_id)
            note = await client.collab.note_create(
                operation_id=op_id,
                context_id=ctx_id,
                source_id=src_id,
                name=_unique("note"),
                text="Original text",
                time_pin=3_000_000_000,
            )
            note_id = note["id"]

            updated = await client.collab.note_update(
                note_id, text="Updated text"
            )
            assert updated.get("id") == note_id
        finally:
            await _teardown_operation(client, op_id)


# ──────────────────────────────────────────────────────────────────────────────
# Links
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_link_create_list_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a link, list it, then delete it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            link = await client.collab.link_create(
                operation_id=op_id,
                doc_id_from="doc_from_1",
                doc_ids=["doc_target_1", "doc_target_2"],
                name=_unique("link"),
            )
            assert link.get("id") is not None
            link_id = link["id"]

            # List links
            links = await client.collab.link_list(
                flt={"operation_ids": [op_id]}
            )
            assert any(l.get("id") == link_id for l in links)

            # Delete
            result = await client.collab.link_delete(link_id)
            assert result.get("id") == link_id
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_link_get_by_id_and_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a link, fetch it by ID, then update it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            link_name = _unique("link")
            link = await client.collab.link_create(
                operation_id=op_id,
                doc_id_from="doc_from_get",
                doc_ids=["doc_target_1"],
                name=link_name,
                description="Original description",
                tags=["initial"],
            )
            link_id = link["id"]

            fetched = await client.collab.link_get_by_id(link_id)
            assert fetched.get("id") == link_id
            assert fetched.get("name") == link_name

            updated = await client.collab.link_update(
                link_id,
                description="Updated description",
                doc_ids=["doc_target_1", "doc_target_2"],
                tags=["updated"],
            )
            assert updated.get("id") == link_id
        finally:
            await _teardown_operation(client, op_id)


# ──────────────────────────────────────────────────────────────────────────────
# Highlights
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_highlight_create_list_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a highlight, list it, then delete it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            highlight = await client.collab.highlight_create(
                operation_id=op_id,
                time_range=[1_000_000_000, 5_000_000_000],
                name=_unique("hl"),
            )
            assert highlight.get("id") is not None
            hl_id = highlight["id"]

            # List
            highlights = await client.collab.highlight_list(
                flt={"operation_ids": [op_id]}
            )
            assert any(h.get("id") == hl_id for h in highlights)

            # Delete
            result = await client.collab.highlight_delete(hl_id)
            assert result.get("id") == hl_id
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_highlight_get_by_id_and_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Create a highlight, fetch it by ID, then update it."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            highlight_name = _unique("hl")
            highlight = await client.collab.highlight_create(
                operation_id=op_id,
                time_range=[10, 20],
                name=highlight_name,
                description="Original highlight",
                tags=["initial"],
            )
            hl_id = highlight["id"]

            fetched = await client.collab.highlight_get_by_id(hl_id)
            assert fetched.get("id") == hl_id
            assert fetched.get("name") == highlight_name

            updated = await client.collab.highlight_update(
                hl_id,
                description="Updated highlight",
                time_range=[15, 25],
                tags=["updated"],
            )
            assert updated.get("id") == hl_id
        finally:
            await _teardown_operation(client, op_id)


# ──────────────────────────────────────────────────────────────────────────────
# Glyphs
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.integration
async def test_glyph_create_list_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Upload a minimal PNG glyph, list it, then delete it."""
    from gulp_sdk import GulpClient

    # Minimal 1x1 white PNG (valid PNG bytes)
    PNG_1X1 = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
        b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    import tempfile
    import pathlib
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(PNG_1X1)
        tmp_path = f.name

    try:
        async with GulpClient(gulp_base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)
            name = _unique("glyph")
            glyph = await client.collab.glyph_create(name=name, img_path=tmp_path)
            assert glyph.get("id") is not None
            glyph_id = glyph["id"]

            # List
            glyphs = await client.collab.glyph_list(flt={})
            assert any(g.get("id") == glyph_id for g in glyphs)

            # Delete
            result = await client.collab.glyph_delete(glyph_id)
            assert result.get("id") == glyph_id
    finally:
        pathlib.Path(tmp_path).unlink(missing_ok=True)


@pytest.mark.integration
async def test_glyph_get_by_id_and_update(gulp_base_url, gulp_test_user, gulp_test_password):
    """Upload a glyph, fetch it by ID, then update its metadata."""
    from gulp_sdk import GulpClient

    PNG_1X1 = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
        b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    import pathlib
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as file_handle:
        file_handle.write(PNG_1X1)
        tmp_path = file_handle.name

    try:
        async with GulpClient(gulp_base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)
            glyph = await client.collab.glyph_create(
                name=_unique("glyph"),
                img_path=tmp_path,
            )
            glyph_id = glyph["id"]

            fetched = await client.collab.glyph_get_by_id(glyph_id)
            assert fetched.get("id") == glyph_id

            updated = await client.collab.glyph_update(
                glyph_id,
                name=_unique("glyph_updated"),
            )
            assert updated.get("id") == glyph_id

            await client.collab.glyph_delete(glyph_id)
    finally:
        pathlib.Path(tmp_path).unlink(missing_ok=True)
