"""Check CLI formatting of the additional runtime UI metadata."""

from gulp_cli.commands.plugin import _plugin_row


def test_runtime_ui_metadata_survives_cli_formatting() -> None:
    """Listing a package preserves its API requirements and asset URLs unchanged."""
    package = {
        "id": "example", "filename": "Example.tsx", "version": "1.0.0",
        "requires_ui_api": {"major": 1, "minor": 0}, "build_hash": "a" * 64,
        "entry_url": "/ui_plugin_asset/example/hash/plugin.js", "type": ["login"],
    }
    row = _plugin_row(package)
    assert row == package
    assert row is not package
