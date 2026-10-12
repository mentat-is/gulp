"""Unit tests for gULP version formatting."""

from gulp import format_version, short_commit_id


def test_short_commit_id_normalizes_git_prefix() -> None:
    """The displayed revision should contain one prefix and six hex digits."""
    assert short_commit_id("gabcdef123456") == "gabcdef"
    assert short_commit_id("abcdef123456") == "gabcdef"


def test_short_commit_id_accepts_missing_revision() -> None:
    """Source trees without generated metadata should omit the revision."""
    assert short_commit_id(None) is None
    assert short_commit_id("") is None


def test_format_version_preserves_string_contract() -> None:
    """CLI and API version strings should retain their existing prefix shape."""
    assert (
        format_version("2.0.0", "gabcdef123456", prefix="gulp_")
        == "gulp_2.0.0 (gabcdef)"
    )


def test_format_version_without_revision() -> None:
    """A missing revision should still produce a usable version string."""
    assert format_version("0.0.0", "", prefix="gulp_") == "gulp_0.0.0"
