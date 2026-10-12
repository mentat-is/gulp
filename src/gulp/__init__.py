"""gulp package."""

try:
    from ._version import version as __version__, commit_id
except ImportError:
    __version__ = "0.0.0"
    commit_id: str | None = None


def short_commit_id(revision: str | None) -> str | None:
    """Return a normalized Git revision with six hexadecimal characters."""
    if not revision:
        return None

    bare_revision = revision.removeprefix("g")
    return f"g{bare_revision[:6]}"


def format_version(
    version: str | None = None,
    revision: str | None = None,
    prefix: str = "",
) -> str:
    """Format a product version while preserving the existing string API."""
    resolved_version = version if version is not None else __version__
    resolved_revision = revision if revision is not None else commit_id
    short_revision = short_commit_id(resolved_revision)
    if short_revision:
        return f"{prefix}{resolved_version} ({short_revision})"
    return f"{prefix}{resolved_version}"


_short_commit = short_commit_id(commit_id)
if _short_commit:
    __version_full__ = f"{__version__}+{_short_commit}"
else:
    __version_full__ = __version__

__all__ = [
    "__version__",
    "__version_full__",
    "commit_id",
    "format_version",
    "short_commit_id",
]
