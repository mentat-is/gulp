"""gulp package."""

try:
    from ._version import version as __version__, commit_id
except ImportError:
    __version__ = "0.0.0"
    commit_id = None

if commit_id:
    __version_full__ = f"{__version__}+{commit_id}"
else:
    __version_full__ = __version__

__all__ = ["__version__", "__version_full__"]
