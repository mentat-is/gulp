"""
This module provides functions for retrieving the version of packages.
"""

try:
    from pkg_resources import get_distribution
except ImportError:  # pragma: no cover
    get_distribution = None

from . import __version__, commit_id


def pkg_version(name: str) -> str:
    """Returns the version of the given package."""
    if get_distribution:
        return get_distribution(name).version
    return "1.0.0"


def muty_version():
    """Returns the version of the muty package."""
    return f"{__version__} ({commit_id})" if commit_id else __version__