import pytest

from gulp.__main__ import _pid_from_pidfile_path


@pytest.mark.unit
def test_pid_from_pidfile_path_strips_prefix_and_suffix():
    assert _pid_from_pidfile_path("/home/vscode/.config/gulp/gulp35329.pid") == 35329
