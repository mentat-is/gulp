import argparse
import re

from gulp.__main__ import _colored_banner, _describe_args


def _parser() -> argparse.ArgumentParser:
    """a small parser mirroring the option kinds gulp's main() uses"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", nargs=1, default=None)
    parser.add_argument("--log-file", nargs=1, default=None)
    parser.add_argument("--reset-collab", action="store_true", default=False)
    parser.add_argument("--create", nargs=1)
    return parser


def test_describe_args_marks_defaults():
    """options not given are marked as default"""
    assert _describe_args(_parser(), []) == {
        "log_level": "None (default)",
        "log_file": "None (default)",
        "reset_collab": "False (default)",
        "create": "None (default)",
    }


def test_describe_args_explicit_values():
    """given options are not marked, even when equal to the default"""
    described = _describe_args(
        _parser(),
        ["--log-level", "info", "--log-file", "/tmp/g.log", "--reset-collab", "--create", "op"],
    )
    assert described == {
        "log_level": "'info'",
        "log_file": "'/tmp/g.log'",
        "reset_collab": "True",
        "create": "'op'",
    }


def test_colored_banner_plain_without_color():
    """no ANSI codes when color is off (e.g. stdout redirected)"""
    assert _colored_banner("BANNER", "v1", False) == "BANNER\nv1"


def test_colored_banner_distinct_colors_and_bold_version():
    """banner and version get two different colors, the version is bold"""
    for _ in range(50):
        out = _colored_banner("BANNER", "v1", True)
        m = re.fullmatch(
            r"\x1b\[(\d+)mBANNER\x1b\[0m\n\x1b\[1;(\d+)mv1\x1b\[0m", out
        )
        assert m is not None
        assert m.group(1) != m.group(2)
