"""Unit coverage for gulp.glug (isolated logic, no external services)."""

import logging
import os
import socket
from pathlib import Path
from uuid import uuid4

import orjson
import pytest

from gulp.glug.context import reset_context, set_context
from gulp.glug.core import Glug
from gulp.glug.formatter import _dotted_module_name, _relative_module_path


def _fresh_glug(tmp_path: Path, **overrides) -> Glug:
    """creates a brand new Glug singleton backed by a unique logger name, for test isolation"""
    kwargs = {
        "name": f"test-glug-{uuid4().hex}",
        "log_dir": str(tmp_path),
        "level": logging.INFO,
        "dev_mode": False,
        "process_role": "main",
        "reconfigure": True,
    }
    kwargs.update(overrides)
    return Glug.get_instance(**kwargs)


def _fresh_console_glug(**overrides) -> Glug:
    """creates a brand new Glug singleton in console (stdout) mode, for test isolation"""
    kwargs = {
        "name": f"test-glug-console-{uuid4().hex}",
        "level": logging.INFO,
        "dev_mode": False,
        "process_role": "main",
        "reconfigure": True,
    }
    kwargs.update(overrides)
    return Glug.get_instance(**kwargs)


def _read_records(tmp_path: Path) -> list[dict]:
    """reads every JSON record written under tmp_path/<hostname>/*.log"""
    log_subdir = tmp_path / socket.gethostname()
    records = []
    for log_file in sorted(log_subdir.glob("*.log")):
        for line in log_file.read_text().splitlines():
            if line:
                records.append(orjson.loads(line))
    return records


@pytest.mark.unit
def test_record_schema_and_main_filename(tmp_path: Path):
    glug = _fresh_glug(tmp_path, process_role="main")
    glug.info("hello world", plugin="win_evtx")

    records = _read_records(tmp_path)
    assert len(records) == 1
    record = records[0]

    assert record["level"] == "INFO"
    assert record["message"] == "hello world"
    assert record["process_role"] == "main"
    assert record["hostname"] == socket.gethostname()
    assert record["pid"] == os.getpid()
    assert record["dev_mode"] is False
    assert record["exception"] is None
    assert record["context"]["plugin"] == "win_evtx"
    assert record["context"]["req_id"] is None
    assert record["module_name"] == "tests.unit.test_glug"
    assert record["module_path"] == os.path.join("tests", "unit", "test_glug.py")

    assert (tmp_path / socket.gethostname() / "gulp-main.log").exists()


@pytest.mark.unit
def test_file_mode_also_writes_to_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture
):
    # log_dir adds the JSON file on top of stdout, never instead of it (mirrors MutyLogger's
    # own stdout-plus-optional-file/syslog behavior) — see plans/log-revolution.md, decision 6.
    glug = _fresh_glug(tmp_path)
    glug.info("both sinks should see this")

    assert len(_read_records(tmp_path)) == 1
    assert "both sinks should see this" in capsys.readouterr().out


@pytest.mark.unit
def test_worker_process_role_uses_pid_filename(tmp_path: Path):
    _fresh_glug(tmp_path, process_role="worker")

    expected = tmp_path / socket.gethostname() / f"gulp-worker-{os.getpid()}.log"
    assert expected.exists()


@pytest.mark.unit
def test_dev_mode_forces_debug_level_and_is_flagged_on_record(tmp_path: Path):
    glug = _fresh_glug(tmp_path, level=logging.WARNING, dev_mode=True)

    assert glug.is_dev_mode() is True
    assert glug._logger.getEffectiveLevel() == logging.DEBUG

    glug.debug("verbose detail")
    records = _read_records(tmp_path)
    assert records[0]["dev_mode"] is True
    assert records[0]["level"] == "DEBUG"


@pytest.mark.unit
def test_explicit_context_kwarg_overrides_contextvar(tmp_path: Path):
    glug = _fresh_glug(tmp_path)
    tokens = set_context(plugin="from-contextvar", req_id="req-1")
    try:
        glug.info("msg", plugin="from-call-site")
    finally:
        reset_context(tokens)

    record = _read_records(tmp_path)[0]
    assert record["context"]["plugin"] == "from-call-site"
    assert record["context"]["req_id"] == "req-1"


@pytest.mark.unit
def test_exception_produces_structured_object(tmp_path: Path):
    glug = _fresh_glug(tmp_path)
    try:
        raise ValueError("boom")
    except ValueError:
        glug.exception("something failed")

    record = _read_records(tmp_path)[0]
    assert record["level"] == "ERROR"
    assert record["exception"]["type"] == "ValueError"
    assert record["exception"]["message"] == "boom"
    assert "ValueError: boom" in record["exception"]["stack"]


@pytest.mark.unit
def test_exc_info_kwarg_on_non_error_level(tmp_path: Path):
    glug = _fresh_glug(tmp_path)
    try:
        raise RuntimeError("degraded")
    except RuntimeError:
        glug.warning("continuing in degraded mode", exc_info=True)

    record = _read_records(tmp_path)[0]
    assert record["level"] == "WARNING"
    assert record["exception"]["type"] == "RuntimeError"


@pytest.mark.unit
def test_apply_reconfig_changes_level_without_rebuilding_handler(tmp_path: Path):
    glug = _fresh_glug(tmp_path, level=logging.INFO, dev_mode=False)
    handlers_before = list(glug._logger.handlers)

    Glug.apply_reconfig(level=logging.ERROR)
    assert glug._logger.getEffectiveLevel() == logging.ERROR
    assert glug._logger.handlers == handlers_before

    Glug.apply_reconfig(dev_mode=True)
    assert glug.is_dev_mode() is True
    assert glug._logger.getEffectiveLevel() == logging.DEBUG
    assert glug._logger.handlers == handlers_before


@pytest.mark.unit
def test_apply_reconfig_before_get_instance_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(Glug, "_instance", None)
    with pytest.raises(RuntimeError):
        Glug.apply_reconfig(level=logging.DEBUG)


@pytest.mark.unit
def test_unopenable_log_file_raises_and_reports_to_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture
):
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("i am a file, not a directory")

    with pytest.raises(OSError):
        Glug.get_instance(
            name=f"test-glug-{uuid4().hex}",
            log_dir=str(blocked),
            reconfigure=True,
        )

    assert "glug: cannot open log file" in capsys.readouterr().out


@pytest.mark.unit
def test_bare_get_instance_without_bootstrap_defaults_to_console_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
):
    # simulates a migrated call site calling Glug.get_instance() with no args, in a process
    # that never had --log-dir (glug bootstrap never ran for real): this is glug's zero-config
    # default (human-readable console output on stdout), not silence.
    monkeypatch.setattr(Glug, "_instance", None)

    glug = Glug.get_instance()
    assert glug.is_dev_mode() is False
    assert glug._logger is not None

    glug.info("hello from a bare instance", plugin="x")
    out = capsys.readouterr().out
    assert "hello from a bare instance" in out
    assert "INFO" in out

    # exception/warning with exc_info still work without a real logger having been bootstrapped
    try:
        raise ValueError("boom")
    except ValueError:
        glug.exception("noop")
    assert "boom" in capsys.readouterr().out

    # apply_reconfig still applies to the console-mode instance
    Glug.apply_reconfig(level=logging.DEBUG, dev_mode=True)
    assert glug.is_dev_mode() is True


@pytest.mark.unit
def test_real_bootstrap_overrides_earlier_console_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(Glug, "_instance", None)

    # a migrated call site fetches the console-mode default before real bootstrap runs
    early = Glug.get_instance()
    assert early._logger is not None

    # real bootstrap (server_api.py/process.py) always passes reconfigure=True and its own
    # log_dir, so it must win regardless of what was cached before it ran
    real = _fresh_glug(tmp_path, process_role="main")
    assert real is Glug.get_instance()
    assert real._logger is not None

    real.info("now it logs to file")
    assert len(_read_records(tmp_path)) == 1


@pytest.mark.unit
def test_console_output_contains_level_module_and_message(
    capsys: pytest.CaptureFixture,
):
    glug = _fresh_console_glug()
    glug.info("chunk ingested", plugin="win_evtx")

    out = capsys.readouterr().out
    assert "INFO" in out
    assert "chunk ingested" in out
    assert "test_glug" in out  # module name derived from this call site
    assert "plugin=win_evtx" in out


@pytest.mark.unit
def test_console_hint_rendered_only_when_given(capsys: pytest.CaptureFixture):
    glug = _fresh_console_glug()

    glug.warning("disk usage high")
    assert "hint" not in capsys.readouterr().out

    glug.error("bulk ingestion failed", hint="check that OpenSearch is reachable")
    assert "hint: check that OpenSearch is reachable" in capsys.readouterr().out


@pytest.mark.unit
def test_console_formatter_color_only_when_requested():
    from gulp.glug.console_formatter import GlugConsoleFormatter

    record = logging.LogRecord(
        name="x",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="boom",
        args=(),
        exc_info=None,
    )
    colored = GlugConsoleFormatter(use_color=True).format(record)
    plain = GlugConsoleFormatter(use_color=False).format(record)
    assert "\x1b[" in colored
    assert "\x1b[" not in plain


@pytest.mark.unit
def test_hint_field_in_json_record(tmp_path: Path):
    glug = _fresh_glug(tmp_path)
    glug.warning("no hint here")
    glug.error("with hint", hint="try again later")

    records = _read_records(tmp_path)
    assert records[0]["hint"] is None
    assert records[1]["hint"] == "try again later"


@pytest.mark.unit
def test_detail_dropped_outside_dev_mode(tmp_path: Path, capsys: pytest.CaptureFixture):
    # dev_mode=False (the default): detail must not reach either the console or the JSON
    # file, even though the call site passed it — Glug._emit() drops it before formatting.
    glug = _fresh_glug(tmp_path, dev_mode=False, level=logging.DEBUG)
    glug.debug("created operation 'test_operation'", detail={"secret_field": "should-not-leak"})

    assert "should-not-leak" not in capsys.readouterr().out
    record = _read_records(tmp_path)[0]
    assert record["detail"] is None


@pytest.mark.unit
def test_detail_shown_pretty_multiline_in_dev_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture
):
    glug = _fresh_glug(tmp_path, dev_mode=True)
    glug.debug(
        "created operation 'test_operation'",
        detail={"id": "test_operation", "granted_user_ids": ["admin"]},
    )

    out = capsys.readouterr().out
    # multi-line: the structure is pretty-printed, not crammed on the message line
    assert "\n" in out
    assert '"id": "test_operation"' in out
    assert '"granted_user_ids"' in out

    record = _read_records(tmp_path)[0]
    assert record["detail"] == {"id": "test_operation", "granted_user_ids": ["admin"]}


@pytest.mark.unit
def test_detail_absent_when_not_passed_even_in_dev_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture
):
    glug = _fresh_glug(tmp_path, dev_mode=True)
    glug.debug("no detail here")

    assert "detail:" not in capsys.readouterr().out
    assert _read_records(tmp_path)[0]["detail"] is None


@pytest.mark.unit
def test_relative_module_path_and_dotted_name():
    abs_path = os.path.join(os.getcwd(), "src", "gulp", "plugin.py")
    relative = _relative_module_path(abs_path)
    assert relative == os.path.join("gulp", "plugin.py")
    assert _dotted_module_name(relative) == "gulp.plugin"


@pytest.mark.unit
def test_relative_module_path_falls_back_to_original_when_no_match():
    unmatched = "/definitely/outside/any/sys/path/entry.py"
    assert _relative_module_path(unmatched) == unmatched
