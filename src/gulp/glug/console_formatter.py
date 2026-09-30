"""Human-readable console formatter for Glug log records.

Renders the same `LogRecord` `GlugJsonFormatter` turns into a JSON line, but
as a short, scannable line for a human reading stdout: this is the "console
encoder" half of the console-vs-JSON-encoder split (see
plans/log-revolution.md, "Output su stdout vs file"). No `coloredlogs`
dependency (that was rejected for `MutyLogger` too, see the same doc):
color is a handful of hand-rolled ANSI codes, applied only when the handler
was set up against a real terminal.
"""

import logging
import traceback

import orjson

from gulp.glug.formatter import _dotted_module_name, _relative_module_path

_RESET = "\x1b[0m"
_DIM = "\x1b[2m"
_HINT = "\x1b[36m"  # cyan
_LEVEL_COLORS = {
    logging.DEBUG: "\x1b[2m",  # dim
    logging.INFO: "\x1b[32m",  # green
    logging.WARNING: "\x1b[33m",  # yellow
    logging.ERROR: "\x1b[31m",  # red
    logging.CRITICAL: "\x1b[1;31m",  # bold red
}


class GlugConsoleFormatter(logging.Formatter):
    """
    formats a LogRecord as one short human-readable line (plus indented
    continuation lines for an optional dev_mode-only detail structure,
    hint, and/or exception traceback).

    Args:
        use_color (bool): whether to wrap level/hint/traceback in ANSI color
            codes. Callers should pass `sys.stdout.isatty()` so redirected or
            piped output (CI logs, `docker logs` without `-t`, ...) stays
            plain text instead of littered with escape codes.
    """

    def __init__(self, use_color: bool = True) -> None:
        super().__init__()
        self._use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        module_path = _relative_module_path(record.pathname)
        module_name = _dotted_module_name(module_path)
        # full date+time (not just HH:MM:SS): losing the date made stdout
        # unreadable across a midnight rollover or when scrolling back through
        # more than a few minutes of history — MutyLogger always kept it.
        time_str = f"{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}.{int(record.msecs):03d}"

        color = self._use_color and _LEVEL_COLORS.get(record.levelno, "") or ""
        reset = _RESET if color else ""

        # process_role/pid are deliberately left out of this stdout line — this
        # formatter is for a human glancing at the console, and both fields are
        # already in the JSON file (GlugJsonFormatter, "process_role"/"pid")
        # for whatever actually parses logs. Keeping stdout terse was an
        # explicit tradeoff the user made after seeing proc=/task= added and
        # then trying it live. The asyncio task name was re-added later, on
        # its own — shown only when set (e.g. not during sync startup code
        # outside any task), so it stays out of the way otherwise.
        dim = _DIM if self._use_color else ""
        dim_reset = _RESET if dim else ""
        task_name = getattr(record, "async_task_name", None)
        task_suffix = f" task={task_name}" if task_name else ""
        header = (
            f"{time_str} {color}{record.levelname:<8}{reset} "
            f"{module_name}:{record.funcName}:{record.lineno}{task_suffix}"
        )

        context = getattr(record, "context", {}) or {}
        ctx_str = " ".join(f"{k}={v}" for k, v in context.items() if v is not None)
        ctx_suffix = f"  {{{ctx_str}}}" if ctx_str else ""

        # message colored the same as its level (DEBUG dim/gray, INFO green,
        # WARNING yellow, ERROR/CRITICAL red) — not just the level tag — so
        # severity is readable at a glance scanning down a column of lines.
        lines = [f"{header}  {color}{record.getMessage()}{ctx_suffix}{reset}"]

        # dev_mode-only, already dropped by Glug._emit() when dev_mode is off (see core.py) —
        # pretty-printed multi-line so a developer can see the whole structure at a glance,
        # unlike the one-line-summary-plus-truncated-repr style MutyLogger used.
        detail = getattr(record, "detail", None)
        if detail is not None:
            detail_text = orjson.dumps(
                detail, option=orjson.OPT_INDENT_2, default=str
            ).decode()
            lines.append(f"  {dim}detail:{dim_reset}")
            lines.extend(f"  {dim}{line}{dim_reset}" for line in detail_text.split("\n"))

        hint = getattr(record, "hint", None)
        if hint:
            hint_color = _HINT if self._use_color else ""
            hint_reset = _RESET if hint_color else ""
            lines.append(f"  {hint_color}→ hint: {hint}{hint_reset}")

        if record.exc_info:
            tb_text = "".join(traceback.format_exception(*record.exc_info)).rstrip("\n")
            lines.extend(f"  {dim}{line}{dim_reset}" for line in tb_text.split("\n"))

        return "\n".join(lines)
