"""
last-line redaction of credentials in Glug's output.

Call sites are expected not to log credentials at all (see docs/logging.md,
"Writing style"), but a session token is a bearer credential and reaches the
log from places gULP does not control or cannot reasonably guard one by one:
exception messages (`token "<id>" not logged in`), third-party loggers routed
through Glug (the `websockets` library dumps every received frame at DEBUG,
login frame included), and call sites added later. Both formatters therefore
pass their final output through `redact()`, so the rule holds for every sink
(stdout, the --log-file copy, the JSON files and whatever ships them).
"""

import re

# value following a sensitive key: `token=<v>`, `token: <v>`, `"token":"<v>"`
# (also JSON-escaped, `\"token\":\"<v>\"`), `'token': '<v>'`, `token "<v>"`.
# The key may be a suffix (access_token, refresh_token). A bare `token <word>`
# with no `=`, `:` or quote is prose ("checking token permission") and is left
# alone, and so is any other identifier (ws_id, req_id, operation_id).
_SENSITIVE_VALUE = re.compile(
    r"""(?ix)
    (
        (?:token|password|pwd_hash)
        (?:
            (?:\\*["'])? \s* [:=] \s* (?:\\*["'])?   # key=value, key: value, "key":"value"
          | \s+ \\*["']                              # key "value"
        )
    )
    [^\s"'\\,;&}\x1b]{6,}
    """
)

_MASK = "***"


def redact(text: str) -> str:
    """
    masks the values of sensitive keys (token, password, pwd_hash) in a formatted log line.

    Args:
        text (str): a fully formatted log line (console text or one JSON document).

    Returns:
        str: the same text with each sensitive value replaced by "***"; values
            shorter than 6 characters (None, "***", ...) are left as they are.
    """
    return _SENSITIVE_VALUE.sub(lambda m: m.group(1) + _MASK, text)
