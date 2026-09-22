"""
context propagation for Glug log records.

Values are set via `contextvars.ContextVar` so they follow async tasks
correctly (unlike thread-locals) and are automatically merged into every log
record emitted while they are set, without having to pass them explicitly at
every call site. An explicit keyword passed at the call site (e.g.
`Glug.get_instance().debug(msg, plugin="x")`) always overrides the
contextvar-derived value for the same field.
"""

from contextvars import ContextVar, Token

req_id_var: ContextVar[str | None] = ContextVar("req_id_var", default=None)
operation_id_var: ContextVar[str | None] = ContextVar("operation_id_var", default=None)
user_id_var: ContextVar[str | None] = ContextVar("user_id_var", default=None)
plugin_var: ContextVar[str | None] = ContextVar("plugin_var", default=None)
client_ip_var: ContextVar[str | None] = ContextVar("client_ip_var", default=None)

# maps a context field name, as it appears in the "context" object of a log
# record, to the contextvar holding it
_CONTEXT_VARS: dict[str, ContextVar] = {
    "req_id": req_id_var,
    "operation_id": operation_id_var,
    "user_id": user_id_var,
    "plugin": plugin_var,
    "client_ip": client_ip_var,
}


def current_context() -> dict[str, str | None]:
    """
    snapshot of all glug contextvars currently set, for the running task/thread

    Returns:
        dict[str, str | None]: field name -> current value (None if unset)
    """
    return {name: var.get() for name, var in _CONTEXT_VARS.items()}


def set_context(**fields: str | None) -> dict[str, Token]:
    """
    sets one or more glug contextvars for the current task/thread

    Args:
        **fields: any of req_id, operation_id, user_id, plugin, client_ip

    Returns:
        dict[str, Token]: tokens to pass to reset_context() to restore the previous values

    Throws:
        KeyError: if a field name is not a known context field
    """
    tokens: dict[str, Token] = {}
    for name, value in fields.items():
        tokens[name] = _CONTEXT_VARS[name].set(value)
    return tokens


def reset_context(tokens: dict[str, Token]) -> None:
    """
    restores glug contextvars to the values they had before the matching set_context() call

    Args:
        tokens (dict[str, Token]): tokens returned by set_context()
    """
    for name, token in tokens.items():
        _CONTEXT_VARS[name].reset(token)
