"""
Glug (Gulp Log Unified Gateway) — gULP's structured JSON logging package.

`gulp.glug` itself only exports the core class and context helpers, which
have no dependency beyond the stdlib and are safe to import from anywhere,
early in process startup included. The Redis-backed dynamic reconfiguration
mechanism (`gulp.glug.reconfig`) is a separate import on purpose: it depends
on `gulp.api.redis_api`, which is only ready to use once Redis has been
initialized (main FastAPI startup / worker spawn) — importing it here at
package level would tie every `import gulp.glug` to that dependency chain
for no benefit.
"""

from gulp.glug.context import (
    client_ip_var,
    current_context,
    operation_id_var,
    plugin_var,
    req_id_var,
    reset_context,
    set_context,
    user_id_var,
)
from gulp.glug.core import Glug

__all__ = [
    "Glug",
    "client_ip_var",
    "current_context",
    "operation_id_var",
    "plugin_var",
    "req_id_var",
    "reset_context",
    "set_context",
    "user_id_var",
]
