"""
# extension plugins

## loading

extension plugins are automatically loaded at startup from `PLUGIN_DIR/extension`.

## internals

- they may extend api through `rest_api.fastapi_app().add_api_route()`.
- they may implement run_command() to handle custom commands
- `their init runs in the MAIN process context`
"""

from typing import Annotated, override

import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendResponse

import gulp.defs
import gulp.utils
from gulp.api import rest_api
from gulp.plugin import PluginBase
from gulp.utils import logger

class Plugin(PluginBase):
    def __init__(
        self,
        path: str,
        **kwargs,
    ) -> None:
        super().__init__(path, **kwargs)
        # add api routes for this plugin
        self._add_api_routes()
        logger().debug(
            "%s extension plugin initialized, aiopool=%s, executor=%s, fastapi_app=%s"
            % (
                self.name(),
                rest_api.aiopool(),
                rest_api.process_executor(),
                rest_api.fastapi_app(),
            )
        )

    def _add_api_routes(self):
        # setup own router
        fa = rest_api.fastapi_app()

        # /example
        fa.add_api_route(
            "/example",
            self.example_handler,
            methods=["GET"],
            response_model=JSendResponse,
            response_model_exclude_none=True,
            tags=["extensions"],
            responses={
                200: {
                    "content": {
                        "application/json": {
                            "example": {
                                "status": "success",
                                "timestamp_msec": 1701278479259,
                                "req_id": "903546ff-c01e-4875-a585-d7fa34a0d237",
                                "data": {"result": "example"},
                            }
                        }
                    }
                }
            },
            summary="just an example.",
        )

    async def example_handler(
        self,
        token: Annotated[str, Header(description="API token")],
        req_id: Annotated[str, Query(description="Request ID")] = None,
    ) -> JSendResponse:
        req_id = gulp.utils.ensure_req_id(req_id)
        return JSONResponse(
            muty.jsend.success_jsend(req_id=req_id, data={"result": "example"})
        )

    def desc(self) -> str:
        return "Extension plugin which adds an API."

    @override
    def internal(self) -> bool:
        return True

    def type(self) -> gulp.defs.GulpPluginType:
        return gulp.defs.GulpPluginType.EXTENSION

    def name(self) -> str:
        return "sample_ext"

    def version(self) -> str:
        return "1.0"
