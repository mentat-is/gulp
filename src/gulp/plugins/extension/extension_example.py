from typing import override

from fastapi import APIRouter
from gulp.api import rest
import gulp.api.rest_api as rest_api
import gulp.defs
import gulp.utils
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams
from typing import Annotated
import muty.file
import muty.jsend
import muty.list
import muty.log
import muty.os
import muty.string
import muty.uploadfile
from fastapi import APIRouter, Body, Header, Query
from fastapi.responses import JSONResponse
from muty.jsend import JSendException, JSendResponse

"""
# extension plugins

## loading

extension plugins are automatically loaded at startup from `PLUGIN_DIR/extension`.

## internals

- they may extend api through `rest_api.fastapi_app().add_api_route()`.
- they may implement run_command() to handle custom commands
- `they run in the MAIN process context`, this implies the following only works inside the MAIN process:
    - using `rest_api.aiopool()` to schedule tasks in the background.
    - using `collab_api.collab() to access the collaboration database and `elastic_api.elastic()` to access the elasticsearch database.
    - using `rest_api.process_executor()` to spawn processes and gather results.
        - operations from spawned processes will need their own in-process `collab` and `elastic` clients.
"""
class Plugin(PluginBase):
    def __init__(
        self,
        path: str,
        collab = None,
        elastic = None,
        **kwargs,
    ) -> None:
        super().__init__(path, collab, elastic, **kwargs)
        # add api routes for this plugin
        self._add_api_routes()
        self.logger().debug("%s extension plugin initialized, collab=%s, elastic=%s, aiopool=%s, executor=%s, fastapi_app=%s" % (self.name(), collab, elastic, rest_api.aiopool(), rest_api.process_executor(), rest_api.fastapi_app()))

    def _add_api_routes(self):
        # setup own router
        fa = rest_api.fastapi_app()
        
        # /example
        fa.add_api_route('/example', self.example_handler, 
                         methods=['GET'],
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
                                                "data": {
                                                    "result": "example"
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                         summary="just an example.")

    async def example_handler(self,
        token: Annotated[str, Header(description="API token")],
        req_id: Annotated[str, Query(description="Request ID")] = None,
    ) -> JSendResponse:
        req_id = gulp.utils.ensure_req_id(req_id)
        return JSONResponse(muty.jsend.success_jsend(req_id=req_id, data={'result': 'example'}))
    
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
