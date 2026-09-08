import orjson
import pytest
from muty.jsend import JSendException
from starlette.requests import Request

from gulp.api.server_api import GulpServer
from gulp.structs import PaginationSnapshotExpired


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/query_raw_paginate",
            "raw_path": b"/query_raw_paginate",
            "query_string": b"operation_id=op-1",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        }
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expired_pagination_snapshot_maps_to_http_410() -> None:
    cause = PaginationSnapshotExpired("point in time expired")
    wrapped = JSendException(req_id="req-1")
    wrapped.__cause__ = cause

    response = await GulpServer()._bad_request_exception_handler(_request(), wrapped)
    payload = orjson.loads(response.body)

    assert response.status_code == 410
    assert payload["req_id"] == "req-1"
    assert payload["data"]["__error"]["name"] == "PaginationSnapshotExpired"
