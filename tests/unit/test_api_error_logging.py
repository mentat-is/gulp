"""How a failed API request is logged: one readable line, level by who is at fault."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from muty.jsend import JSendException

from gulp.api import server_api
from gulp.api.collab.structs import MissingPermission, WrongUsernameOrPassword
from gulp.api.server_api import GulpServer


def _request(path: str = "/login", method: str = "POST") -> SimpleNamespace:
    return SimpleNamespace(
        url=SimpleNamespace(path=path),
        method=method,
        query_params="ws_id=abc",
        headers={"content-type": "application/json"},
        json=AsyncMock(return_value={"user_id": "admin", "password": "wrong-one"}),
        body=AsyncMock(return_value=b""),
    )


@pytest.fixture
def logged(monkeypatch: pytest.MonkeyPatch) -> list[tuple]:
    """records every (level, message, kwargs) the handler logs through Glug"""
    calls: list[tuple] = []
    recorder = SimpleNamespace(
        warning=lambda msg, **kw: calls.append(("warning", msg, kw)),
        error=lambda msg, **kw: calls.append(("error", msg, kw)),
    )
    monkeypatch.setattr(server_api.Glug, "get_instance", staticmethod(lambda: recorder))
    return calls


async def _handle(ex: Exception, request: SimpleNamespace = None):
    try:
        raise JSendException(req_id="req-1") from ex
    except JSendException as wrapped:
        return await GulpServer.get_instance()._bad_request_exception_handler(
            request or _request(), wrapped
        )


@pytest.mark.unit
async def test_client_error_is_one_warning_line_without_traceback(logged: list[tuple]):
    response = await _handle(WrongUsernameOrPassword("wrong password for user_id=admin"))

    assert response.status_code == 401
    assert len(logged) == 1
    level, message, kwargs = logged[0]
    assert level == "warning"
    assert message == (
        "POST /login -> 401 WrongUsernameOrPassword: wrong password for user_id=admin"
    )
    # no traceback for the caller's own mistake; the request goes to detail= (dev_mode only)
    assert "exc_info" not in kwargs
    assert kwargs["detail"]["path"] == "/login"
    assert kwargs["req_id"] == "req-1"
    # the client gets the error name and message, and nothing else
    body = json.loads(response.body)
    assert body["status"] == "error" and body["req_id"] == "req-1"
    assert body["data"] == {
        "__error": {
            "name": "WrongUsernameOrPassword",
            "msg": "wrong password for user_id=admin",
        }
    }


@pytest.mark.unit
async def test_missing_permission_is_a_warning(logged: list[tuple]):
    response = await _handle(MissingPermission("admin required"), _request("/user_list", "GET"))

    assert response.status_code == 403
    assert [(level, message) for level, message, _ in logged] == [
        ("warning", "GET /user_list -> 403 MissingPermission: admin required")
    ]


@pytest.mark.unit
async def test_server_error_is_an_error_with_traceback(logged: list[tuple]):
    failure = RuntimeError("opensearch went away")
    response = await _handle(failure, _request("/query_raw"))

    assert response.status_code == 500
    level, message, kwargs = logged[0]
    assert level == "error"
    assert message == "POST /query_raw -> 500 RuntimeError: opensearch went away"
    assert kwargs["exc_info"] is failure
    # the details are for the log: the client only learns where to look
    body = json.loads(response.body)
    assert body["data"] == {
        "__error": {
            "name": "InternalError",
            "msg": "internal error, see the server logs (req_id=req-1)",
        }
    }
    assert "opensearch" not in response.body.decode()


@pytest.mark.unit
async def test_the_request_and_the_traceback_never_go_back_to_the_client(
    logged: list[tuple],
):
    request = _request()
    request.headers = {"token": "a-session-token", "content-type": "application/json"}

    for failure in (MissingPermission("admin required"), RuntimeError("boom")):
        response = await _handle(failure, request)
        text = response.body.decode()
        assert "a-session-token" not in text and "wrong-one" not in text
        assert "Traceback" not in text and "trace" not in text
        assert "request" not in json.loads(text)["data"]
    # they are still available to whoever reads the log (detail=, dev_mode only)
    assert logged[0][2]["detail"]["headers"]["token"] == "a-session-token"


@pytest.mark.unit
async def test_validation_error_names_the_parameter_not_its_value(logged: list[tuple]):
    from fastapi.exceptions import RequestValidationError

    failure = RequestValidationError(
        [
            {
                "type": "string_pattern_mismatch",
                "loc": ("body", "password"),
                "msg": "String should match pattern '^.{8,64}$'",
                "input": "Sup3rSecret",
                "ctx": {"pattern": "^.{8,64}$"},
            },
            {"type": "missing", "loc": ("query", "user_id"), "msg": "Field required", "input": None},
        ]
    )
    response = await GulpServer.get_instance()._bad_request_exception_handler(
        _request("/user_create"), failure
    )

    assert response.status_code == 400
    error = json.loads(response.body)["data"]["__error"]
    assert error == {
        "name": "RequestValidationError",
        "msg": "body.password: String should match pattern '^.{8,64}$'; "
        "query.user_id: Field required",
    }
    # neither the client nor the log line gets the rejected value back
    assert "Sup3rSecret" not in response.body.decode()
    assert "Sup3rSecret" not in logged[0][1]


@pytest.mark.unit
async def test_polled_request_not_found_stays_silent(logged: list[tuple]):
    from gulp.structs import ObjectNotFound

    response = await _handle(ObjectNotFound("no such request"), _request("/request_get_by_id", "GET"))

    assert response.status_code == 404
    assert logged == []
