#!/usr/bin/env python3
import asyncio
import json
import os
import orjson
import pytest
import pytest_asyncio
import websockets

from gulp.api.ws_api import GulpWsAuthPacket
from gulp_client.common import (
    GulpAPICommon,
    _ensure_test_operation,
    _cleanup_test_operation,
)
from gulp_client.user import GulpAPIUser
from gulp_client.test_values import (
    TEST_HOST,
    TEST_INDEX,
    TEST_OPERATION_ID,
    TEST_REQ_ID,
    TEST_WS_ID,
)

from muty.log import MutyLogger

_DATA: list[dict] = [
    {
        "timestamp": "2025-11-17T14:10:05.120Z",
        "agent.type": "iis",
        "event.original": "2025-11-17 14:10:05 10.0.0.5 GET /api/users.php user_id=101' 80 - 192.168.50.15 Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64) 500 0 0 15",
        "gulp.source_id": "W3SVC1-u_ex251117.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:05.145Z",
        "agent.type": "php-error-log",
        "event.original": "[17-Nov-2025 14:10:05 UTC] PHP Fatal error: Uncaught mysqli_sql_exception: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ''101''' at line 1 in C:\\inetpub\\wwwroot\\api\\users.php:25",
        "gulp.source_id": "php_errors.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:08.200Z",
        "agent.type": "iis",
        "event.original": "2025-11-17 14:10:08 10.0.0.5 GET /api/users.php user_id=101+OR+1=1 80 - 192.168.50.15 Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64) 500 0 0 20",
        "gulp.source_id": "W3SVC1-u_ex251117.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:08.230Z",
        "agent.type": "php-error-log",
        "event.original": "[17-Nov-2025 14:10:08 UTC] PHP Fatal error: Uncaught mysqli_sql_exception: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'OR 1=1' at line 1 in C:\\inetpub\\wwwroot\\api\\users.php:25",
        "gulp.source_id": "php_errors.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:12.550Z",
        "agent.type": "iis",
        "event.original": "2025-11-17 14:10:12 10.0.0.5 GET /api/users.php user_id=101+UNION+SELECT+1,2,3-- 80 - 192.168.50.15 Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64) 500 0 0 18",
        "gulp.source_id": "W3SVC1-u_ex251117.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:12.580Z",
        "agent.type": "php-error-log",
        "event.original": "[17-Nov-2025 14:10:12 UTC] PHP Warning: mysqli_query(): The used SELECT statements have a different number of columns in C:\\inetpub\\wwwroot\\api\\users.php on line 25",
        "gulp.source_id": "php_errors.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:18.100Z",
        "agent.type": "iis",
        "event.original": "2025-11-17 14:10:18 10.0.0.5 GET /api/users.php user_id=101;+DROP+TABLE+users;-- 80 - 192.168.50.15 Mozilla/5.0+(Windows+NT+10.0;+Win64;+x64) 500 0 0 22",
        "gulp.source_id": "W3SVC1-u_ex251117.log",
        "gulp.context_id": "APP-SRV-01",
    },
    {
        "timestamp": "2025-11-17T14:10:18.135Z",
        "agent.type": "php-error-log",
        "event.original": "[17-Nov-2025 14:10:18 UTC] PHP Fatal error: Uncaught mysqli_sql_exception: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '; DROP TABLE users;--' at line 1 in C:\\inetpub\\wwwroot\\api\\users.php:25",
        "gulp.source_id": "php_errors.log",
        "gulp.context_id": "APP-SRV-01",
    },
]


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize environment
    """
    GulpAPICommon.get_instance().init(
        host=TEST_HOST, ws_id=TEST_WS_ID, req_id=TEST_REQ_ID, index=TEST_INDEX
    )
    if os.getenv("SKIP_RESET", "0") == 1:
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()


@pytest.mark.asyncio
async def test_ai_assistant():

    edit_token = await GulpAPIUser.login("editor", "editor", TEST_REQ_ID)
    assert edit_token
    _, host = TEST_HOST.split("://")
    ws_url = f"ws://{host}/ws"
    test_completed = False

    params = {
        "operation_id": TEST_OPERATION_ID,
        "ws_id": TEST_WS_ID,
        "req_id": TEST_REQ_ID,
    }

    body = _DATA
    async with websockets.connect(ws_url) as ws:
        # connect websocket
        p: GulpWsAuthPacket = GulpWsAuthPacket(token=edit_token, ws_id=TEST_WS_ID)
        report = ""
        await ws.send(p.model_dump_json(exclude_none=True))
        try:
            while True:
                response = await ws.recv()
                data = json.loads(response)
                payload = data.get("payload", {})
                if data["type"] == "ws_connected":
                    await GulpAPICommon.get_instance().make_request(
                        method="POST",
                        endpoint="/get_ai_hint",
                        params=params,
                        token=edit_token,
                        body=body,
                        expected_status=200,
                    )
                if data["type"] == "ai_assistant_stream":
                    report += payload
                if data["type"] == "ai_assistant_done":
                    # test completed
                    test_completed = True
                    break
        except websockets.exceptions.ConnectionClosed as ex:
            MutyLogger.get_instance().exception(ex)
        MutyLogger.get_instance().warning(f"\n\n----- RESPONSE -----\n\n{report}")
    assert test_completed
    MutyLogger.get_instance().info(test_ai_assistant.__name__ + " succeeded!")
