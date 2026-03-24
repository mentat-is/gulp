import asyncio
import json
import os
import uuid

import orjson
import pytest
import pytest_asyncio
import redis.asyncio as redis
import websockets
from gulp_client.common import _cleanup_test_operation, _ensure_test_operation
from gulp_client.test_values import TEST_HOST
from gulp_client.user import GulpAPIUser

from gulp.api.ws_api import (
    WSDATA_CLIENT_DATA,
    GulpClientDataPacket,
    GulpWsAuthPacket,
    GulpWsData,
    GulpRedisChannel,
)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def _setup():
    """
    this is called before any test, to initialize the environment
    """
    if os.getenv("SKIP_RESET") == "1":
        await _cleanup_test_operation()
    else:
        await _ensure_test_operation()

@pytest.mark.asyncio
async def test_ws_client_data_end_to_end():
    # assumes gulp server is running and accessible via TEST_HOST
    _, host = TEST_HOST.split('://')
    ws_url = f"ws://{host}/ws_client_data"

    # login to get token
    token = await GulpAPIUser.login('editor', 'editor')
    assert token
    token_2 = await GulpAPIUser.login('guest', 'guest')
    assert token_2

    ws_id_a = f"test-ws-a-{uuid.uuid4()}"
    ws_id_b = f"test-ws-b-{uuid.uuid4()}"

    async with websockets.connect(ws_url) as wa, websockets.connect(ws_url) as wb:
        # authenticate both sockets
        auth_a = GulpWsAuthPacket(token=token, ws_id=ws_id_a)
        auth_b = GulpWsAuthPacket(token=token_2, ws_id=ws_id_b)
        await wa.send(auth_a.model_dump_json(exclude_none=True))
        await wb.send(auth_b.model_dump_json(exclude_none=True))

        # wait for connected ack from both
        async def wait_connected(ws):
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                if data.get('type') == 'ws_connected':
                    return data

        await asyncio.gather(wait_connected(wa), wait_connected(wb))

        # send client_data from A
        client_payload = {"hello": "from_a"}
        p = GulpClientDataPacket(data=client_payload, operation_id='test_operation')
        await wa.send(p.model_dump_json(exclude_none=True))

        # B should receive a client_data message
        received = None
        try:
            # allow some time for routing
            msg = await asyncio.wait_for(wb.recv(), timeout=5.0)
            data = json.loads(msg)
            if data.get('type') == WSDATA_CLIENT_DATA:
                received = data
        except asyncio.TimeoutError:
            pytest.fail("client_data not delivered to peer websocket")

        assert received is not None
        assert received.get('payload', {}).get('data') == client_payload

        # now test cross-instance publish via Redis: publish a GulpWsData-like message
        redis_url = 'redis://:Gulp1234!@localhost:6379/0'
        r = redis.from_url(redis_url)

        # add a small delay to ensure websockets are fully registered
        await asyncio.sleep(0.5)

        wsd = GulpWsData(
            timestamp=1,
            type=WSDATA_CLIENT_DATA,
            ws_id=ws_id_b,
            user_id='test',
            operation_id='test_operation',
            payload={"data": {"hello": "from_redis"}},
        )
        msg = wsd.model_dump(exclude_none=True)
        msg['__server_id__'] = 'remote-test-server'
        msg['__sender_ws_id__'] = 'remote-sender'
        # explicit channel metadata required by new pubsub routing (Phase 4)
        msg['__channel__'] = GulpRedisChannel.CLIENT_DATA.value

        publish_result = await r.publish('gulpredis:client_data', orjson.dumps(msg))
        assert isinstance(publish_result, int)
        assert publish_result > 0

        # B should receive the redis-published message
        msg2 = None
        try:
            raw = await asyncio.wait_for(wb.recv(), timeout=5.0)
            data2 = json.loads(raw)
            if data2.get('type') == WSDATA_CLIENT_DATA and data2.get('payload', {}).get('data', {}).get('hello') == 'from_redis':
                msg2 = data2
        except asyncio.TimeoutError:
            pytest.fail('redis-published client_data not received')

        assert msg2 is not None
