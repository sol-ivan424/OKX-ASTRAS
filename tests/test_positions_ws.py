import asyncio
import json
import os

import pytest
import websockets


@pytest.mark.anyio
async def test_positions_get_and_subscribe_v2():
    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-positions-guid"

    request = {
        "opcode": "PositionsGetAndSubscribeV2",
        "exchange": "OKX",
        "portfolio": "DEV_portfolio",
        "skipHistory": False,
        "format": "Simple",
        "guid": guid,
        "token": "test-token",
    }

    async def recv_json(ws, timeout=5.0) -> dict:
        raw = await asyncio.wait_for(ws.recv(), timeout)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    async with websockets.connect(uri, ping_interval=None) as ws:
        await ws.send(json.dumps(request))

        # ждём первый Astras-ответ именно для нашего guid
        deadline = asyncio.get_event_loop().time() + 10.0
        while True:
            if asyncio.get_event_loop().time() > deadline:
                pytest.fail("Did not receive Astras ACK/ERROR for requestGuid in time")

            msg = await recv_json(ws, timeout=5.0)
            print("\n=== WS MESSAGE ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

            # пропускаем служебные сообщения (например, Connected)
            if msg.get("requestGuid") != guid:
                continue

            # Astras ACK/ERROR формат
            assert "httpCode" in msg and "message" in msg and "requestGuid" in msg, msg

            if msg["httpCode"] != 200:
                pytest.fail(f"Astras ERROR: {msg}")

            assert msg["message"] == "Handled successfully", msg
            break

        # дальше data может быть 0..N (на аккаунте может не быть позиций)
        end_time = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await recv_json(ws, timeout=0.5)
            except asyncio.TimeoutError:
                break

            print("\n=== WS MESSAGE ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

            # если пришли data по нашему guid — проверим, что формат корректный
            if msg.get("guid") == guid and "data" in msg:
                assert isinstance(msg["data"], dict), msg
                assert "symbol" in msg["data"], msg
                assert "existing" in msg["data"], msg