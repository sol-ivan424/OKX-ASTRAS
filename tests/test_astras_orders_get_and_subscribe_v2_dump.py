# tests/test_astras_orders_get_and_subscribe_v2_dump.py
import os
import json
import time
import asyncio
import pytest
import websockets


@pytest.mark.anyio
async def test_astras_orders_get_and_subscribe_v2_dump():
    """
    Имитируем Astras:
    - шлём OrdersGetAndSubscribeV2 на наш сервер (ws://127.0.0.1:8000/stream)
    - печатаем ВСЕ сообщения, которые придут за первые 10 секунд
    - ничего не меняем на аккаунте (только чтение/подписка)
    """
    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-orders-subscribe-dump"

    req = {
        "opcode": "OrdersGetAndSubscribeV2",
        "exchange": "OKX",
        "portfolio": "DEV_portfolio",
        "orderStatuses": ["filled"],
        #"skipHistory": False,
        "format": "Simple",
        "guid": guid,
        "token": "test-token",
    }

    async def recv_json(ws, timeout_s: float = 1.0):
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    async with websockets.connect(uri, ping_interval=None) as ws:
        await ws.send(json.dumps(req))

        end = time.monotonic() + 10.0
        i = 0

        while time.monotonic() < end:
            try:
                msg = await recv_json(ws, timeout_s=1.0)
            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                break

            i += 1
            print(f"\n=== SERVER MESSAGE #{i} ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

        print(f"\n=== DONE: received {i} message(s) in 10s ===")