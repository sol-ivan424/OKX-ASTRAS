# tests/test_astras_trades_get_and_subscribe_v2_dump.py

import asyncio
import json
import os

import pytest
import websockets


@pytest.mark.anyio
async def test_astras_trades_get_and_subscribe_v2_dump_all_messages_10s():
    """
    Имитирует Astras:
    - Подключается к ws://127.0.0.1:8000/stream
    - Отправляет opcode TradesGetAndSubscribeV2
    - Печатает ВСЕ сообщения, которые придут за первые 10 секунд
    """

    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-trades-subscribe-dump"

    req = {
        "opcode": "TradesGetAndSubscribeV2",
        "exchange": "OKX",
        "portfolio": "DEV_portfolio",
        "skipHistory": False,
        "format": "Simple",
        "guid": guid,
        "token": "test",
    }

    async def recv_one(ws, timeout_s: float = 1.0):
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    async with websockets.connect(uri, ping_interval=None) as ws:
        await ws.send(json.dumps(req, ensure_ascii=False))

        end_time = asyncio.get_event_loop().time() + 10.0
        i = 0

        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await recv_one(ws, timeout_s=1.0)
            except asyncio.TimeoutError:
                continue

            i += 1
            print(f"\n=== SERVER MESSAGE #{i} ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

        # тест “проходит” всегда, его цель — дамп сообщений
        assert True