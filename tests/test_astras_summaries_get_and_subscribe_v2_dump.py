import asyncio
import json
import time

import pytest
import websockets


WS_URL = "ws://127.0.0.1:8000/stream"


@pytest.mark.anyio
async def test_astras_summaries_get_and_subscribe_v2_dump():
    """
    Тест имитирует Astras:
    - отправляет SummariesGetAndSubscribeV2
    - печатает ВСЕ сообщения от server.py за первые 10 секунд
    """

    req = {
        "opcode": "SummariesGetAndSubscribeV2",
        "exchange": "MOEX",
        "portfolio": "D39004",
        "skipHistory": False,
        "format": "Simple",
        "guid": "test-summaries-subscribe-dump",
        "token": "TEST_TOKEN",
    }

    start_ts = time.time()
    received = 0

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps(req))

        while True:
            timeout = 10 - (time.time() - start_ts)
            if timeout <= 0:
                break

            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break

            received += 1
            print(f"\n=== SERVER MESSAGE #{received} ===")
            try:
                print(json.dumps(json.loads(msg), indent=2, ensure_ascii=False))
            except Exception:
                print(msg)

    print(f"\n=== DONE: received {received} message(s) in 10s ===")