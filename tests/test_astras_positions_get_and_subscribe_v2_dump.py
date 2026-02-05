# tests/test_astras_positions_get_and_subscribe_v2_dump.py

import os
import json
import asyncio
import pytest
import websockets


@pytest.mark.anyio
async def test_astras_positions_get_and_subscribe_v2_dump():
    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-positions-subscribe-dump"

    req = {
        "opcode": "PositionsGetAndSubscribeV2",
        "exchange": "OKX",        # как у Astras (у нас внутри все равно OKX)
        "portfolio": "D39004",     # пример Astras
        "skipHistory": False,
        "format": "Simple",
        "guid": guid,
        "token": "test-token",
    }

    async def recv_json(ws, timeout_s: float = 10.0) -> dict:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    async with websockets.connect(uri, ping_interval=None) as ws:
        await ws.send(json.dumps(req))

        end_time = asyncio.get_event_loop().time() + 10.0
        i = 0

        while asyncio.get_event_loop().time() < end_time:
            try:
                msg = await recv_json(ws, timeout_s=1.0)
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                print("\n=== WS CLOSED BY SERVER ===")
                break

            i += 1
            print(f"\n=== SERVER MESSAGE #{i} ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

        print(f"\n=== DONE: received {i} message(s) in 10s ===")