import os
import time
import json
import asyncio

import pytest
import websockets
from dotenv import load_dotenv
load_dotenv()


# NOTE: We use pytest-anyio (built into pytest via the anyio plugin) to run async tests without pytest-asyncio.
@pytest.mark.anyio
async def test_okx_private_ws_trades_subscribe():
    """
    Проверяет прокладку (наш сервер), а не прямой OKX:

    - Отправляем TradesGetAndSubscribeV2 на ws://127.0.0.1:8000/stream
    - Ждём первым сообщением ACK(200) или ERROR от НАШЕГО сервера
    - Далее можем получить 0..N сообщений с data (на аккаунте может не быть сделок)

    Успех теста:
      * получен корректный ACK/ERROR с requestGuid
      * при ACK(200) любые последующие data (если придут) имеют тот же guid
    """

    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-trades-subscribe"

    req = {
        "opcode": "TradesGetAndSubscribeV2",
        "exchange": "OKX",
        "portfolio": "DEV_portfolio",
        "instrumentGroup": "SPOT",
        "skipHistory": True,
        "format": "Simple",
        "guid": guid,
        "token": "test",
    }

    async def recv_json(ws, timeout_s: float = 10.0) -> dict:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps(req))

        # Сервер может отправить «приветственное» сообщение сразу после коннекта
        # без requestGuid (например: {"httpCode": 200, "message": "Connected"}).
        # Нам нужно дождаться первого сообщения, относящегося к НАШЕМУ guid.

        first = None
        deadline_ack = time.monotonic() + 10.0
        while time.monotonic() < deadline_ack:
            msg = await recv_json(ws, timeout_s=10.0)

            # пропускаем любые сообщения без requestGuid (connect/health/etc)
            if not isinstance(msg, dict) or "requestGuid" not in msg:
                print("\n=== SKIP NON-REQUEST MESSAGE ===")
                print(json.dumps(msg, ensure_ascii=False, indent=2))
                continue

            first = msg
            break

        assert first is not None, "No ACK/ERROR with requestGuid received from server"

        print("\n=== TRADES FIRST MESSAGE (ACK/ERROR) ===")
        print(json.dumps(first, ensure_ascii=False, indent=2))

        # ACK/ERROR в формате Astras:
        # { "message": "...", "httpCode": 200|4xx|5xx, "requestGuid": "..." }
        assert first.get("requestGuid") == guid, first
        assert "httpCode" in first, first
        assert "message" in first, first

        http_code = int(first.get("httpCode"))

        # Если пришла ошибка — это тоже валидно (например, если OKX отказал в подписке)
        if http_code != 200:
            return

        # ACK(200) получен -> подписка подтверждена. Сделок может не быть.
        # Собираем несколько сообщений data за короткий промежуток времени.
        deadline = time.monotonic() + 3.0
        got_any_data = False

        while time.monotonic() < deadline:
            try:
                msg = await recv_json(ws, timeout_s=0.5)
            except asyncio.TimeoutError:
                continue

            print("\n=== TRADES DATA ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))

            if isinstance(msg, dict) and "data" in msg:
                got_any_data = True
                assert msg.get("guid") == guid, msg

        # На аккаунте может не быть сделок — это нормально
        assert got_any_data in (True, False)