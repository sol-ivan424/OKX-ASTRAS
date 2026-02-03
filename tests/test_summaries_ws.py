import os
import json
import asyncio

import pytest
import websockets


@pytest.mark.anyio
async def test_summaries_get_and_subscribe_v2():
    """Проверка WS SummariesGetAndSubscribeV2 на нашем сервере.

    Что проверяем по факту (без выдумок):
    - Сервер принимает запрос.
    - Сервер отправляет подтверждение (ACK) в виде:
        {"message": "Handled successfully", "httpCode": 200, "requestGuid": "<guid>"}
      (именно так у нас сейчас реализовано для WS-ACK).
    - Соединение не падает сразу после ACK.
    - Печатаем первые 5 сообщений, если они приходят.

    На аккаунте может не быть событий/изменений, поэтому после ACK сообщений может не быть.
    """

    uri = os.getenv("ASTRAS_WS_URL", "ws://127.0.0.1:8000/stream")
    guid = "test-summaries-guid"

    request = {
        "opcode": "SummariesGetAndSubscribeV2",
        "exchange": "OKX",
        "portfolio": "DEV_portfolio",
        "skipHistory": False,
        "format": "Simple",
        "guid": guid,
        "token": "test-token",
    }

    async def recv_json(ws, timeout_s: float = 1.0):
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout_s)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)

    ack_received = False
    printed = 0
    max_print = 5

    async with websockets.connect(uri, ping_interval=None) as ws:
        await ws.send(json.dumps(request))

        # Ждём ACK до 5 секунд. Параллельно печатаем всё, что приходит.
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < 5.0:
            try:
                msg = await recv_json(ws, timeout_s=1.0)
            except asyncio.TimeoutError:
                # Нет сообщений — продолжаем ждать ACK, но не падаем.
                continue

            if printed < max_print:
                print(f"\n=== WS MESSAGE {printed + 1} ===")
                print(json.dumps(msg, ensure_ascii=False, indent=2))
                printed += 1

            # ACK нашего сервера
            if (
                isinstance(msg, dict)
                and msg.get("httpCode") == 200
                and msg.get("requestGuid") == guid
            ):
                ack_received = True
                break

        assert ack_received, "Server didn't send ACK for SummariesGetAndSubscribeV2"

        # После ACK попробуем дочитать ещё сообщения (если есть), но не требуем их.
        end_deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < end_deadline and printed < max_print:
            try:
                msg = await recv_json(ws, timeout_s=1.0)
            except asyncio.TimeoutError:
                break

            print(f"\n=== WS MESSAGE {printed + 1} ===")
            print(json.dumps(msg, ensure_ascii=False, indent=2))
            printed += 1

        # Важный минимум: соединение живо (не упало исключением) и ACK получен.
        # Если сервер сам закрывает WS, websockets кинет исключение — тест упадёт.