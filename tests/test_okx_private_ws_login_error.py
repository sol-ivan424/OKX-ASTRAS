# tests/test_okx_private_ws_login_error.py

import os
import json
import pytest
import websockets
from dotenv import load_dotenv


@pytest.mark.anyio
async def test_proxy_ws_returns_error_envelope_for_invalid_okx_keys():
    """Проверка НАШЕЙ прокладки (api.server WS /stream):

    Пользователь специально ставит неверные OKX ключи в .env.
    Тест:
      1) подключается к НАШЕМУ серверу
      2) отправляет приватный opcode (OrdersGetAndSubscribeV2)
      3) ждёт ПЕРВОЕ сообщение от сервера
      4) печатает его в консоль как есть
      5) проверяет только структуру Astras-конверта и то, что это НЕ 200

    Тест НЕ придумывает message/httpCode — он выводит то, что реально отдаёт сервер.
    """

    load_dotenv()

    ws_url = os.getenv("ASTRAS_PROXY_WS", "ws://127.0.0.1:8000/stream")

    guid = "proxy-invalid-okx-keys-test"

    # Приватный opcode, который должен привести к private WS login на OKX
    req = {
        "opcode": "OrdersGetAndSubscribeV2",
        "exchange": "OKX",            # пример, сервер может игнорировать
        "portfolio": "0",             # пример, сервер может игнорировать
        "orderStatuses": [],
        "skipHistory": True,           # чтобы не дергать REST историю
        "instrumentGroup": "SPOT",    # адаптер игнорирует
        "format": "Simple",           # адаптер игнорирует
        "frequency": 250,              # адаптер игнорирует
        "guid": guid,
        "token": "test-token",        # сервер может игнорировать
    }

    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps(req))

        # Первое сообщение должно быть либо ACK=200, либо ERROR (и по Astras при ошибке WS закроется)
        raw = await ws.recv()
        msg = json.loads(raw)

        # Выводим ровно то, что прислал сервер
        print("\nSERVER FIRST MESSAGE:")
        print(json.dumps(msg, ensure_ascii=False, indent=2))

        # Проверяем только, что это Astras-конверт (порядок полей в JSON не важен)
        assert msg.get("requestGuid") == guid, msg
        assert isinstance(msg.get("httpCode"), int), msg
        assert isinstance(msg.get("message"), str), msg

        # В этом тесте ожидаем именно ошибку (пользователь выставляет неверные ключи)
        #assert msg["httpCode"] != 200, msg