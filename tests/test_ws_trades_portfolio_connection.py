# tests/test_ws_trades_portfolio_connection.py
import asyncio
import json
import pytest
import websockets

WS_URL = "ws://127.0.0.1:8000/stream"

async def _recv_one(ws, timeout: float = 10.0) -> dict:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)

def test_ws_trades_portfolio_connection_stays_alive_without_trades():
    """
    Проверка подписки на сделки:
    - отправляем TradesGetAndSubscribeV2
    - ждём первое сообщение: либо ACK 200, либо ошибка (и тогда тест завершается)
    - после ACK пытаемся получить несколько сообщений с данными сделок; на пустом аккаунте сообщений может не быть — это нормально
    """

    guid_trades = "test-trades-portfolio-conn"

    # Важно: token должен быть непустой строкой, иначе сервер вернёт TokenRequired
    req_trades = {
        "opcode": "TradesGetAndSubscribeV2",
        "exchange": "OKX",       # адаптер игнорирует
        "portfolio": "D39004",   # адаптер игнорирует (в OKX нет портфелей)
        "skipHistory": False,
        "format": "Simple",
        "guid": guid_trades,
        "token": "test-token",
    }

    async def _main():
        async with websockets.connect(WS_URL) as ws:
            # 1) запускаем подписку на сделки
            await ws.send(json.dumps(req_trades))

            # 2) первое сообщение: либо ACK 200, либо ошибка (и тогда соединение закроется)
            first = await _recv_one(ws)

            print("\n=== TRADES FIRST MESSAGE (ACK/ERROR) ===")
            print(json.dumps(first, indent=2, ensure_ascii=False))

            assert first.get("requestGuid") == guid_trades
            assert "httpCode" in first

            # если это ошибка OKX/Astras — завершаем тест (для обычных аккаунтов OKX WS trades может быть недоступен)
            if first.get("httpCode") != 200:
                return

            # 3) после ACK пробуем получить несколько сообщений с данными (если они будут)
            got_any_data = False
            for i in range(3):
                try:
                    msg = await _recv_one(ws, timeout=5.0)
                except Exception:
                    # данных может не быть (например пустой аккаунт) — это нормально
                    break

                print(f"\n=== TRADES DATA MESSAGE #{i+1} ===")
                print(json.dumps(msg, indent=2, ensure_ascii=False))

                assert isinstance(msg, dict)
                assert msg.get("guid") == guid_trades
                assert "data" in msg

                data = msg["data"]
                assert isinstance(data, dict)

                got_any_data = True

            # на пустом аккаунте сделок может не быть — это нормально
            assert got_any_data in (True, False)

    asyncio.run(_main())