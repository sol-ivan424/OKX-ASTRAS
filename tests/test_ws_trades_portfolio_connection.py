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
    Проверка без совершения сделок:
    1) Отправляем TradesGetAndSubscribeV2 (сделки по "портфелю" Astras -> фактически по аккаунту OKX).
       На пустом аккаунте сообщений может не быть — это нормально.
    2) Проверяем, что соединение не умерло: отправляем QuotesSubscribe и ждём котировку.
       Если котировка пришла — значит websocket и сервер живы, а обработчик Trades не сломал поток.
    """

    guid_trades = "test-trades-portfolio-conn"
    guid_quotes = "test-quotes-after-trades-conn"

    # Важно: token должен быть непустой строкой, иначе сервер вернёт TokenRequired
    req_trades = {
        "opcode": "TradesGetAndSubscribeV2",
        "exchange": "OKX",       #адаптер игнорирует
        "portfolio": "D39004",   #адаптер игнорирует (в OKX нет портфелей)
        "skipHistory": False,
        "format": "Simple",
        "guid": guid_trades,
        "token": "test-token",
    }

    # Публичные котировки должны приходить даже на пустом аккаунте.
    # Выбери инструмент, который у тебя точно используется в OKX (например BTC-USDT).
    req_quotes = {
        "opcode": "QuotesSubscribe",
        "exchange": "OKX",        #адаптер игнорирует
        "code": "BTC-USDT",       # OKX instId
        "instrumentGroup": "0",   #адаптер игнорирует
        "format": "Simple",       #адаптер игнорирует
        "frequency": 250,
        "guid": guid_quotes,
        "token": "test-token",
    }

    async def _main():
        async with websockets.connect(WS_URL) as ws:
            # 1) запускаем подписку на сделки
            await ws.send(json.dumps(req_trades))

            pong_waiter = await ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=5.0)

            # Не ждём сделку (её может не быть). Просто даём серверу время стартануть задачу.
            await asyncio.sleep(0.5)

            # 2) проверяем, что соединение живо — подписываемся на котировки
            await ws.send(json.dumps(req_quotes))

            # ждём первое сообщение котировок
            msg = await _recv_one(ws, timeout=10.0)

            # Печать для отладки (запускай pytest с -s)
            print("\n=== MESSAGE AFTER TRADES SUBSCRIBE (EXPECT QUOTES) ===")
            print(json.dumps(msg, indent=2, ensure_ascii=False))

            assert isinstance(msg, dict)
            assert msg.get("guid") == guid_quotes
            assert "data" in msg
            assert "opcode" not in msg, "Получено сообщение, похожее на уведомление/ошибку, а не data-ответ"

            data = msg["data"]
            assert isinstance(data, dict)

            # Минимальные проверки формата котировок Astras
            assert "symbol" in data
            assert "last_price" in data
            assert data["symbol"] in ("BTC-USDT", "0") or isinstance(data["symbol"], str)

    asyncio.run(_main())