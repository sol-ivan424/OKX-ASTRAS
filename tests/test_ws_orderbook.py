import asyncio
import json

import websockets


async def _recv_one(ws, timeout_sec: float = 10.0) -> dict:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
    return json.loads(raw)


def test_ws_orderbook_simple():
    """
    Интеграционный тест.

    Важно:
    сервер должен быть запущен отдельно:
    uvicorn api.server:app --reload --host 127.0.0.1 --port 8000

    Тест проверяет формат Simple для стакана и обрезку по depth.
    """

    async def run():
        uri = "ws://127.0.0.1:8000/stream"

        guid = "test-orderbook-guid"
        depth = 10

        req = {
            "opcode": "OrderBookGetAndSubscribe",
            "exchange": "MOEX",              #адаптер игнорирует
            "code": "BTC-USDT",              # OKX instId
            "instrumentGroup": "TQBR",       #адаптер игнорирует
            "depth": depth,
            "format": "Simple",              #адаптер игнорирует
            "frequency": 250,
            "guid": guid,
            "token": "test-token",
        }

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(req))

            msg = await _recv_one(ws, timeout_sec=15.0)

            assert "data" in msg
            assert msg.get("guid") == guid

            data = msg["data"]

            assert "snapshot" in data
            assert "existing" in data
            assert isinstance(data["existing"], bool)
            assert data["snapshot"] == data["existing"]

            assert "ms_timestamp" in data
            assert isinstance(data["ms_timestamp"], int)

            assert "timestamp" in data
            assert isinstance(data["timestamp"], int)
            if data["ms_timestamp"] != 0:
                assert data["timestamp"] == data["ms_timestamp"] // 1000

            assert "bids" in data and isinstance(data["bids"], list)
            assert "asks" in data and isinstance(data["asks"], list)

            assert len(data["bids"]) <= depth
            assert len(data["asks"]) <= depth

            for side in ("bids", "asks"):
                for lvl in data[side]:
                    assert "price" in lvl
                    assert "volume" in lvl
                    assert isinstance(lvl["price"], (int, float))
                    assert isinstance(lvl["volume"], (int, float))

    asyncio.run(run())


def test_ws_orderbook_existing_switches_to_false_on_updates():
    """
    Проверяем, что после первого сообщения (snapshot/existing=True)
    приходят обновления с existing=False.

    Сервер должен быть запущен отдельно.
    """
    async def run():
        uri = "ws://127.0.0.1:8000/stream"

        guid = "test-orderbook-existing-switch"
        req = {
            "opcode": "OrderBookGetAndSubscribe",
            "exchange": "MOEX",              # адаптер игнорирует
            "code": "BTC-USDT",              # OKX instId
            "instrumentGroup": "TQBR",       # адаптер игнорирует
            "depth": 10,
            "format": "Simple",              # адаптер игнорирует
            "frequency": 25,                 # минимально для Simple
            "guid": guid,
            "token": "test-token",
        }

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(req))

            first = await _recv_one(ws, timeout_sec=15.0)

            assert first.get("guid") == guid
            assert first["data"]["existing"] is True
            assert first["data"]["snapshot"] is True

            # ждём следующее сообщение (update). На реальном рынке оно обычно приходит быстро,
            # но на спокойном рынке может потребоваться подождать чуть дольше.
            second = await _recv_one(ws, timeout_sec=30.0)

            assert second.get("guid") == guid
            assert second["data"]["existing"] is False
            assert second["data"]["snapshot"] is False

    asyncio.run(run())


def test_ws_orderbook_frequency_throttling():
    """
    Проверяем троттлинг по frequency:
    сервер не должен отправлять сообщения чаще, чем раз в N мс (frequency).

    Сервер должен быть запущен отдельно.
    """
    async def run():
        uri = "ws://127.0.0.1:8000/stream"

        guid = "test-orderbook-frequency"
        freq_ms = 500

        req = {
            "opcode": "OrderBookGetAndSubscribe",
            "exchange": "MOEX",              # адаптер игнорирует
            "code": "BTC-USDT",              # OKX instId
            "instrumentGroup": "TQBR",       # адаптер игнорирует
            "depth": 10,
            "format": "Simple",              # адаптер игнорирует
            "frequency": freq_ms,
            "guid": guid,
            "token": "test-token",
        }

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(req))

            m1 = await _recv_one(ws, timeout_sec=15.0)

            assert m1.get("guid") == guid

            t1 = int(m1["data"].get("ms_timestamp", 0) or 0)
            assert t1 > 0

            m2 = await _recv_one(ws, timeout_sec=30.0)

            assert m2.get("guid") == guid

            t2 = int(m2["data"].get("ms_timestamp", 0) or 0)
            assert t2 > 0

            # сервер берёт "последнее" сообщение в интервале frequency,
            # поэтому разница по времени должна быть >= frequency
            assert (t2 - t1) >= freq_ms

    asyncio.run(run())


if __name__ == "__main__":
    async def _demo():
        uri = "ws://127.0.0.1:8000/stream"
        guid = "demo-orderbook"
        req = {
            "opcode": "OrderBookGetAndSubscribe",
            "exchange": "MOEX",              # адаптер игнорирует
            "code": "BTC-USDT",              # OKX instId
            "instrumentGroup": "TQBR",       # адаптер игнорирует
            "depth": 10,
            "format": "Simple",              # адаптер игнорирует
            "frequency": 250,
            "guid": guid,
            "token": "test-token",
        }

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(req))

            # печатаем несколько сообщений: snapshot + пару update
            for i in range(3):
                msg = await _recv_one(ws, timeout_sec=30.0)
                print(f"\n=== DEMO ORDERBOOK MESSAGE {i+1} ===")
                print(json.dumps(msg, indent=2, ensure_ascii=False))

    asyncio.run(_demo())