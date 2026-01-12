import asyncio
import json
import time
import websockets


async def _recv_one(ws, timeout_sec=20.0):
    return json.loads(
        await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
    )


def test_ws_quotes_simple():
    """
    Проверка QuotesSubscribe (Astras Simple) для OKX.
    """

    async def run():
        uri = "ws://127.0.0.1:8000/stream"
        guid = "test-quotes-simple"

        req = {
            "opcode": "QuotesSubscribe",
            "exchange": "MOEX",          # пример, сервер игнорирует
            "code": "BTC-USDT",          # instId OKX
            "instrumentGroup": "TQBR",   # пример, сервер игнорирует
            "format": "Simple",
            "frequency": 250,
            "guid": guid,
            "token": "test-token",
        }

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(req))

            # 1) Сначала ACK об успешной обработке подписки (Astras httpCode=200)
            ack = await _recv_one(ws)

            assert ack.get("requestGuid") == guid
            assert ack.get("httpCode") == 200
            assert isinstance(ack.get("message"), str)

            print("=== ACK 200 ===")
            print(json.dumps(ack, indent=2, ensure_ascii=False))

            # 2) Затем несколько сообщений с данными котировки (обычно приходят периодически)
            async def _assert_quotes_data(data: dict):
                # обязательные идентификаторы
                assert data["symbol"] == "BTC-USDT"
                assert data["exchange"] == "OKX"

                # description и prev_close_price — строго null
                assert data["description"] is None
                assert data["prev_close_price"] is None

                # последняя цена и таймстамп
                assert isinstance(data["last_price"], (int, float))
                assert data["last_price"] >= 0

                assert isinstance(data["last_price_timestamp"], int)
                assert data["last_price_timestamp"] > 0

                # high / low — берём 24h значения OKX
                assert isinstance(data["high_price"], (int, float))
                assert isinstance(data["low_price"], (int, float))

                # объёмы
                assert isinstance(data["volume"], (int, float))
                assert data["volume"] >= 0

                # bid / ask
                assert isinstance(data["bid"], (int, float))
                assert isinstance(data["ask"], (int, float))

                # объёмы лучшего уровня
                assert isinstance(data["bid_vol"], (int, float))
                assert isinstance(data["ask_vol"], (int, float))

                # ob_ms_timestamp в QuotesSubscribe не используется
                assert data["ob_ms_timestamp"] is None

                # open_price по договорённости 0
                assert data["open_price"] == 0

                # поля без источника данных
                assert data["open_interest"] is None
                assert data["yield"] is None

                assert data["lotsize"] == 0
                assert data["lotvalue"] == 0
                assert data["facevalue"] == 0
                assert data["type"] == "0"

                assert data["total_bid_vol"] == 0
                assert data["total_ask_vol"] == 0

                # deprecated
                assert data["accrued_interest"] == 0

                # change зависит от prev_close_price -> 0
                assert data["change"] == 0
                assert data["change_percent"] == 0

            # ждём сообщения с котировками
            for i in range(10):
                msg = await _recv_one(ws)

                assert msg.get("guid") == guid
                data = msg.get("data")
                assert isinstance(data, dict)

                print(f"=== QUOTES DATA #{i+1} ===")
                print(json.dumps(msg, indent=2, ensure_ascii=False))

                await _assert_quotes_data(data)

    asyncio.run(run())