import os
import time
import json
import hmac
import base64
import hashlib

import pytest
import websockets
from dotenv import load_dotenv
load_dotenv()


def _okx_ws_sign(api_secret: str, ts: str) -> str:
    """
    OKX private WS sign:
    sign = Base64( HMAC_SHA256( ts + "GET" + "/users/self/verify" ) )
    """
    msg = f"{ts}GET/users/self/verify"
    mac = hmac.new(api_secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode("utf-8")


@pytest.mark.anyio
async def test_okx_private_ws_trades_subscribe():
    """
    Проверяет, что:
    - private WS login проходит (event=login, code=0)
    - подписка на канал исполнений (fills) проходит (event=subscribe)
    Реальные сделки не нужны: подтверждение subscribe достаточно.
    """

    api_key = os.getenv("OKX_API_KEY")
    api_secret = os.getenv("OKX_API_SECRET")
    api_passphrase = os.getenv("OKX_API_PASSPHRASE")

    if not (api_key and api_secret and api_passphrase):
        pytest.skip("OKX API ключи не заданы в окружении (OKX_API_KEY/OKX_API_SECRET/OKX_API_PASSPHRASE).")

    url = "wss://ws.okx.com:8443/ws/v5/private"

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        # 1) login
        ts = str(int(time.time()))
        sign = _okx_ws_sign(api_secret, ts)

        await ws.send(
            json.dumps(
                {
                    "op": "login",
                    "args": [
                        {
                            "apiKey": api_key,
                            "passphrase": api_passphrase,
                            "timestamp": ts,
                            "sign": sign,
                        }
                    ],
                }
            )
        )

        msg = json.loads(await ws.recv())
        assert msg.get("event") == "login"
        assert str(msg.get("code")) in ("0", 0), msg

        # 2) subscribe to fills (исполнения/сделки)
        await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "fills", "instType": "SPOT"}]}))

        msg = json.loads(await ws.recv())

        # OKX может запрещать подписку на канал fills для аккаунтов ниже VIP6.
        # Тогда приходит событие error с кодом 60029.
        if msg.get("event") == "error" and str(msg.get("code")) == "60029":
            pytest.skip(
                "OKX запретил подписку на private WS канал 'fills' (code=60029): "
                "доступно только для VIP6+ по trading fee tier. "
                "Для проверки 'сделок' на обычном аккаунте используйте REST fills-history "
                "или private WS канал 'orders' (исполнения приходят в ордер-событиях)."
            )

        assert msg.get("event") == "subscribe", msg
        arg = msg.get("arg") or {}
        assert arg.get("channel") == "fills"
        assert arg.get("instType") == "SPOT"