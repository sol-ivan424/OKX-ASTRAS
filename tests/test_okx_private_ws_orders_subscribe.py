import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv
import websockets

from adapters.okx_adapter import OkxAdapter

#загрузка .env из корня проекта
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


async def main():
    a = OkxAdapter(
        api_key=os.getenv("OKX_API_KEY"),
        api_secret=os.getenv("OKX_API_SECRET"),
        api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
    )

    async with websockets.connect(a._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
        #login
        await ws.send(json.dumps(a._ws_login_payload()))
        login_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        print("login:", login_msg)

        #subscribe orders
        sub_msg = {
            "op": "subscribe",
            "args": [
                {"channel": "orders", "instType": "SPOT"},
            ],
        }
        await ws.send(json.dumps(sub_msg))
        print("sent subscribe:", sub_msg)

        #читаем входящие сообщения 30 секунд
        end = asyncio.get_event_loop().time() + 30.0
        while asyncio.get_event_loop().time() < end:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                print("recv:", msg)
            except asyncio.TimeoutError:
                print("recv: (timeout)")
                continue

    await a.close()


asyncio.run(main())
