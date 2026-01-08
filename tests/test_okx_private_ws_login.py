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
        await ws.send(json.dumps(a._ws_login_payload()))
        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        print("recv:", msg)

    await a.close()

<<<<<<< HEAD
asyncio.run(main())
=======
asyncio.run(main())
>>>>>>> 9dce9d62bafd68df2017e072cf510512f3dfb220
