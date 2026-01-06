import asyncio
import json
import time
import websockets

async def main():
    uri = "ws://127.0.0.1:8000/stream"
    async with websockets.connect(uri) as ws:
        from_ts = int(time.time()) - 30 * 60

        req = {
            "opcode": "BarsGetAndSubscribe",
            "exchange": "OKX",
            "code": "BTC-USDT",
            "instrumentGroup": "SPOT",
            "tf": "60",
            "from": from_ts,
            "skipHistory": False,
            "splitAdjust": True,
            "format": "Simple",
            "frequency": 250,
            "guid": "bars-history-test",
            "token": "test"
        }

        await ws.send(json.dumps(req))
        print("sent:", req)

        while True:
            msg = await ws.recv()
            print("recv:", msg)

asyncio.run(main())
