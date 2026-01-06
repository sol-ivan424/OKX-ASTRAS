import asyncio
import json
import websockets

async def main():
    uri = "ws://127.0.0.1:8000/stream"
    async with websockets.connect(uri) as ws:
        req = {
            "opcode": "BarsGetAndSubscribe",
            "exchange": "OKX",
            "code": "BTC-USDT",
            "instrumentGroup": "SPOT",
            "tf": "60",
            "from": 0,
            "skipHistory": True,
            "splitAdjust": True,
            "format": "Simple",
            "frequency": 250,
            "guid": "bars-online-test",
            "token": "test"
        }

        await ws.send(json.dumps(req))
        print("sent:", req)

        while True:
            msg = await ws.recv()
            print("recv:", msg)

asyncio.run(main())
