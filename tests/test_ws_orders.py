# tests/test_ws_orders.py

import asyncio
import json
import websockets

async def main():
    uri = "ws://127.0.0.1:8000/stream"

    async with websockets.connect(uri) as ws:
        req = {
            "opcode": "OrdersGetAndSubscribeV2",
            "exchange": "OKX",
            "portfolio": "0",
            "symbols": [],

            "orderStatuses": [],
            "skipHistory": False,

            "instrumentGroup": "SPOT",   #адаптер игнорирует
            "format": "Simple",          #адаптер игнорирует
            "frequency": 250,            #адаптер игнорирует

            "guid": "orders-test",
            "token": "test",
        }

        await ws.send(json.dumps(req))
        print("sent:", req)

        while True:
            msg = await ws.recv()
            print("recv:", msg)

asyncio.run(main())
