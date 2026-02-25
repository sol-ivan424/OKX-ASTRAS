import asyncio
import json
from typing import Any, Dict


class OkxWsOrderTransportMixin:
    async def _place_order_via_private_ws(self, body: Dict[str, Any], req_id: str) -> Dict[str, Any]:
        order_msg = {"id": req_id, "op": "order", "args": [body]}

        async with self._order_ws_req_lock:
            last_err = None
            for _ in range(2):
                ws = await self._ensure_order_ws()
                try:
                    await ws.send(json.dumps(order_msg))

                    resp_deadline = asyncio.get_event_loop().time() + 5.0
                    while True:
                        if asyncio.get_event_loop().time() > resp_deadline:
                            raise RuntimeError("OKX WS order timeout")

                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        msg = json.loads(raw)
                        if msg.get("id") != req_id:
                            continue
                        return msg
                except Exception as e:
                    last_err = e
                    await self._close_order_ws()
                    continue
            if last_err is not None:
                raise last_err
            raise RuntimeError("OKX WS order error")

    async def _cancel_order_via_private_ws(self, body: Dict[str, Any], req_id: str) -> Dict[str, Any]:
        cancel_msg = {"id": req_id, "op": "cancel-order", "args": [body]}

        async with self._order_ws_req_lock:
            last_err = None
            for _ in range(2):
                ws = await self._ensure_order_ws()
                try:
                    await ws.send(json.dumps(cancel_msg))

                    resp_deadline = asyncio.get_event_loop().time() + 5.0
                    while True:
                        if asyncio.get_event_loop().time() > resp_deadline:
                            raise RuntimeError("OKX WS cancel-order timeout")

                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        msg = json.loads(raw)
                        if msg.get("id") != req_id:
                            continue
                        return msg
                except Exception as e:
                    last_err = e
                    await self._close_order_ws()
                    continue
            if last_err is not None:
                raise last_err
            raise RuntimeError("OKX WS cancel-order error")
