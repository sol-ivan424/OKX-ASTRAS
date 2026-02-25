import asyncio
import json
from typing import Any, Dict, List, Optional

import websockets


class OkxWsOrderConnectionMixin:
    async def _ws_unsubscribe(self, ws, args: Optional[List[Dict[str, Any]]]) -> None:
        if not args:
            return
        try:
            await ws.send(json.dumps({"op": "unsubscribe", "args": args}))
        except Exception:
            return

    async def _close_order_ws(self) -> None:
        ws = self._order_ws
        self._order_ws = None
        task = self._order_ws_keepalive_task
        self._order_ws_keepalive_task = None
        current = asyncio.current_task()
        if task is not None and task is not current:
            task.cancel()
            try:
                await task
            except Exception:
                pass
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def _order_ws_keepalive_loop(self, ws) -> None:
        while self._order_ws is ws:
            try:
                await asyncio.sleep(5.0)
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=5.0)
            except Exception:
                if self._order_ws is ws:
                    await self._close_order_ws()
                    try:
                        await self._ensure_order_ws()
                    except Exception:
                        pass
                return

    def _start_order_ws_keepalive(self, ws) -> None:
        task = self._order_ws_keepalive_task
        if task is not None:
            task.cancel()
        self._order_ws_keepalive_task = asyncio.create_task(self._order_ws_keepalive_loop(ws))

    async def _ensure_order_ws(self):
        if self._order_ws is not None:
            return self._order_ws

        async with self._order_ws_lock:
            if self._order_ws is not None:
                return self._order_ws

            ws = await websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20)
            await ws.send(json.dumps(self._ws_login_payload()))

            login_deadline = asyncio.get_event_loop().time() + 5.0
            while True:
                if asyncio.get_event_loop().time() > login_deadline:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    raise RuntimeError("OKX WS login timeout")

                raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                msg = json.loads(raw)

                if msg.get("event") == "error":
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")

                if msg.get("event") == "login":
                    if msg.get("code") == "0":
                        self._order_ws = ws
                        self._start_order_ws_keepalive(ws)
                        return ws
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    raise RuntimeError(f"OKX WS login error {msg.get('code')}: {msg.get('msg')}")
