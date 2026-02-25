import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import websockets


class OkxWsOrderBookSubscriptionMixin:
    async def subscribe_order_book(
        self,
        symbol: str,
        depth: int,
        on_data: Callable[[dict], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "channel": "books",
                    "instId": symbol,
                }
            ],
        }
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_public_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))
                    subscribed_sent = False

                    bids_state: Dict[float, float] = {}
                    asks_state: Dict[float, float] = {}

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        msg = json.loads(raw)

                        if msg.get("event") == "subscribe":
                            if (not subscribed_sent) and on_subscribed is not None:
                                res = on_subscribed(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            subscribed_sent = True
                            continue

                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        action = (msg.get("action") or "").lower()
                        is_snapshot = action == "snapshot"

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            ts_ms = self._to_int(item.get("ts"))

                            if is_snapshot:
                                bids_state.clear()
                                asks_state.clear()

                            self._apply_okx_book_delta(bids_state, item.get("bids") or [])
                            self._apply_okx_book_delta(asks_state, item.get("asks") or [])

                            bids_full = sorted(bids_state.items(), key=lambda x: x[0], reverse=True)
                            asks_full = sorted(asks_state.items(), key=lambda x: x[0])

                            book = {
                                "symbol": symbol,
                                "ts": ts_ms,
                                "bids": [(float(p), float(v)) for (p, v) in bids_full],
                                "asks": [(float(p), float(v)) for (p, v) in asks_full],
                                "existing": bool(is_snapshot),
                            }

                            res = on_data(book)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception:
                await asyncio.sleep(1.0)
                continue
