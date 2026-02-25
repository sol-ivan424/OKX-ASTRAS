import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import websockets


class OkxWsQuotesSubscriptionMixin:
    async def subscribe_quotes(
        self,
        symbol: str,
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
                    "channel": "tickers",
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

                        data = msg.get("data")
                        if not data:
                            continue

                        for item in data:
                            t = self._parse_okx_ticker_any(symbol, item)
                            res = on_data(t)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception:
                await asyncio.sleep(1.0)
                continue
