import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import websockets


class OkxWsBarsSubscriptionMixin:
    async def subscribe_bars(
        self,
        symbol: str,
        tf: str,
        from_ts: int,
        skip_history: bool,
        split_adjust: bool,
        on_data: Callable[[dict], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
        inst_type: Optional[str] = None,
    ) -> None:
        channel = self._tf_to_okx_ws_channel(tf)
        sub_msg = {
            "op": "subscribe",
            "args": [{"channel": channel, "instId": symbol}],
        }
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_candles_url, ping_interval=20, ping_timeout=20) as ws:
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

                        for candle_arr in data:
                            bar = self._parse_okx_candle_any(symbol, candle_arr, inst_type=inst_type)
                            res = on_data(bar)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception:
                await asyncio.sleep(1.0)
                continue
