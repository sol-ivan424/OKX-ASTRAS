import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import websockets


class OkxWsTradesSubscriptionMixin:
    async def subscribe_trades(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        inst_type: str = "SPOT",
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        login_msg = self._ws_login_payload()

        inst_type_u = str(inst_type).strip().upper() if inst_type else ""
        if inst_type_u in ("SPOT", "SWAP", "FUTURES"):
            inst_types = [inst_type_u]
        else:
            inst_types = ["SPOT", "SWAP", "FUTURES"]

        sub_args_list: List[Dict[str, Any]] = []
        for it in inst_types:
            sub_args_list.append({"channel": "orders", "instType": it})

        sub_msg = {"op": "subscribe", "args": sub_args_list}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_msg))

                    authed = False
                    login_deadline = asyncio.get_event_loop().time() + 5.0
                    while not authed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > login_deadline:
                            if on_error is not None:
                                res = on_error({"event": "error", "code": None, "msg": "OKX WS login timeout"})
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        raw = await ws.recv()
                        msg = json.loads(raw)

                        if msg.get("event") == "error":
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

                        if msg.get("event") == "login":
                            if msg.get("code") == "0":
                                authed = True
                                break
                            if on_error is not None:
                                res = on_error(msg)
                                if asyncio.iscoroutine(res):
                                    await res
                            return

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
                            trade_id = item.get("tradeId")
                            if not trade_id:
                                continue

                            fill_sz = self._to_float(item.get("fillSz"))
                            if fill_sz <= 0:
                                continue

                            trade = self._parse_okx_trade_any(item, is_history=False, inst_type=inst_type)
                            res = on_data(trade)
                            if asyncio.iscoroutine(res):
                                await res

                    if stop_event.is_set() and subscribed_sent:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                if on_error is not None:
                    try:
                        res = on_error({"event": "error", "code": None, "msg": f"OKX adapter error: {e}"})
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
                return
