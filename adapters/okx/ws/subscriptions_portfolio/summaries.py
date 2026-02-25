import asyncio
import json
from typing import Any, Callable, Dict, List, Optional

import websockets


class OkxWsSummariesSubscriptionMixin:
    async def subscribe_summaries(
        self,
        on_data: Callable[[Dict[str, Any]], Any],
        stop_event: asyncio.Event,
        on_subscribed: Optional[Callable[[Dict[str, Any]], Any]] = None,
        on_error: Optional[Callable[[Dict[str, Any]], Any]] = None,
        unsub_args: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self._assert_private_ws("account", self._ws_private_url)

        async def _call(cb, arg):
            if cb is None:
                return
            try:
                r = cb(arg)
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                return

        login_msg = self._ws_login_payload()
        sub_msg = {"op": "subscribe", "args": [{"channel": "account"}]}
        unsub_args = unsub_args or sub_msg.get("args")

        while not stop_event.is_set():
            try:
                async with websockets.connect(self._ws_private_url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(login_msg))

                    authed = False
                    while not stop_event.is_set() and not authed:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "login":
                            if m.get("code") == "0":
                                authed = True
                                break
                            await _call(on_error, m)
                            return

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                    if not authed:
                        continue

                    await ws.send(json.dumps(sub_msg))

                    subscribed = False
                    sub_deadline = asyncio.get_event_loop().time() + 5.0
                    while not subscribed and not stop_event.is_set():
                        if asyncio.get_event_loop().time() > sub_deadline:
                            await _call(on_error, {"event": "error", "msg": "OKX WS subscribe timeout"})
                            return

                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        m = json.loads(raw)

                        if m.get("event") == "subscribe":
                            await _call(on_subscribed, m)
                            subscribed = True
                            break

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        arg = m.get("arg") or {}
                        if arg.get("channel") == "account":
                            data = m.get("data") or []
                            if data:
                                summary = self._parse_okx_account_summary_any(data[0] or {})
                                r = on_data(summary)
                                if asyncio.iscoroutine(r):
                                    await r

                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            continue

                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")

                        m = json.loads(raw)

                        if m.get("event") == "error":
                            await _call(on_error, m)
                            return

                        arg = m.get("arg") or {}
                        if arg.get("channel") != "account":
                            continue

                        data = m.get("data") or []
                        if not data:
                            continue

                        summary = self._parse_okx_account_summary_any(data[0] or {})
                        r = on_data(summary)
                        if asyncio.iscoroutine(r):
                            await r

                    if stop_event.is_set() and subscribed:
                        await self._ws_unsubscribe(ws, unsub_args)

            except Exception as e:
                await _call(on_error, {"event": "error", "msg": str(e)})
                await asyncio.sleep(1.0)
                continue
