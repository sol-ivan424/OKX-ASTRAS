import asyncio
from fastapi import WebSocket
from starlette.websockets import WebSocketState


class WSContext:
    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.active: dict[str, list[asyncio.Event]] = {
            "quotes": [],
            "book": [],
            "fills": [],
            "orders": [],
            "positions": [],
            "summaries": [],
            "bars": [],
        }
        self.subs: dict[str, asyncio.Event] = {}
        self.last_sent_ms_book: dict[str, int] = {}
        self.last_sent_ms_quotes: dict[str, int] = {}

    async def safe_send_json(self, payload: dict):
        if self.ws.client_state != WebSocketState.CONNECTED:
            return
        try:
            await self.ws.send_json(payload)
        except Exception:
            return

    async def send_ack_200(self, guid: str | None):
        await self.safe_send_json(
            {
                "message": "Handled successfully",
                "httpCode": 200,
                "requestGuid": guid,
            }
        )

    async def wait_okx_subscribed_or_error(
        self,
        subscribed_ev: asyncio.Event,
        error_ev: asyncio.Event,
        guid: str,
        stop_ev: asyncio.Event,
        timeout_s: float = 5.0,
    ) -> bool:
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(subscribed_ev.wait()),
                asyncio.create_task(error_ev.wait()),
            ],
            timeout=timeout_s,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

        if error_ev.is_set():
            stop_ev.set()
            return False
        if subscribed_ev.is_set():
            return True

        await self.safe_send_json(
            {
                "message": "OKX subscribe timeout",
                "httpCode": 504,
                "requestGuid": guid,
            }
        )
        stop_ev.set()
        return False

    async def send_error_and_close(self, guid: str | None, http_code: int, message: str):
        try:
            await self.safe_send_json(
                {
                    "message": message,
                    "httpCode": http_code,
                    "requestGuid": guid,
                }
            )
        finally:
            await self.ws.close()

    def _okx_code_as_int(self, code):
        try:
            return int(code)
        except Exception:
            return 500

    async def handle_okx_ws_error(self, guid: str | None, ev: dict):
        okx_code = ev.get("code")
        okx_msg = ev.get("msg") or ev.get("message") or "OKX WS error"
        http_code = self._okx_code_as_int(okx_code)
        await self.send_error_and_close(guid, http_code, okx_msg)

    def replace_sub(self, guid: str, stop: asyncio.Event, channel: str):
        old = self.subs.pop(guid, None)
        if old:
            old.set()
        self.active[channel].append(stop)
        self.subs[guid] = stop

    def unsubscribe_guid(self, guid: str):
        ev = self.subs.pop(guid, None)
        if ev:
            ev.set()
        self.last_sent_ms_book.pop(guid, None)
        self.last_sent_ms_quotes.pop(guid, None)

    async def cleanup(self):
        for lst in self.active.values():
            for ev in lst:
                ev.set()

        for ev in self.subs.values():
            try:
                ev.set()
            except Exception:
                pass
        self.subs.clear()
