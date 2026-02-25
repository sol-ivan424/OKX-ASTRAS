from fastapi import WebSocket
from starlette.websockets import WebSocketState


class CWSContext:
    def __init__(self, ws: WebSocket):
        self.ws = ws

    async def safe_send_json(self, payload: dict):
        if self.ws.client_state != WebSocketState.CONNECTED:
            return
        try:
            await self.ws.send_json(payload)
        except Exception:
            return

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
