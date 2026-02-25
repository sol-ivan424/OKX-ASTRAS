from typing import List
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .common import WSContext
from .control import handle_control_opcode
from .market import handle_market_opcode
from .portfolio import handle_portfolio_opcode

router = APIRouter()


@router.websocket("/ws")
async def ws_stream(ws: WebSocket):
    try:
        await ws.accept()
    except Exception:
        return

    ctx = WSContext(ws)

    try:
        while True:
            try:
                msg = await ws.receive_json()
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception:
                break

            opcode = msg.get("opcode")
            token = msg.get("token")
            symbols: List[str] = msg.get("symbols", [])
            req_guid = msg.get("guid")

            if await handle_control_opcode(ctx, msg, opcode, req_guid):
                continue

            if opcode and (not isinstance(token, str) or not token.strip()):
                await ctx.safe_send_json(
                    {
                        "data": {
                            "error": "TokenRequired",
                            "message": "Field 'token' is required for opcode requests",
                        },
                        "guid": req_guid or msg.get("guid"),
                    }
                )
                continue

            if await handle_market_opcode(ctx, msg, opcode, req_guid, symbols):
                continue

            if await handle_portfolio_opcode(ctx, msg, opcode, req_guid, symbols):
                continue

    except WebSocketDisconnect:
        pass
    finally:
        await ctx.cleanup()


@router.websocket("/stream")
async def stream_alias(ws: WebSocket):
    await ws_stream(ws)
