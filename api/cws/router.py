from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api import idempotency
from .common import CWSContext
from .control import handle_control_opcode
from .create import handle_create_opcode
from .delete import handle_delete_opcode
from .update import handle_update_opcode

router = APIRouter()


@router.websocket("/cws")
async def cws_stream(ws: WebSocket):
    try:
        await ws.accept()
    except Exception:
        return

    ctx = CWSContext(ws)

    try:
        market_idem = idempotency.cws_market_idempotency()
        limit_idem = idempotency.cws_limit_idempotency()
        delete_market_idem = idempotency.cws_delete_market_idempotency()
        delete_limit_idem = idempotency.cws_delete_limit_idempotency()
        update_limit_idem = idempotency.cws_update_limit_idempotency()
        order_symbol_by_id = idempotency.cws_order_symbol_by_id()

        while True:
            try:
                msg = await ws.receive_json()
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception:
                break

            opcode = msg.get("opcode")
            req_guid = msg.get("guid")
            idempotency.maybe_cleanup()

            if await handle_control_opcode(ctx, msg, opcode, req_guid):
                continue

            if await handle_create_opcode(
                ctx,
                msg,
                opcode,
                req_guid,
                market_idem,
                limit_idem,
                order_symbol_by_id,
            ):
                continue

            if await handle_delete_opcode(
                ctx,
                msg,
                opcode,
                req_guid,
                delete_market_idem,
                delete_limit_idem,
                order_symbol_by_id,
            ):
                continue

            if await handle_update_opcode(
                ctx,
                msg,
                opcode,
                req_guid,
                update_limit_idem,
                order_symbol_by_id,
            ):
                continue

    except WebSocketDisconnect:
        pass
