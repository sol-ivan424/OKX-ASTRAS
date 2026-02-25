from fastapi import WebSocketDisconnect

from api import core
from api import idempotency
from api import instruments_cache
from .common import CWSContext


async def handle_delete_opcode(
    ctx: CWSContext,
    msg: dict,
    opcode: str | None,
    req_guid: str | None,
    delete_market_idem: dict,
    delete_limit_idem: dict,
    order_symbol_by_id: dict,
) -> bool:
    if opcode not in ("delete:market", "delete:limit"):
        return False

    order_guid = msg.get("guid") or req_guid
    if not order_guid:
        await ctx.send_error_and_close(None, 400, "guid is required")
        raise WebSocketDisconnect
    okx_client_id = core.okx_client_id(order_guid)

    check_duplicates = msg.get("checkDuplicates", True)
    delete_idempotency = delete_market_idem if opcode == "delete:market" else delete_limit_idem
    if bool(check_duplicates):
        cached = idempotency.get_cached_payload(delete_idempotency, order_guid)
        if cached is not None:
            await ctx.safe_send_json(cached)
            return True

    order_id = str(msg.get("orderId") or "").strip()
    if not order_id:
        await ctx.send_error_and_close(order_guid, 400, "orderId is required")
        raise WebSocketDisconnect

    symbol = idempotency.get_order_symbol(order_symbol_by_id, order_id)
    cancel_inst_type = None
    if symbol:
        instr_for_cancel = await instruments_cache.get_instr(symbol)
        cancel_inst_type = str((instr_for_cancel or {}).get("instType") or "").strip().upper() or None

    try:
        res = await core.adapter.cancel_order_ws(
            symbol=symbol or "",
            order_id=order_id,
            inst_type=cancel_inst_type,
            ws_request_id=okx_client_id,
        )
    except Exception as e:
        err_text = str(e).strip() or type(e).__name__
        await ctx.send_error_and_close(order_guid, 502, err_text)
        raise WebSocketDisconnect

    ord_id = str(res.get("ordId") or order_id or "0")
    out = {
        "requestGuid": order_guid,
        "httpCode": 200,
        "message": f"An order '{ord_id}' has been deleted.",
        "orderNumber": ord_id,
    }
    idempotency.pop_order_symbol(order_symbol_by_id, ord_id)
    if bool(check_duplicates):
        idempotency.put_cached_payload(delete_idempotency, order_guid, out)

    await ctx.safe_send_json(out)
    return True
