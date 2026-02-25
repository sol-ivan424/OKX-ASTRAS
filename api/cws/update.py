from fastapi import WebSocketDisconnect

from api import core
from api import idempotency
from api import instruments_cache
from .common import CWSContext


async def handle_update_opcode(
    ctx: CWSContext,
    msg: dict,
    opcode: str | None,
    req_guid: str | None,
    update_limit_idem: dict,
    order_symbol_by_id: dict,
) -> bool:
    if opcode != "update:limit":
        return False

    order_guid = msg.get("guid") or req_guid
    if not order_guid:
        await ctx.send_error_and_close(None, 400, "guid is required")
        raise WebSocketDisconnect
    okx_client_id = core.okx_client_id(order_guid)

    check_duplicates = msg.get("checkDuplicates", True)
    if bool(check_duplicates):
        cached = idempotency.get_cached_payload(update_limit_idem, order_guid)
        if cached is not None:
            await ctx.safe_send_json(cached)
            return True

    order_id = str(msg.get("orderId") or "").strip()
    if not order_id:
        await ctx.send_error_and_close(order_guid, 400, "orderId is required")
        raise WebSocketDisconnect

    side = (msg.get("side") or "").lower()
    qty = msg.get("quantity")
    price = msg.get("price")
    instr = msg.get("instrument") or {}
    symbol = instr.get("symbol")
    inst_type = (
        instr.get("instrumentGroup")
        or instr.get("board")
        or msg.get("instrumentGroup")
        or msg.get("board")
    )
    allow_margin = bool(msg.get("allowMargin", False))
    inst_type_s = str(inst_type).strip().upper() if inst_type is not None else ""
    ccy = await core.resolve_order_ccy(symbol, inst_type_s, side, allow_margin)

    if inst_type_s in ("FUTURES", "SWAP"):
        td_mode = "cross"
    else:
        td_mode = "cross" if allow_margin else "cash"

    cancel_req_id = core.okx_client_id(f"{order_guid}:cancel")
    new_req_id = core.okx_client_id(f"{order_guid}:new")

    cancel_symbol = symbol or idempotency.get_order_symbol(order_symbol_by_id, order_id)
    cancel_inst_type = None
    if cancel_symbol:
        instr_for_cancel = await instruments_cache.get_instr(cancel_symbol)
        cancel_inst_type = str((instr_for_cancel or {}).get("instType") or "").strip().upper() or None

    try:
        await core.adapter.cancel_order_ws(
            symbol=cancel_symbol,
            order_id=order_id,
            inst_type=cancel_inst_type,
            ws_request_id=cancel_req_id,
        )
    except Exception as e:
        err_text = str(e).strip() or type(e).__name__
        await ctx.send_error_and_close(order_guid, 400, err_text)
        raise WebSocketDisconnect

    try:
        res = await core.adapter.place_limit_order_ws(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=price,
            inst_type=inst_type_s,
            ord_type="limit",
            td_mode=td_mode,
            ccy=ccy,
            cl_ord_id=okx_client_id,
            ws_request_id=new_req_id,
        )
    except Exception as e:
        err_text = str(e).strip() or type(e).__name__
        await ctx.send_error_and_close(order_guid, 400, err_text)
        raise WebSocketDisconnect

    new_ord_id = str(res.get("ordId") or "0")
    out = {
        "requestGuid": order_guid,
        "httpCode": 200,
        "message": f"An order has been updated. New order ID is '{new_ord_id}'.",
        "orderNumber": new_ord_id,
    }
    idempotency.pop_order_symbol(order_symbol_by_id, order_id)
    idempotency.set_order_symbol(order_symbol_by_id, new_ord_id, symbol)
    if bool(check_duplicates):
        idempotency.put_cached_payload(update_limit_idem, order_guid, out)

    await ctx.safe_send_json(out)
    return True
