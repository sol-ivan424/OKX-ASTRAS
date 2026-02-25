from fastapi import WebSocketDisconnect

from api import core
from api import idempotency
from .common import CWSContext


async def handle_create_opcode(
    ctx: CWSContext,
    msg: dict,
    opcode: str | None,
    req_guid: str | None,
    market_idem: dict,
    limit_idem: dict,
    order_symbol_by_id: dict,
) -> bool:
    if opcode == "create:market":
        order_guid = msg.get("guid") or req_guid
        if not order_guid:
            await ctx.send_error_and_close(None, 400, "guid is required")
            raise WebSocketDisconnect
        okx_client_id = core.okx_client_id(order_guid)

        check_duplicates = msg.get("checkDuplicates", True)
        if bool(check_duplicates):
            cached = idempotency.get_cached_payload(market_idem, order_guid)
            if cached is not None:
                await ctx.safe_send_json(cached)
                return True

        side = (msg.get("side") or "").lower()
        qty = msg.get("quantity")
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
        tgt_ccy = "base_ccy" if (inst_type_s == "SPOT" and side == "buy") else None
        ccy = await core.resolve_order_ccy(symbol, inst_type_s, side, allow_margin)
        if inst_type_s in ("FUTURES", "SWAP"):
            td_mode = "cross"
        elif inst_type_s == "SPOT":
            td_mode = "cross" if allow_margin else "cash"

        try:
            res = await core.adapter.place_market_order_ws(
                symbol=symbol,
                side=side,
                quantity=qty,
                inst_type=inst_type_s,
                tgt_ccy=tgt_ccy,
                ccy=ccy,
                td_mode=td_mode,
                cl_ord_id=okx_client_id,
                ws_request_id=okx_client_id,
            )
        except Exception as e:
            err_text = str(e).strip() or type(e).__name__
            await ctx.send_error_and_close(order_guid, 502, err_text)
            raise WebSocketDisconnect

        ord_id = str(res.get("ordId") or "0")
        out = {
            "requestGuid": order_guid,
            "httpCode": 200,
            "message": f"An order '{ord_id}' has been created.",
            "orderNumber": ord_id,
        }
        idempotency.set_order_symbol(order_symbol_by_id, ord_id, symbol)
        if bool(check_duplicates):
            idempotency.put_cached_payload(market_idem, order_guid, out)

        await ctx.safe_send_json(out)
        return True

    if opcode == "create:limit":
        order_guid = msg.get("guid") or req_guid
        if not order_guid:
            await ctx.send_error_and_close(None, 400, "guid is required")
            raise WebSocketDisconnect
        okx_client_id = core.okx_client_id(order_guid)

        check_duplicates = msg.get("checkDuplicates", True)
        if bool(check_duplicates):
            cached = idempotency.get_cached_payload(limit_idem, order_guid)
            if cached is not None:
                await ctx.safe_send_json(cached)
                return True

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

        tif = str(msg.get("timeInForce") or "OneDay").lower()
        if tif in ("oneday", "goodtillcancelled"):
            okx_ord_type = "limit"
        elif tif == "immediateorcancel":
            okx_ord_type = "ioc"
        elif tif == "fillorkill":
            okx_ord_type = "fok"
        elif tif == "bookorcancel":
            okx_ord_type = "post_only"
        elif tif == "attheclose":
            await ctx.send_error_and_close(order_guid, 400, "timeInForce attheclose is not supported for OKX")
            raise WebSocketDisconnect
        else:
            await ctx.send_error_and_close(order_guid, 400, f"Unsupported timeInForce: {tif}")
            raise WebSocketDisconnect

        if inst_type_s in ("FUTURES", "SWAP"):
            td_mode = "cross"
        else:
            td_mode = "cross" if allow_margin else "cash"

        try:
            res = await core.adapter.place_limit_order_ws(
                symbol=symbol,
                side=side,
                quantity=qty,
                price=price,
                inst_type=inst_type_s,
                ord_type=okx_ord_type,
                td_mode=td_mode,
                ccy=ccy,
                cl_ord_id=okx_client_id,
                ws_request_id=okx_client_id,
            )
        except Exception as e:
            err_text = str(e).strip() or type(e).__name__
            await ctx.send_error_and_close(order_guid, 502, err_text)
            raise WebSocketDisconnect

        ord_id = str(res.get("ordId") or "0")
        out = {
            "requestGuid": order_guid,
            "httpCode": 200,
            "message": f"An order '{ord_id}' has been created.",
            "orderNumber": ord_id,
        }
        idempotency.set_order_symbol(order_symbol_by_id, ord_id, symbol)
        if bool(check_duplicates):
            idempotency.put_cached_payload(limit_idem, order_guid, out)

        await ctx.safe_send_json(out)
        return True

    return False
