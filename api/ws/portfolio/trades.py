import asyncio

from api import astras
from api import core
from ..common import WSContext


async def handle_trades(ctx: WSContext, msg: dict, req_guid: str | None):
    stop = asyncio.Event()
    portfolio = msg.get("portfolio")
    exchange_out = "OKX"
    skip_history = bool(msg.get("skipHistory", False))
    sub_guid = msg.get("guid") or req_guid
    ctx.replace_sub(sub_guid, stop, "fills")

    async def send_trade_astras(t: dict, existing_flag: bool, _guid: str):
        if str(t.get("id", "0")) == "0" or str(t.get("symbol", "0")) == "0":
            return
        ts_ms = int(t.get("ts", 0) or 0)
        date_iso = astras.iso_from_unix_ms(ts_ms) if ts_ms else None
        symbol = t.get("symbol") or "N/A"
        qty = float(t.get("qty", 0) or 0)
        price = float(t.get("price", 0) or 0)
        payload = {
            "id": str(t.get("id", "0")),
            "orderno": str(t.get("orderno", "0")),
            "comment": None,
            "symbol": symbol,
            "brokerSymbol": symbol,
            "exchange": exchange_out,
            "date": date_iso,
            "board": None,
            "qtyUnits": 0,
            "qtyBatch": 0,
            "qty": qty,
            "price": price,
            "accruedInt": 0,
            "side": t.get("side", "0"),
            "existing": bool(existing_flag),
            "commission": t.get("commission", 0) or 0,
            "repoSpecificFields": None,
            "settleDate": None,
            "volume": t.get("volume", 0) or 0,
            "value": 0,
        }
        await ctx.safe_send_json({"data": payload, "guid": _guid})

    subscribed_evt = asyncio.Event()
    error_evt = asyncio.Event()
    history_done = False
    live_buffer: list[dict] = []

    def _on_subscribed(_ev: dict):
        subscribed_evt.set()

    def _on_error(ev: dict):
        error_evt.set()
        return asyncio.create_task(ctx.handle_okx_ws_error(sub_guid, ev))

    async def _on_live_trade(tr: dict, _guid: str):
        nonlocal history_done
        if not history_done:
            live_buffer.append(tr)
            return
        await send_trade_astras(tr, False, _guid)

    inst_type_msg = msg.get("instrumentGroup") or msg.get("board")
    inst_type_ws = str(inst_type_msg).strip().upper() if inst_type_msg else ""
    inst_type_rest = str(inst_type_msg).strip().upper() if inst_type_msg else None
    inst_types_ws = [inst_type_ws] if inst_type_ws in ("SPOT", "SWAP", "FUTURES") else ["SPOT", "SWAP", "FUTURES"]
    unsub_args = [{"channel": "orders", "instType": it} for it in inst_types_ws]

    asyncio.create_task(
        core.adapter.subscribe_trades(
            on_data=lambda tr, _g=sub_guid: asyncio.create_task(_on_live_trade(tr, _g)),
            stop_event=stop,
            inst_type=inst_type_ws,
            on_subscribed=_on_subscribed,
            on_error=_on_error,
            unsub_args=unsub_args,
        )
    )

    if not await ctx.wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
        return
    await ctx.send_ack_200(sub_guid)

    if not skip_history:
        try:
            history = await core.adapter.get_trades_history(inst_type=inst_type_rest, limit=100)
            for tr in history:
                await send_trade_astras(tr, True, sub_guid)
        except Exception:
            pass

    history_done = True

    if live_buffer:
        for tr in live_buffer:
            await send_trade_astras(tr, False, sub_guid)
        live_buffer.clear()
