import asyncio

from api import core
from ..common import WSContext


async def handle_positions(ctx: WSContext, msg: dict, req_guid: str | None):
    stop = asyncio.Event()
    exchange_out = "OKX"
    portfolio = msg.get("portfolio")
    portfolio_out = portfolio or "DEV_portfolio"
    skip_history = bool(msg.get("skipHistory", False))
    sub_guid = msg.get("guid") or req_guid
    inst_type = msg.get("instrumentGroup") or msg.get("board")
    inst_type_s = str(inst_type).strip().upper() if inst_type else None
    ctx.replace_sub(sub_guid, stop, "positions")

    async def send_pos_astras(p: dict, existing_flag: bool, _guid: str):
        symbol = p.get("symbol")
        qty_units_raw = p.get("qtyUnits")
        avg_price_raw = p.get("avgPrice")
        qty_units = float(qty_units_raw) if qty_units_raw is not None else 0.00
        avg_price = float(avg_price_raw) if avg_price_raw is not None else 0
        volume = 0.00
        current_volume = 0

        payload = {
            "volume": volume,
            "currentVolume": current_volume,
            "symbol": symbol,
            "brokerSymbol": f"{exchange_out}:{symbol}",
            "portfolio": portfolio_out,
            "exchange": exchange_out,
            "avgPrice": avg_price,
            "qtyUnits": qty_units,
            "openUnits": 0,
            "lotSize": float(p.get("lotSize", 0) or 0),
            "shortName": p.get("shortName") or symbol,
            "qtyT0": qty_units,
            "qtyT1": qty_units,
            "qtyT2": qty_units,
            "qtyTFuture": qty_units,
            "qtyT0Batch": 0,
            "qtyT1Batch": 0,
            "qtyT2Batch": 0,
            "qtyTFutureBatch": 0,
            "qtyBatch": 0,
            "openQtyBatch": 0,
            "qty": qty_units,
            "open": 0,
            "dailyUnrealisedPl": 0,
            "unrealisedPl": 0,
            "isCurrency": bool(p.get("isCurrency", False)),
            "existing": bool(existing_flag),
        }

        await ctx.safe_send_json({"data": payload, "guid": _guid})

    subscribed_evt = asyncio.Event()
    error_evt = asyncio.Event()
    history_done = False
    live_buffer: list[dict] = []

    def _on_error(ev: dict):
        error_evt.set()
        return asyncio.create_task(ctx.handle_okx_ws_error(sub_guid, ev))

    async def _on_live_pos(pos: dict, _guid: str):
        nonlocal history_done
        if not history_done:
            live_buffer.append(pos)
            return
        await send_pos_astras(pos, False, _guid)

    def _on_subscribed(_ev: dict):
        subscribed_evt.set()

    unsub_args = [{"channel": "account"}]
    inst_type_u = (inst_type_s or "SPOT").strip().upper()
    if inst_type_u in ("FUTURES", "SWAP", "MARGIN"):
        unsub_args.append({"channel": "positions", "instType": inst_type_u})
    else:
        unsub_args.append({"channel": "positions", "instType": "MARGIN"})

    asyncio.create_task(
        core.adapter.subscribe_positions(
            on_data=lambda p, _g=sub_guid: asyncio.create_task(_on_live_pos(p, _g)),
            stop_event=stop,
            inst_type=inst_type_s,
            on_error=_on_error,
            on_subscribed=_on_subscribed,
            unsub_args=unsub_args,
        )
    )
    if not await ctx.wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
        return
    await ctx.send_ack_200(sub_guid)

    if not skip_history:
        try:
            snap = await core.adapter.get_positions_snapshot(inst_type=inst_type_s)
            for pos in (snap or []):
                await send_pos_astras(pos, True, sub_guid)
        except Exception:
            pass

    history_done = True

    if live_buffer:
        for pos in live_buffer:
            await send_pos_astras(pos, False, sub_guid)
        live_buffer.clear()
