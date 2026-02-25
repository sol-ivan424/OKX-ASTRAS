import asyncio
from typing import List

from api import astras
from api import core
from ..common import WSContext


async def handle_orders(ctx: WSContext, msg: dict, req_guid: str | None, symbols: List[str]):
    stop = asyncio.Event()
    exchange = msg.get("exchange") or "0"
    portfolio = msg.get("portfolio") or "0"
    statuses = msg.get("orderStatuses") or []
    skip_history = bool(msg.get("skipHistory", False))
    sub_guid = msg.get("guid") or req_guid
    ctx.replace_sub(sub_guid, stop, "orders")

    instrument_group = msg.get("instrumentGroup")
    data_format = msg.get("format")
    frequency = msg.get("frequency")
    try:
        frequency = int(frequency) if frequency is not None else 25
    except Exception:
        frequency = 25

    async def send_order_astras(order_any: dict, existing_flag: bool, _guid=sub_guid, apply_status_filter: bool = False):
        payload = astras.astras_order_simple_from_okx_neutral(
            order_any,
            exchange=exchange,
            portfolio=portfolio,
            existing=existing_flag,
        )
        if payload.get("id") == "0" or payload.get("symbol") == "0" or payload.get("status") == "0":
            return
        if apply_status_filter and statuses:
            st = payload.get("status", "0")
            if st not in statuses:
                return

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

    async def _on_live_order(o: dict, _guid: str):
        nonlocal history_done
        if not history_done:
            live_buffer.append(o)
            return
        await send_order_astras(o, False, _guid)

    inst_type_msg = msg.get("instrumentGroup") or msg.get("board")
    inst_type_ws = str(inst_type_msg).strip().upper() if inst_type_msg else ""
    inst_type_rest = str(inst_type_msg).strip().upper() if inst_type_msg else None
    if inst_type_ws == "SPOT":
        inst_types_ws = ["SPOT", "MARGIN"]
    elif inst_type_ws in ("SWAP", "FUTURES", "MARGIN"):
        inst_types_ws = [inst_type_ws]
    else:
        inst_types_ws = ["SPOT", "SWAP", "FUTURES", "MARGIN"]
    unsub_args = [{"channel": "orders", "instType": it} for it in inst_types_ws]

    asyncio.create_task(
        core.adapter.subscribe_orders(
            symbols,
            lambda ord_, _g=sub_guid: asyncio.create_task(_on_live_order(ord_, _g)),
            stop,
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
            if not inst_type_rest:
                inst_types = ["SPOT", "FUTURES", "SWAP", "MARGIN"]
            elif inst_type_rest == "SPOT":
                inst_types = ["SPOT", "MARGIN"]
            else:
                inst_types = [inst_type_rest]
            for it in inst_types:
                history = await core.adapter.get_orders_history(
                    inst_type=it,
                    limit=100,
                )
                for ho in history:
                    await send_order_astras(ho, True, sub_guid, apply_status_filter=True)
        except Exception:
            pass

    if not skip_history:
        try:
            if not inst_type_rest:
                inst_types = ["SPOT", "FUTURES", "SWAP", "MARGIN"]
            elif inst_type_rest == "SPOT":
                inst_types = ["SPOT", "MARGIN"]
            else:
                inst_types = [inst_type_rest]
            for it in inst_types:
                history_orders = await core.adapter.get_orders_pending(
                    inst_type=it,
                    inst_id=(symbols[0] if symbols else None),
                )
                for ho in history_orders:
                    await send_order_astras(ho, True, sub_guid)
        except Exception:
            pass

    history_done = True

    if live_buffer:
        for o in live_buffer:
            await send_order_astras(o, False, sub_guid)
        live_buffer.clear()
