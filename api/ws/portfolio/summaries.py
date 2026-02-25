import asyncio

from api import core
from ..common import WSContext


async def handle_summaries(ctx: WSContext, msg: dict, req_guid: str | None):
    stop = asyncio.Event()
    portfolio = msg.get("portfolio")
    skip_history = bool(msg.get("skipHistory", False))
    sub_guid = msg.get("guid") or req_guid
    ctx.replace_sub(sub_guid, stop, "summaries")

    async def send_summary_astras(s: dict, _guid: str):
        total_eq = float(s.get("totalEq", 0) or 0)
        adj_eq = float(s.get("adjEq", 0) or 0)
        avail_eq = float(s.get("availEq", 0) or 0)
        imr = float(s.get("imr", 0) or 0)
        by_ccy = s.get("byCcy") or []
        buying_power_by_ccy = []
        details_imr_sum = 0.0
        for x in by_ccy:
            ccy = x.get("ccy")
            if not ccy:
                continue
            details_imr_sum += float(x.get("imr", 0) or 0)
            buying_power_by_ccy.append(
                {
                    "currency": str(ccy),
                    "buyingPower": float(x.get("availEq", 0) or 0),
                }
            )
        initial_margin = imr if imr != 0 else details_imr_sum
        payload = {
            "buyingPowerAtMorning": 0,
            "buyingPower": avail_eq,
            "profit": 0,
            "profitRate": 0,
            "portfolioEvaluation": adj_eq,
            "portfolioLiquidationValue": total_eq,
            "initialMargin": initial_margin,
            "correctedMargin": 0,
            "riskBeforeForcePositionClosing": 0,
            "commission": 0,
            "buyingPowerByCurrency": buying_power_by_ccy,
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

    async def _on_live_summary(s: dict, _guid: str):
        nonlocal history_done
        if not history_done:
            live_buffer.append(s)
            return
        await send_summary_astras(s, _guid)

    asyncio.create_task(
        core.adapter.subscribe_summaries(
            on_data=lambda s, _g=sub_guid: asyncio.create_task(_on_live_summary(s, _g)),
            stop_event=stop,
            on_subscribed=_on_subscribed,
            on_error=_on_error,
            unsub_args=[{"channel": "account"}],
        )
    )

    if not await ctx.wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
        return
    await ctx.send_ack_200(sub_guid)

    if not skip_history:
        try:
            snap = await core.adapter.get_summaries_snapshot()
            await send_summary_astras(snap or {}, sub_guid)
        except Exception:
            pass

    history_done = True

    if live_buffer:
        for s in live_buffer:
            await send_summary_astras(s, sub_guid)
        live_buffer.clear()
