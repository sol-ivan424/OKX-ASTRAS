import asyncio
import time

from api import core
from ..common import WSContext


async def handle_bars(ctx: WSContext, msg: dict, req_guid: str | None):
    sub_guid = msg.get("guid") or req_guid

    stop = asyncio.Event()
    ctx.replace_sub(sub_guid, stop, "bars")

    code = msg.get("code")
    tf = str(msg.get("tf", "60"))
    from_ts = int(msg.get("from", int(time.time())))
    skip_history = bool(msg.get("skipHistory", False))
    split_adjust = bool(msg.get("splitAdjust", True))

    exchange = msg.get("exchange")
    instrument_group = msg.get("instrumentGroup")
    data_format = msg.get("format")
    frequency = msg.get("frequency")

    async def send_bar_astras(bar: dict, _guid: str):
        o = bar.get("open")
        h = bar.get("high")
        l = bar.get("low")
        c = bar.get("close")
        if o is None or h is None or l is None or c is None:
            return

        ts_ms = bar.get("ts", 0)
        try:
            t_sec = int(int(ts_ms) / 1000)
        except Exception:
            return

        vol = bar.get("volume", 0)

        payload = {
            "time": t_sec,
            "close": c,
            "open": o,
            "high": h,
            "low": l,
            "volume": vol,
        }

        await ctx.safe_send_json({"data": payload, "guid": _guid})

    if code:
        unsub_args = [{
            "channel": core.adapter.tf_to_okx_ws_channel(tf),
            "instId": code,
        }]
        subscribed_evt = asyncio.Event()
        error_evt = asyncio.Event()

        history_done = False
        live_buffer: list[dict] = []

        def _on_subscribed(_ev: dict):
            subscribed_evt.set()

        def _on_error(ev: dict):
            error_evt.set()
            return asyncio.create_task(ctx.handle_okx_ws_error(sub_guid, ev))

        async def _on_live_bar(b: dict, _guid: str):
            nonlocal history_done
            if not history_done:
                live_buffer.append(b)
                return
            await send_bar_astras(b, _guid)

        asyncio.create_task(
            core.adapter.subscribe_bars(
                symbol=code,
                tf=tf,
                from_ts=from_ts,
                skip_history=skip_history,
                split_adjust=split_adjust,
                inst_type=instrument_group,
                on_data=lambda b, _g=sub_guid: asyncio.create_task(_on_live_bar(b, _g)),
                stop_event=stop,
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
                history = await core.adapter.get_bars_history(
                    symbol=code,
                    tf=tf,
                    from_ts=from_ts,
                    inst_type=instrument_group,
                )

                history.sort(key=lambda x: int(x.get("ts", 0)))

                for hbar in history:
                    await send_bar_astras(hbar, sub_guid)
            except Exception:
                pass

        history_done = True

        if live_buffer:
            for b in live_buffer:
                await send_bar_astras(b, sub_guid)
            live_buffer.clear()
