import asyncio
import time

from api import core
from ..common import WSContext


async def handle_order_book(ctx: WSContext, msg: dict, req_guid: str | None):
    stop = asyncio.Event()
    code = msg.get("code")
    depth = int(msg.get("depth", 20) or 20)
    data_format = msg.get("format")
    data_format_norm = str(data_format).strip().lower()
    frequency = msg.get("frequency")
    try:
        frequency = int(frequency) if frequency is not None else None
    except Exception:
        frequency = None

    min_freq = 10 if data_format_norm == "slim" else 25
    if frequency is None:
        frequency = min_freq
    elif frequency < min_freq:
        frequency = min_freq

    sub_guid = msg.get("guid") or req_guid
    ctx.replace_sub(sub_guid, stop, "book")

    exchange = msg.get("exchange")
    instrument_group = msg.get("instrumentGroup")

    async def on_book(book: dict, _guid: str):
        now_ms = int(time.time() * 1000)
        freq_ms = frequency if isinstance(frequency, int) else 25
        prev_ms = ctx.last_sent_ms_book.get(_guid, 0)
        if freq_ms > 0 and (now_ms - prev_ms) < freq_ms:
            return

        ctx.last_sent_ms_book[_guid] = now_ms
        ms_ts = int(book.get("ts", 0) or 0)
        ts_sec = int(ms_ts / 1000) if ms_ts else 0
        existing_flag = bool(book.get("existing", False))
        bids_in = book.get("bids") or []
        asks_in = book.get("asks") or []

        if depth and depth > 0:
            bids_in = bids_in[:depth]
            asks_in = asks_in[:depth]
        if data_format_norm == "slim":
            payload = {
                "b": [{"p": float(p), "v": float(v)} for (p, v) in bids_in],
                "a": [{"p": float(p), "v": float(v)} for (p, v) in asks_in],
                "t": ms_ts,
                "h": bool(existing_flag),
            }
        elif data_format_norm == "simple":
            payload = {
                "snapshot": existing_flag,
                "bids": [{"price": float(p), "volume": float(v)} for (p, v) in bids_in],
                "asks": [{"price": float(p), "volume": float(v)} for (p, v) in asks_in],
                "timestamp": ts_sec,
                "ms_timestamp": ms_ts,
                "existing": existing_flag,
            }
        await ctx.safe_send_json({"data": payload, "guid": _guid})

    if code:
        subscribed_evt = asyncio.Event()
        error_evt = asyncio.Event()

        def _on_subscribed(_ev: dict):
            subscribed_evt.set()

        def _on_error(ev: dict):
            error_evt.set()
            return asyncio.create_task(ctx.handle_okx_ws_error(sub_guid, ev))

        asyncio.create_task(
            core.adapter.subscribe_order_book(
                symbol=code,
                depth=depth,
                on_data=lambda b, _g=sub_guid: asyncio.create_task(on_book(b, _g)),
                stop_event=stop,
                on_subscribed=_on_subscribed,
                on_error=_on_error,
                unsub_args=[{"channel": "books", "instId": code}],
            )
        )

        if not await ctx.wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
            return
        await ctx.send_ack_200(sub_guid)
