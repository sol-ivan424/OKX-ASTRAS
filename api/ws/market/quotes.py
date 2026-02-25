import asyncio
import time
from typing import List

from api import core
from ..common import WSContext


async def handle_quotes(ctx: WSContext, msg: dict, req_guid: str | None, symbols: List[str]):
    stop = asyncio.Event()
    code = msg.get("code")
    instrument_group = msg.get("instrumentGroup")
    data_format = msg.get("format")
    frequency = msg.get("frequency")
    try:
        frequency = int(frequency) if frequency is not None else 25
    except Exception:
        frequency = 25

    sub_guid = msg.get("guid") or req_guid
    ctx.replace_sub(sub_guid, stop, "quotes")

    exchange_out = "OKX"
    symbol = code or (symbols[0] if symbols else "")

    async def send_quote_astras(t: dict, _guid: str):
        last_raw = t.get("last")
        bid_raw = t.get("bid")
        ask_raw = t.get("ask")
        if last_raw is None and bid_raw is None and ask_raw is None:
            return

        now_ms = int(time.time() * 1000)
        freq_ms = frequency if isinstance(frequency, int) else 25
        prev_ms = ctx.last_sent_ms_quotes.get(_guid, 0)
        if freq_ms > 0 and (now_ms - prev_ms) < freq_ms:
            return

        ctx.last_sent_ms_quotes[_guid] = now_ms
        ts_ms = int(t.get("ts", 0) or 0)
        ts_sec = int(ts_ms / 1000) if ts_ms else 0
        last_price = last_raw or 0
        bid = t.get("bid", 0) or 0
        ask = t.get("ask", 0) or 0
        bid_sz = t.get("bid_sz", 0) or 0
        ask_sz = t.get("ask_sz", 0) or 0
        high_price = t.get("high24h", 0) or 0
        low_price = t.get("low24h", 0) or 0
        open_price = t.get("open24h", 0) or 0
        volume = t.get("vol24h", 0) or 0
        if open_price > 0:
            change = last_price - open_price
            change_percent = (change / open_price) * 100.0
        else:
            change = 0.0
            change_percent = 0.0

        payload = {
            "symbol": symbol,
            "exchange": exchange_out,
            "description": None,
            "prev_close_price": open_price,
            "last_price": last_price,
            "last_price_timestamp": ts_sec,
            "high_price": high_price,
            "low_price": low_price,
            "accruedInt": 0,
            "volume": volume,
            "open_interest": None,
            "ask": ask,
            "bid": bid,
            "ask_vol": ask_sz,
            "bid_vol": bid_sz,
            "ob_ms_timestamp": None,
            "open_price": open_price,
            "yield": None,
            "lotsize": 0,
            "lotvalue": 0,
            "facevalue": 0,
            "type": "0",
            "total_bid_vol": 0,
            "total_ask_vol": 0,
            "accrued_interest": 0,
            "change": change,
            "change_percent": change_percent,
        }

        await ctx.safe_send_json({"data": payload, "guid": _guid})

    if symbol:
        unsub_args = [{"channel": "tickers", "instId": symbol}]
        subscribed_evt = asyncio.Event()
        error_evt = asyncio.Event()
        history_done = False
        live_buffer: list[dict] = []

        def _on_subscribed(_ev: dict):
            subscribed_evt.set()

        def _on_error(ev: dict):
            error_evt.set()
            return asyncio.create_task(ctx.handle_okx_ws_error(sub_guid, ev))

        async def _on_live_quote(q: dict, _guid: str):
            nonlocal history_done
            if not history_done:
                live_buffer.append(q)
                return
            await send_quote_astras(q, _guid)

        asyncio.create_task(
            core.adapter.subscribe_quotes(
                symbol=symbol,
                on_data=lambda q, _g=sub_guid: asyncio.create_task(_on_live_quote(q, _g)),
                stop_event=stop,
                on_subscribed=_on_subscribed,
                on_error=_on_error,
                unsub_args=unsub_args,
            )
        )

        if not await ctx.wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
            return

        await ctx.send_ack_200(sub_guid)

        history_done = True

        if live_buffer:
            for q in live_buffer:
                await send_quote_astras(q, sub_guid)
            live_buffer.clear()
