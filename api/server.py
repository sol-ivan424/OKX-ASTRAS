# api/server.py

import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from typing import List
import asyncio
import datetime

from api.schemas import Order
from adapters.mock_adapter import MockAdapter
from adapters.okx_adapter import OkxAdapter

from dotenv import load_dotenv
load_dotenv()


def _make_adapter():
    name = os.getenv("ADAPTER", "mock").lower()

    if name == "okx":
        return OkxAdapter(
            api_key=os.getenv("OKX_API_KEY"),
            api_secret=os.getenv("OKX_API_SECRET"),
            api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
        )

    return MockAdapter()


app = FastAPI(title="Astras Crypto Gateway")
adapter = _make_adapter()


def _astras_instrument_simple(d: dict) -> dict:
    """
    Преобразует нейтральный формат инструмента от адаптера в Astras SIMPLE.

    если поля нет — в ответе будет 0.
    Ожидаемые поля от адаптера для OKX:
    symbol, exchange, instType, state, baseCcy, quoteCcy, lotSz, tickSz
    """

    symbol = d["symbol"]
    exchange = d["exchange"]

    inst_type = d.get("instType")
    state = d.get("state")

    lot_sz = d.get("lotSz")
    tick_sz = d.get("tickSz")

    quote_ccy = d.get("quoteCcy")

    price_multiplier = d.get("priceMultiplier")
    if price_multiplier is None:
        price_multiplier = 0

    price_shown_units = d.get("priceShownUnits")
    if price_shown_units is None:
        price_shown_units = 0

    return {
        "symbol": symbol,
        "shortname": symbol,
        "description": d.get("description", 0),
        "exchange": exchange,
        "market": inst_type if inst_type is not None else 0,
        "type": d.get("type", 0),

        "lotsize": lot_sz if lot_sz is not None else 0,
        "facevalue": d.get("facevalue", 0),
        "cfiCode": d.get("cfiCode", 0),
        "cancellation": d.get("cancellation", 0),

        "minstep": tick_sz if tick_sz is not None else 0,
        "rating": d.get("rating", 0),
        "marginbuy": d.get("marginbuy", 0),
        "marginsell": d.get("marginsell", 0),
        "marginrate": d.get("marginrate", 0),

        "pricestep": tick_sz,
        "priceMax": d.get("priceMax", 0),
        "priceMin": d.get("priceMin", 0),
        "theorPrice": d.get("theorPrice", 0),
        "theorPriceLimit": d.get("theorPriceLimit", 0),
        "volatility": d.get("volatility", 0),

        "currency": quote_ccy if quote_ccy is not None else 0,
        "ISIN": d.get("ISIN", 0),
        "yield": d.get("yield", 0),

        "board": inst_type if inst_type is not None else 0,
        "primary_board": inst_type if inst_type is not None else 0,

        "tradingStatus": d.get("tradingStatus", 0),
        "tradingStatusInfo": state if state is not None else 0,

        "complexProductCategory": d.get("complexProductCategory", 0),
        "priceMultiplier": price_multiplier,
        "priceShownUnits": price_shown_units,
    }


# все торговые инструменты
@app.get("/v2/instruments")
async def get_instruments(exchange: str = "MOEX", format: str = "Slim", token: str = None):

    if not token:
        raise HTTPException(400, detail="TokenRequired")

    raw = await adapter.list_instruments()
    return [_astras_instrument_simple(x) for x in raw]


@app.websocket("/stream")
async def stream(ws: WebSocket):
    await ws.accept()

    active: dict[str, list[asyncio.Event]] = {
        "instruments": [],   # пока не используются в opcode
        "quotes": [],
        "book": [],
        "fills": [],
        "orders": [],
        "positions": [],
        "summaries": [],
        "bars": [],
    }

    async def send_wrapped(name: str, payload, guid: str | None = None):
        data = payload.dict() if hasattr(payload, "dict") else payload
        await ws.send_json({"data": data, "guid": guid or f"{name}:req"})

    try:
        while True:
            msg = await ws.receive_json()
            opcode = msg.get("opcode")
            token = msg.get("token")
            symbols: List[str] = msg.get("symbols", [])
            req_guid = msg.get("guid")

            if opcode and (not isinstance(token, str) or not token.strip()):
                await ws.send_json(
                    {
                        "data": {
                            "error": "TokenRequired",
                            "message": "Field 'token' is required for opcode requests",
                        },
                        "guid": req_guid or msg.get("guid"),
                    }
                )
                continue

            # свечи (история + подписка). присылаются и открытые и закрытые свечи (confirm == 0,1)
            if opcode == "BarsGetAndSubscribe":
                stop = asyncio.Event()
                active["bars"].append(stop)

                code = msg.get("code")
                tf = str(msg.get("tf", "60"))
                from_ts = int(msg.get("from", int(time.time())))
                skip_history = bool(msg.get("skipHistory", False))
                split_adjust = bool(msg.get("splitAdjust", True))  #адаптер игнорирует
                guid = msg.get("guid") or req_guid

                exchange = msg.get("exchange")  #адаптер игнорирует
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует
                frequency = msg.get("frequency")  #адаптер игнорирует

                async def send_bar_astras(bar: dict):
                    ts_ms = bar.get("ts", 0)
                    try:
                        t_sec = int(int(ts_ms) / 1000)
                    except Exception:
                        t_sec = 0

                    payload = {
                        "time": t_sec,
                        "close": bar.get("close", 0),
                        "open": bar.get("open", 0),
                        "high": bar.get("high", 0),
                        "low": bar.get("low", 0),
                        "volume": bar.get("volume", 0),
                    }

                    await ws.send_json({
                        "data": payload,
                        "guid": guid,
                    })

                if code:
                    if (not skip_history) and hasattr(adapter, "get_bars_history"):
                        try:
                            history = await adapter.get_bars_history(
                                symbol=code,
                                tf=tf,
                                from_ts=from_ts,
                            )
                            for hbar in history:
                                await send_bar_astras(hbar)
                        except Exception:
                            pass

                    asyncio.create_task(
                        adapter.subscribe_bars(
                            symbol=code,
                            tf=tf,
                            from_ts=from_ts,
                            skip_history=skip_history,
                            split_adjust=split_adjust,
                            on_data=lambda b: asyncio.create_task(send_bar_astras(b)),
                            stop_event=stop,
                        )
                    )
                continue

            # все заявки по портфелю
            if opcode == "OrdersGetAndSubscribeV2":
                stop = asyncio.Event()
                active["orders"].append(stop)

                exchange = msg.get("exchange") or "MOCK"
                portfolio = msg.get("portfolio") or "PORTF"
                statuses = msg.get("orderStatuses") or []
                skip_history = bool(msg.get("skipHistory", False))
                guid = msg.get("guid") or req_guid

                def _order_to_simple(o: Order) -> dict:
                    ts_sec = o.ts / 1000.0
                    tt = datetime.datetime.utcfromtimestamp(ts_sec).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    et = (
                        datetime.datetime.utcfromtimestamp(ts_sec)
                        + datetime.timedelta(days=1)
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    qty = o.quantity
                    px = o.price or 0.0
                    volume = round(px * qty, 5) if px and qty else 0.0
                    return {
                        "id": o.id,
                        "sym": o.symbol,
                        "tic": f"{exchange}:{o.symbol}",
                        "p": portfolio,
                        "ex": exchange,
                        "cmt": None,
                        "t": o.type,
                        "s": o.side,
                        "st": o.status,
                        "tt": tt,
                        "ut": tt,
                        "et": et,
                        "q": qty,
                        "qb": qty,
                        "fq": o.filledQuantity,
                        "fqb": o.filledQuantity,
                        "px": px,
                        "h": False,
                        "tf": "oneday",
                        "i": None,
                        "v": volume,
                    }

                async def on_order_v2(o: Order):
                    if statuses:
                        if o.status not in statuses:
                            return
                    payload = _order_to_simple(o)
                    await ws.send_json({"data": payload, "guid": guid})

                asyncio.create_task(
                    adapter.subscribe_orders(
                        symbols,
                        lambda ord_: asyncio.create_task(on_order_v2(ord_)),
                        stop,
                    )
                )
                continue

            # стакан
            if opcode == "OrderBookGetAndSubscribe":
                stop = asyncio.Event()
                active["book"].append(stop)

                code = msg.get("code")
                group = msg.get("instrumentGroup")
                symbol = code or (symbols[0] if symbols else "")

                guid = msg.get("guid") or req_guid

                async def on_book(b):
                    payload = {
                        "b": [
                            {"p": price, "v": volume}
                            for price, volume in b.bids
                        ],
                        "a": [
                            {"p": price, "v": volume}
                            for price, volume in b.asks
                        ],
                        "t": b.ts,
                        "h": False,
                    }
                    await ws.send_json(
                        {"data": payload, "guid": guid or f"book:{b.symbol}"}
                    )

                if symbol:
                    asyncio.create_task(
                        adapter.subscribe_order_book(
                            [symbol],
                            lambda b: asyncio.create_task(on_book(b)),
                            stop,
                        )
                    )
                continue

            # котировки инструмента
            if opcode == "QuotesSubscribe":
                stop = asyncio.Event()
                active["quotes"].append(stop)

                code = msg.get("code")
                group = msg.get("instrumentGroup")
                symbol = code or (symbols[0] if symbols else "")

                guid = msg.get("guid") or req_guid

                async def on_quote_opcode(q):
                    await send_wrapped(
                        "quotes",
                        q,
                        guid or f"quotes:{getattr(q, 'symbol', '')}",
                    )

                if symbol:
                    asyncio.create_task(
                        adapter.subscribe_quotes(
                            [symbol],
                            lambda q: asyncio.create_task(on_quote_opcode(q)),
                            stop,
                        )
                    )
                continue

            # все сделки по портфелю
            if opcode == "TradesGetAndSubscribeV2":
                stop = asyncio.Event()
                active["fills"].append(stop)

                portfolio = msg.get("portfolio")
                guid = msg.get("guid") or req_guid
                _skip_history = msg.get("skipHistory", False)

                async def on_fill_opcode(fill):
                    await ws.send_json({
                        "data": fill,
                        "guid": guid
                    })

                asyncio.create_task(
                    adapter.subscribe_fills(
                        [portfolio] if portfolio else [],
                        lambda f: asyncio.create_task(on_fill_opcode(f)),
                        stop,
                    )
                )
                continue

            # все позиции портфеля
            if opcode == "PositionsGetAndSubscribeV2":
                stop = asyncio.Event()
                active["positions"].append(stop)

                exchange = msg.get("exchange") or "MOEX"
                portfolio = msg.get("portfolio") or "D39004"
                guid = msg.get("guid") or req_guid
                _skip_history = msg.get("skipHistory", False)

                async def on_pos_opcode(pos):
                    qty = pos.qty or 0.0
                    px = pos.avgPrice or 0.0
                    volume = round(qty * px, 6)

                    payload = {
                        "v": volume,
                        "cv": volume,
                        "sym": pos.symbol,
                        "tic": f"{exchange}:{pos.symbol}",
                        "p": portfolio,
                        "ex": exchange,

                        "pxavg": px,
                        "q": qty,
                        "o": qty,
                        "lot": 1,
                        "n": pos.symbol,

                        "q0": qty,
                        "q1": qty,
                        "q2": qty,
                        "qf": qty,

                        "upd": 0.0,
                        "up": pos.pnl or 0.0,
                        "cur": False,
                        "h": True,
                    }

                    await ws.send_json({"data": payload, "guid": guid})

                asyncio.create_task(
                    adapter.subscribe_positions(
                        symbols,
                        lambda p: asyncio.create_task(on_pos_opcode(p)),
                        stop,
                    )
                )
                continue

    except WebSocketDisconnect:
        for lst in active.values():
            for ev in lst:
                ev.set()
