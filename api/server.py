import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from typing import List
import asyncio
import datetime

from api.schemas import Order
from adapters.mock_adapter import MockAdapter
from adapters.okx_adapter import OkxAdapter


def _make_adapter():
    name = os.getenv("ADAPTER", "mock").lower()
    if name == "okx":
        return OkxAdapter()
        #raise RuntimeError("OkxAdapter is not wired yet")
    return MockAdapter()


app = FastAPI(title="Astras Crypto Gateway")
adapter = _make_adapter()


def _to_simple_full(d: dict) -> dict:
    return {
        "symbol": d.get("symbol") or d.get("sym"),
        "exchange": d.get("exchange") or d.get("ex"),
        "board": d.get("board") or d.get("bd") or "SPOT",
        "tradingStatus": d.get("tradingStatus") if "tradingStatus" in d else d.get("st", 17),
        "tradingStatusInfo": d.get("tradingStatusInfo") if "tradingStatusInfo" in d else d.get("sti", "normal trading"),
        "priceMin": d.get("priceMin") if "priceMin" in d else d.get("pxmn"),
        "priceMax": d.get("priceMax") if "priceMax" in d else d.get("pxmx"),
    }


def _to_simple_delta(d: dict) -> dict:
    if not (
        ("pxmn" in d)
        or ("pxmx" in d)
        or ("priceMin" in d)
        or ("priceMax" in d)
    ):
        return _to_simple_full(d)
    out = {"symbol": d.get("symbol") or d.get("sym")}
    if "pxmn" in d:
        out["priceMin"] = d["pxmn"]
    if "pxmx" in d:
        out["priceMax"] = d["pxmx"]
    if "priceMin" in d:
        out["priceMin"] = d["priceMin"]
    if "priceMax" in d:
        out["priceMax"] = d["priceMax"]
    return out

# все торговые инструменты
@app.get("/v2/instruments")
async def get_instruments(exchange: str = "MOEX", format: str = "Slim", token: str = None):

    if not token:
        raise HTTPException(400, detail="TokenRequired")

    raw = await adapter.list_instruments()
    return raw


@app.websocket("/stream")
async def stream(ws: WebSocket):
    await ws.accept()

    active: dict[str, list[asyncio.Event]] = {
        "instruments": [],   # пока не используются в opcode, оставлены на будущее
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

            # свечи (история)
            if opcode == "BarsGetAndSubscribe":
                stop = asyncio.Event()
                active["bars"].append(stop)

                code = msg.get("code")
                tf = str(msg.get("tf", "60"))
                from_ts = int(msg.get("from", int(time.time())))
                skip_history = bool(msg.get("skipHistory", False))
                split_adjust = bool(msg.get("splitAdjust", True))
                guid = msg.get("guid") or req_guid

                async def on_bar(bar: dict):
                    await ws.send_json({
                        "data": bar,
                        "guid": guid,
                    })

                if code:
                    asyncio.create_task(
                        adapter.subscribe_bars(
                            symbol=code,
                            tf=tf,
                            from_ts=from_ts,
                            skip_history=skip_history,
                            split_adjust=split_adjust,
                            on_data=lambda b: asyncio.create_task(on_bar(b)),
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
            
                        # === PositionsGetAndSubscribeV2 (позиции по портфелю, Slim-формат) ===

            # все позиции портфеля
            if opcode == "PositionsGetAndSubscribeV2":
                stop = asyncio.Event()
                active["positions"].append(stop)

                # параметры запроса Astras
                exchange = msg.get("exchange") or "MOEX"
                portfolio = msg.get("portfolio") or "D39004"
                guid = msg.get("guid") or req_guid
                _skip_history = msg.get("skipHistory", False)  # в mock игнорируем
                # format в mock тоже игнорируем и всегда отдаём Slim

                async def on_pos_opcode(pos):
                    # pos — это наш api.schemas.Position (symbol, qty, avgPrice, pnl, ts)
                    qty = pos.qty or 0.0
                    px = pos.avgPrice or 0.0
                    volume = round(qty * px, 6)

                    payload = {
                        "v": volume,                       # стоимость позиции
                        "cv": volume,                      # текущая стоимость (в mock = v)
                        "sym": pos.symbol,                 # тикер, например "APTK"
                        "tic": f"{exchange}:{pos.symbol}", # "MOEX:APTK"
                        "p": portfolio,                    # портфель
                        "ex": exchange,                    # биржа

                        "pxavg": px,                       # средняя цена
                        "q": qty,                          # текущий остаток
                        "o": qty,                          # исходный объём (в mock = q)
                        "lot": 1,                          # размер лота (захардкожен)
                        "n": pos.symbol,                   # имя инструмента (в mock = тикер)

                        "q0": qty,                         # нач. остаток
                        "q1": qty,                         # на начало дня
                        "q2": qty,                         # на конец дня
                        "qf": qty,                         # фактический остаток

                        "upd": 0.0,                        # изменение стоимости (mock)
                        "up": pos.pnl or 0.0,              # PnL
                        "cur": False,                      # валютная? (mock)
                        "h": True,                         # позиция активна
                    }

                    await ws.send_json({"data": payload, "guid": guid})

                asyncio.create_task(
                    adapter.subscribe_positions(
                        symbols,  # если пусто — mock использует все инструменты
                        lambda p: asyncio.create_task(on_pos_opcode(p)),
                        stop,
                    )
                )
                continue

            # другие opcode...
            
            # неизвестный opcode игнорируем

    except WebSocketDisconnect:
        for lst in active.values():
            for ev in lst:
                ev.set()
