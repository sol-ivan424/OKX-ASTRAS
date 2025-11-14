import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List
import asyncio

from api.schemas import Order
from adapters.mock_adapter import MockAdapter
# from adapters.okx_adapter import OkxAdapter  # подключим позже


# ---------- фабрика выбора адаптера ----------
def _make_adapter():
    name = os.getenv("ADAPTER", "mock").lower()
    if name == "okx":
        # return OkxAdapter(...)
        raise RuntimeError("OkxAdapter is not wired yet")
    return MockAdapter()


app = FastAPI(title="Astras Crypto Gateway")
adapter = _make_adapter()


# ---------- helpers: нормализация к Simple-формату Astras ----------
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
    # если это не дельта (нет изменения цен) — вернём полный объект
    if not (("pxmn" in d) or ("pxmx" in d) or ("priceMin" in d) or ("priceMax" in d)):
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


# ---------- REST ----------
@app.get("/instruments")
async def instruments():
    items = await adapter.list_instruments()
    simple = [_to_simple_full(it) for it in items]
    return {"data": simple, "guid": "instruments:list"}


@app.get("/securities")
async def securities(symbols: str | None = None):
    items = await adapter.list_instruments()
    if symbols:
        want = {s.strip() for s in symbols.split(",")}
        items = [it for it in items if (it.get("symbol") or it.get("sym")) in want]
    simple = [_to_simple_full(it) for it in items]
    return {"data": simple, "guid": "securities:snapshot"}


@app.get("/history")
async def history(symbol: str, tf: str = "1m", limit: int = 100):
    bars = await adapter.get_history(symbol, tf, min(limit, 100))
    return {"data": bars, "guid": f"history:{symbol}:{tf}"}


@app.get("/account")
async def account():
    return (await adapter.get_account_info()).dict()


@app.get("/positions")
async def positions():
    return [p.dict() for p in await adapter.get_positions()]


# рыночная лента сделок через REST (Slim)
@app.get("/trades")
async def trades(symbol: str, exchange: str = "MOCK", limit: int = 100):
    items = await adapter.get_all_trades(exchange, symbol, min(max(limit, 1), 1000))
    out = [it.dict() if hasattr(it, "dict") else it for it in items]
    return {"data": out, "guid": f"alltrades:{exchange}:{symbol}"}


@app.post("/orders")
async def place(o: dict):
    order = Order(
        id="",
        symbol=o["symbol"],
        side=o["side"],
        type=o["type"],
        price=o.get("price"),
        quantity=o["quantity"],
        status="new",
        filledQuantity=0.0,
        ts=0,
    )
    return (await adapter.place_order(order)).dict()


@app.delete("/orders/{oid}")
async def cancel(oid: str, symbol: str):
    return (await adapter.cancel_order(oid, symbol)).dict()


# ---------- WS ----------
@app.websocket("/stream")
async def stream(ws: WebSocket):
    await ws.accept()

    active: dict[str, list[asyncio.Event]] = {
        "instruments": [],
        "quotes": [],
        "book": [],
        "fills": [],
        "orders": [],
        "positions": [],
        "summaries": [],
    }

    async def send_wrapped(name: str, payload, guid: str | None = None):
        data = payload.dict() if hasattr(payload, "dict") else payload
        await ws.send_json({"data": data, "guid": guid or f"{name}:req"})

    try:
        while True:
            msg = await ws.receive_json()
            topic = msg.get("stream")
            opcode = msg.get("opcode")
            token = msg.get("token")
            symbols: List[str] = msg.get("symbols", [])
            req_guid = msg.get("guid")

            # ---------- ALOR-style OPCODE обработка с токеном ----------

            # Для всех opcode требуем непустой token
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

            # OrderBookGetAndSubscribe (биржевой стакан)
            if opcode == "OrderBookGetAndSubscribe":
                stop = asyncio.Event()
                active["book"].append(stop)

                code = msg.get("code")     # например "SBER" или "BTC-USDT"
                group = msg.get("instrumentGroup")  # например "TQBR" или "SPOT"
                # В mock мы игнорируем exchange/board и используем только symbol
                symbol = code or (symbols[0] if symbols else "")

                guid = msg.get("guid") or req_guid

                async def on_book(b):
                    # b — это BookSlim (symbol, bids[(price, qty)], asks[(price, qty)], ts в мс)
                    payload = {
                        "b": [
                            {"p": price, "v": volume}
                            for price, volume in b.bids
                        ],
                        "a": [
                            {"p": price, "v": volume}
                            for price, volume in b.asks
                        ],
                        "t": b.ts,   # ms_timestamp
                        "h": False,  # heavy = false (общий Slim)
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
                # opcode уже обработали, дальше по topic не идём
                continue

            # ---------- subscribe handlers по нашему "stream" протоколу ----------

            if topic == "instruments":
                stop = asyncio.Event()
                active["instruments"].append(stop)

                async def send_inst(raw: dict):
                    if "exchange" in raw or "ex" in raw:
                        await ws.send_json(
                            {"data": _to_simple_full(raw), "guid": req_guid or "instruments:req"}
                        )
                    else:
                        await ws.send_json(
                            {"data": _to_simple_delta(raw), "guid": req_guid or "instruments:req"}
                        )

                asyncio.create_task(adapter.stream_instruments(symbols, send_inst, stop))

            elif topic == "quotes":
                stop = asyncio.Event()
                active["quotes"].append(stop)

                async def on_quote(q):
                    # у QuoteSlim в schemas сейчас поле sym (если поменяешь на symbol — поправь тут)
                    await send_wrapped("quotes", q, req_guid or f"quotes:{getattr(q, 'sym', None) or getattr(q, 'symbol', '')}")

                asyncio.create_task(
                    adapter.subscribe_quotes(
                        symbols,
                        lambda q: asyncio.create_task(on_quote(q)),
                        stop,
                    )
                )

            elif topic == "book":
                stop = asyncio.Event()
                active["book"].append(stop)

                async def on_book(b):
                    # b — это BookSlim (symbol, bids[(price, qty)], asks[(price, qty)], ts в мс)
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
                        {"data": payload, "guid": req_guid or f"book:{b.symbol}"}
                    )

                asyncio.create_task(
                    adapter.subscribe_order_book(
                        symbols,
                        lambda b: asyncio.create_task(on_book(b)),
                        stop,
                    )
                )

            elif topic == "fills":
                stop = asyncio.Event()
                active["fills"].append(stop)

                async def on_fill(d: dict):
                    await ws.send_json({"data": d, "guid": req_guid or "fills:req"})

                asyncio.create_task(
                    adapter.subscribe_fills(
                        symbols,
                        lambda x: asyncio.create_task(on_fill(x)),
                        stop,
                    )
                )

            elif topic == "orders":
                stop = asyncio.Event()
                active["orders"].append(stop)

                async def on_order(o):
                    await send_wrapped("orders", o, req_guid or f"orders:{o.symbol}")

                asyncio.create_task(
                    adapter.subscribe_orders(
                        symbols,
                        lambda o: asyncio.create_task(on_order(o)),
                        stop,
                    )
                )

            elif topic == "positions":
                stop = asyncio.Event()
                active["positions"].append(stop)

                async def on_pos(p):
                    await send_wrapped("positions", p, req_guid or f"positions:{p.symbol}")

                asyncio.create_task(
                    adapter.subscribe_positions(
                        symbols,
                        lambda p: asyncio.create_task(on_pos(p)),
                        stop,
                    )
                )

            elif topic == "summaries":
                stop = asyncio.Event()
                active["summaries"].append(stop)

                async def on_sum(d: dict):
                    await ws.send_json({"data": d, "guid": req_guid or "summaries:req"})

                asyncio.create_task(
                    adapter.subscribe_summaries(
                        lambda d: asyncio.create_task(on_sum(d)),
                        stop,
                    )
                )

            elif topic == "bars":
                # разовая выборка — без регистрации в active
                tf = msg.get("tf", "1m")
                limit = int(msg.get("limit", 50))
                sym = symbols[0] if symbols else "UNKNOWN"
                bars = await adapter.get_history(sym, tf, min(limit, 100))
                for b in bars:
                    await ws.send_json(
                        {"data": b, "guid": req_guid or f"bars:{sym}"}
                    )

            elif topic == "unsubscribe":
                # можно прислать topic (конкретный) или опустить (отписка от всех)
                which = msg.get("topic")  # any from active keys | None
                targets = [which] if which in active else list(active.keys())
                for t in targets:
                    for ev in active[t]:
                        ev.set()
                    active[t].clear()
                await ws.send_json(
                    {"data": {"ok": True, "unsubscribed": targets}, "guid": req_guid or "unsub:ok"}
                )

            # неизвестный stream/opcode — игнорируем

    except WebSocketDisconnect:
        # отписываемся от всего при разрыве
        for lst in active.values():
            for ev in lst:
                ev.set()