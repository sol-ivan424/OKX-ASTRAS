from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List
import asyncio

from api.schemas import Order
from adapters.mock_adapter import MockAdapter

app = FastAPI(title="Astras Crypto Gateway")
adapter = MockAdapter()

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
    if "pxmn" in d: out["priceMin"] = d["pxmn"]
    if "pxmx" in d: out["priceMax"] = d["pxmx"]
    if "priceMin" in d: out["priceMin"] = d["priceMin"]
    if "priceMax" in d: out["priceMax"] = d["priceMax"]
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
    active = {"instruments": None}  # asyncio.Event

    async def send_wrapped(name: str, payload):
        data = payload.dict() if hasattr(payload, "dict") else payload
        sym = data.get("symbol") or data.get("sym") or "multi"
        await ws.send_json({"data": data, "guid": f"{name}:{sym}"})

    try:
        while True:
            msg = await ws.receive_json()
            stream = msg.get("stream")
            symbols: List[str] = msg.get("symbols", [])

            if stream == "quotes":
                await adapter.subscribe_quotes(
                    symbols, lambda d: asyncio.create_task(send_wrapped("quotes", d))
                )

            elif stream == "book":
                await adapter.subscribe_order_book(
                    symbols, lambda d: asyncio.create_task(send_wrapped("book", d))
                )

            elif stream == "trades":
                await adapter.subscribe_trades(
                    symbols, lambda d: asyncio.create_task(send_wrapped("trades", d))
                )

            elif stream == "bars":
                tf = msg.get("tf", "1m")
                limit = int(msg.get("limit", 50))
                sym = symbols[0] if symbols else "UNKNOWN"
                bars = await adapter.get_history(sym, tf, min(limit, 100))
                for b in bars:
                    await ws.send_json({"data": b, "guid": f"bars:{sym}"})

            elif stream == "instruments":
                # остановим прошлую подписку
                if active["instruments"] is not None:
                    active["instruments"].set()
                    active["instruments"] = None

                req_guid = msg.get("guid") or "instruments:req"
                stop_event = asyncio.Event()
                active["instruments"] = stop_event

                # помним, по каким символам уже отправили FULL
                seen_full = set()

                async def send_inst(raw: dict):
                    sym = raw.get("symbol") or raw.get("sym")
                    if sym not in seen_full:
                        payload = _to_simple_full(raw)   # первое сообщение — FULL
                        seen_full.add(sym)
                    else:
                        payload = _to_simple_delta(raw)  # далее — DELTA
                    await ws.send_json({"data": payload, "guid": req_guid})

                asyncio.create_task(adapter.stream_instruments(symbols, send_inst, stop_event))

            elif stream == "unsubscribe":
                if msg.get("topic") == "instruments" and active["instruments"] is not None:
                    active["instruments"].set()
                    active["instruments"] = None
                    await ws.send_json({"data": {"ok": True}, "guid": "instruments:unsub"})

    except WebSocketDisconnect:
        if active["instruments"] is not None:
            active["instruments"].set()







                                                    #котировки; bid - цена по которой кто-то готов купить; ask - цена ... продать
                                                    #стакан
                                                    #сделки