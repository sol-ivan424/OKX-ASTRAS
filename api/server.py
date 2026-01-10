# api/server.py

"""Вопросы:


1. в заявках отдаем qty дробным, хотя Astras ждет int32
2. bids[].volume и asks[].volume отдаются float, Astras ждет int64
"""


import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from typing import List
import asyncio
import datetime

from adapters.okx_adapter import OkxAdapter

from dotenv import load_dotenv
load_dotenv()


def _make_adapter():
    name = os.getenv("ADAPTER", "okx").lower()

    if name == "okx":
        return OkxAdapter(
            api_key=os.getenv("OKX_API_KEY"),
            api_secret=os.getenv("OKX_API_SECRET"),
            api_passphrase=os.getenv("OKX_API_PASSPHRASE"),
        )

    raise RuntimeError("Поддерживается только ADAPTER=okx")


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


def _iso_from_unix_ms(ts_ms: int):
    if not ts_ms:
        return None
    try:
        dt = datetime.datetime.fromtimestamp(
            int(ts_ms) / 1000.0,
            tz=datetime.timezone.utc
        )
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        return None


def _astras_order_simple_from_okx_neutral(
    o: dict,
    exchange: str,
    portfolio: str,
    existing: bool,
) -> dict:
    # нормализация статуса OKX -> Astras
    # схема:
    # OKX live / partially_filled -> Astras working
    # OKX filled -> Astras filled
    # OKX canceled -> Astras canceled
    # OKX rejected -> Astras rejected
    # иначе "0"
    def _norm_order_status_okx_to_astras(st: str) -> str:
        s = (st or "").lower()
        if s in ("live", "partially_filled"):
            return "working"
        if s == "filled":
            return "filled"
        if s in ("canceled", "cancelled"):
            return "canceled"
        if s == "rejected":
            return "rejected"
        return "0"

    # нормализация типа OKX -> Astras
    # схема:
    # OKX market* -> Astras market
    # OKX limit* -> Astras limit
    # иначе "0"
    def _norm_order_type_okx_to_astras(t: str) -> str:
        s = (t or "").lower()
        if s.startswith("market"):
            return "market"
        if s.startswith("limit"):
            return "limit"
        return "0"
    
    # нормализация tif OKX -> timeInForce Astras
    # схема:
    # OKX day -> Astras OneDay
    # OKX ioc -> Astras ImmediateOrCancel
    # OKX fok -> Astras FillOrKill
    # OKX gtc -> Astras GoodTillCancelled
    # иначе None
    def _norm_tif_okx_to_astras(tif: str):
        s = (tif or "").lower()
        if s == "day":
            return "OneDay"
        if s == "ioc":
            return "ImmediateOrCancel"
        if s == "fok":
            return "FillOrKill"
        if s == "gtc":
            return "GoodTillCancelled"
        return None
    

    symbol = o.get("symbol", "0")
    price = o.get("price", 0) or 0
    qty = o.get("qty", 0) or 0
    filled = o.get("filled", 0) or 0

    order_type = _norm_order_type_okx_to_astras(str(o.get("type", "0")))
    order_status = _norm_order_status_okx_to_astras(str(o.get("status", "0")))
    time_in_force = _norm_tif_okx_to_astras(str(o.get("tif", "")))

    # brokerSymbol по схеме string/null
    broker_symbol = None
    if exchange != "0" and symbol != "0":
        broker_symbol = f"{exchange}:{symbol}"

    trans_time = _iso_from_unix_ms(int(o.get("ts_create", 0) or 0))
    update_time = _iso_from_unix_ms(int(o.get("ts_update", 0) or 0))

    # volume: для market по схеме должен быть null, для limit можно посчитать price * qty
    if order_type == "market":
        volume = None
    else:
        try:
            volume = float(price) * float(qty)
        except Exception:
            volume = 0

    return {
        "id": o.get("id", "0"),
        "symbol": symbol,
        "brokerSymbol": broker_symbol,
        "portfolio": portfolio,
        "exchange": exchange,

        # comment по схеме string/null, у OKX нет комментария -> null
        "comment": None,

        "type": order_type,
        "side": o.get("side", "0"),
        "status": order_status,

        "transTime": trans_time,
        "updateTime": update_time,

        # endTime по схеме date-time/null, у OKX здесь нет -> null
        "endTime": None,

        # OKX не оперирует "штуками/лотами" как MOEX -> оставляем 0
        "qtyUnits": 0,
        "qtyBatch": 0,

        # qty у OKX может быть дробным, отдаём как есть
        "qty": qty,

        # OKX не оперирует "штуками/лотами" как MOEX -> оставляем 0
        "filledQtyUnits": 0,
        "filledQtyBatch": 0,

        # filled у OKX может быть дробным, отдаём как есть
        "filled": filled,

        "price": price,

        # existing: True — данные из снепшота/истории, False — новые события
        "existing": bool(existing),

        # timeInForce по схеме строка/null, берём из OKX tif и нормализуем под Astras
        "timeInForce": time_in_force,

        # iceberg по схеме object/null, не реализовано -> null
        "iceberg": None,

        "volume": volume,
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

            # все заявки по портфелю (история + подписка) в Astras Simple
            if opcode == "OrdersGetAndSubscribeV2":
                stop = asyncio.Event()
                active["orders"].append(stop)

                exchange = msg.get("exchange") or "0"
                portfolio = msg.get("portfolio") or "0"

                statuses = msg.get("orderStatuses") or []
                skip_history = bool(msg.get("skipHistory", False))
                guid = msg.get("guid") or req_guid

                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует
                frequency = msg.get("frequency")  #адаптер игнорирует

                async def send_order_astras(order_any: dict, existing_flag: bool):
                    payload = _astras_order_simple_from_okx_neutral(
                        order_any,
                        exchange=exchange,
                        portfolio=portfolio,
                        existing=existing_flag,
                    )

                    if statuses:
                        st = payload.get("status", "0")
                        if st not in statuses:
                            return

                    await ws.send_json({"data": payload, "guid": guid})

                # история заявок (pending) через REST, если доступно и если skipHistory=false
                if (not skip_history) and hasattr(adapter, "get_orders_pending"):
                    try:
                        inst_id = symbols[0] if symbols else None
                        history_orders = await adapter.get_orders_pending(
                            inst_type="SPOT",
                            inst_id=inst_id,
                        )
                        for ho in history_orders:
                            await send_order_astras(ho, True)  # existing=True — данные из снепшота/истории (pending REST)
                    except Exception:
                        pass

                asyncio.create_task(
                    adapter.subscribe_orders(
                        symbols,
                        lambda ord_: asyncio.create_task(send_order_astras(ord_, False)),  # existing=False — новые события (private WS orders)
                        stop,
                    )
                )
                continue

            # стакан (Astras Simple)
            if opcode == "OrderBookGetAndSubscribe":
                stop = asyncio.Event()
                active["book"].append(stop)

                code = msg.get("code")
                depth = int(msg.get("depth", 20) or 20)
                frequency = int(msg.get("frequency", 25) or 25)  # минимально для Simple в Astras ~25мс
                guid = msg.get("guid") or req_guid

                # эти поля Astras сейчас не влияют на OKX (оставляем как есть)
                exchange = msg.get("exchange")  #адаптер игнорирует
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует

                # троттлинг: Astras просит отдавать не чаще, чем раз в frequency мс
                last_sent_ms = 0

                async def on_book(book: dict):
                    nonlocal last_sent_ms

                    now_ms = int(time.time() * 1000)
                    if frequency > 0 and (now_ms - last_sent_ms) < frequency:
                        return

                    last_sent_ms = now_ms

                    """" 
                    book (нейтральный формат от адаптера):
                    {
                       "symbol": "...",
                       "ts": <ms>,
                       "bids": [(price, volume), ...],
                       "asks": [(price, volume), ...],
                       "existing": True|False
                    }
                    """
                    ms_ts = int(book.get("ts", 0) or 0)
                    ts_sec = int(ms_ts / 1000) if ms_ts else 0

                    existing_flag = bool(book.get("existing", False))

                    bids_in = book.get("bids") or []
                    asks_in = book.get("asks") or []

                    # применяем глубину на стороне
                    if depth and depth > 0:
                        bids_in = bids_in[:depth]
                        asks_in = asks_in[:depth]

                    bids = [{"price": float(p), "volume": float(v)} for (p, v) in bids_in]
                    asks = [{"price": float(p), "volume": float(v)} for (p, v) in asks_in]

                    payload = {
                        # snapshot и timestamp устаревшие
                        "snapshot": existing_flag,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": ts_sec,
                        "ms_timestamp": ms_ts,
                        "existing": existing_flag,
                    }

                    await ws.send_json({"data": payload, "guid": guid})

                if code:
                    asyncio.create_task(
                        adapter.subscribe_order_book(
                            symbol=code,
                            depth=depth,
                            on_data=lambda b: asyncio.create_task(on_book(b)),
                            stop_event=stop,
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