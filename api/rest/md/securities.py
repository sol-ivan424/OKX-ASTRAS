import time
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from api import astras
from api import core
from api import instruments_cache

router = APIRouter()


@router.get("/md/v2/Securities/{broker_symbol}/quotes")
async def md_quotes(broker_symbol: str):
    symbol = broker_symbol
    if ":" in broker_symbol:
        symbol = broker_symbol.split(":", 1)[1]
    exchange_out = "OKX"
    t = await core.adapter.get_quote_snapshot_rest(symbol=symbol)
    ts_ms = int(t.get("ts", 0) or 0)
    ts_sec = int(ts_ms / 1000) if ts_ms else 0
    last_price = t.get("last", 0) or 0
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
    ob_ms_timestamp = t.get("ob_ts")
    total_bid_vol = t.get("total_bid_vol", 0) or 0
    total_ask_vol = t.get("total_ask_vol", 0) or 0
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
        "ob_ms_timestamp": ob_ms_timestamp,
        "open_price": open_price,
        "yield": None,
        "lotsize": 0,
        "lotvalue": 0,
        "facevalue": 0,
        "type": "0",
        "total_bid_vol": total_bid_vol,
        "total_ask_vol": total_ask_vol,
        "accrued_interest": 0,
        "change": change,
        "change_percent": change_percent,
    }

    return JSONResponse([payload])


@router.get("/md/v2/Securities/{exchange}/{symbol}")
async def md_security(exchange: str, symbol: str, instrumentGroup: str | None = None):
    symbol = symbol.replace("_", "-")
    await instruments_cache.ensure_instr_cache()
    instr = instruments_cache.get_instruments_cache().get(symbol)

    if instr is None:
        raise HTTPException(
            status_code=404,
            detail=f"Instrument '{symbol}' not found on OKX",
        )

    x = astras.astras_instrument_simple(instr)
    try:
        q = await core.adapter.get_quote_snapshot_rest(symbol=symbol)
        x["priceMax"] = q.get("high24h")
        x["priceMin"] = q.get("low24h")
    except Exception:
        pass

    if instrumentGroup:
        x["board"] = instrumentGroup
        x["primary_board"] = instrumentGroup

    return JSONResponse(x)


@router.get("/md/v2/Securities/{exchange}")
async def md_securities(
    exchange: str,
    query: str | None = None,
    limit: int = 200,
    instrumentGroup: str | None = None,
):
    await instruments_cache.ensure_instr_cache()
    items = list(instruments_cache.get_instruments_cache().values())

    if query:
        q = str(query).upper()
        items = [x for x in items if q in str(x.get("symbol", "")).upper()]

    items = items[: max(1, min(limit, 1000))]
    out = await astras.astras_instruments_with_price_limits(items, instrumentGroup)
    if instrumentGroup:
        for x in out:
            x["board"] = instrumentGroup
            x["primary_board"] = instrumentGroup

    return JSONResponse(out)


@router.get("/md/v2/Securities/{exchange}/{symbol}/availableBoards")
async def md_security_available_boards(exchange: str, symbol: str):
    return JSONResponse(core.SUPPORTED_BOARDS)


@router.get("/md/v2/boards")
def md_boards():
    return JSONResponse(core.SUPPORTED_BOARDS)


@router.get("/md/v2/history")
async def md_history(
    symbol: str,
    exchange: str,
    from_: int = Query(alias="from"),
    to: int = Query(...),
    tf: str = "D",
    countBack: int = 300,
):
    tf_in = (tf or "D").upper()
    if tf_in in ("D", "1D"):
        tf_okx = "1D"
        step = 86400
    elif tf_in in ("H", "1H"):
        tf_okx = "1H"
        step = 3600
    elif tf_in in ("M", "1M", "1MIN", "MIN", "1MINS"):
        tf_okx = "1m"
        step = 60
    else:
        tf_okx = tf
        step = None

    items: list[dict] = []
    try:
        raw = await core.adapter.get_bars_history(
            symbol=symbol,
            tf=str(tf_okx),
            from_ts=int(from_ or 0),
        )
        for b in (raw or []):
            ts_ms = int(b.get("ts", 0) or 0)
            t_sec = int(ts_ms / 1000) if ts_ms else 0
            if t_sec and int(to) and t_sec > int(to):
                continue

            v = b.get("volume", 0)

            items.append(
                {
                    "time": t_sec,
                    "close": b.get("close", 0),
                    "open": b.get("open", 0),
                    "high": b.get("high", 0),
                    "low": b.get("low", 0),
                    "volume": v,
                }
            )
    except Exception:
        pass

    items.sort(key=lambda x: x.get("time", 0))

    if countBack and countBack > 0 and len(items) > countBack:
        items = items[-countBack:]

    if step and items:
        first_t = items[0].get("time")
        last_t = items[-1].get("time")
        prev_t = (int(first_t) - step) if first_t else None
        next_t = (int(last_t) + step) if last_t else None
    else:
        prev_t = None
        next_t = None

    return JSONResponse(
        {
            "history": items,
            "next": next_t,
            "prev": prev_t,
        }
    )


@router.get("/md/v2/Securities")
async def md_securities_root(
    exchange: str | None = None,
    query: str | None = None,
    limit: int = 200,
    instrumentGroup: str | None = None,
):
    await instruments_cache.ensure_instr_cache()
    items = list(instruments_cache.get_instruments_cache().values())

    if query:
        q = query.upper()
        items = [x for x in items if q in x.get("symbol", "").upper()]

    items = items[: max(1, min(limit, 1000))]
    out = []
    for raw in items:
        x = astras.astras_instrument_simple(raw)

        if instrumentGroup:
            x["board"] = instrumentGroup
            x["primary_board"] = instrumentGroup
        out.append(x)

    return JSONResponse(out)


@router.get("/md/v2/time")
def md_time():
    return int(time.time())
