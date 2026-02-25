import datetime
from typing import Callable, Awaitable
from api import instruments_cache


def astras_instrument_simple(d: dict) -> dict:
    return {
        "symbol": d.get("symbol"),
        "shortname": d.get("symbol"),
        "description": d.get("description"),
        "exchange": d.get("exchange"),
        "market": d.get("instType"),
        "type": d.get("type"),
        "lotsize": d.get("lotSz"),
        "facevalue": d.get("facevalue"),
        "cfiCode": d.get("cfiCode"),
        "cancellation": d.get("cancellation"),
        "minstep": d.get("tickSz"),
        "rating": d.get("rating"),
        "marginbuy": d.get("marginbuy"),
        "marginsell": d.get("marginsell"),
        "marginrate": d.get("marginrate"),
        "pricestep": 0,
        "priceMax": d.get("priceMax"),
        "priceMin": d.get("priceMin"),
        "theorPrice": d.get("theorPrice"),
        "theorPriceLimit": d.get("theorPriceLimit"),
        "volatility": d.get("volatility"),
        "currency": d.get("quoteCcy"),
        "ISIN": d.get("ISIN"),
        "yield": d.get("yield"),
        "board": d.get("instType"),
        "primary_board": d.get("instType"),
        "tradingStatus": d.get("tradingStatus"),
        "tradingStatusInfo": d.get("state"),
        "complexProductCategory": d.get("complexProductCategory"),
        "priceMultiplier": d.get("priceMultiplier"),
        "priceShownUnits": d.get("priceShownUnits"),
    }


def iso_from_unix_ms(ts_ms: int):
    if not ts_ms:
        return None
    try:
        dt = datetime.datetime.fromtimestamp(
            int(ts_ms) / 1000.0,
            tz=datetime.timezone.utc,
        )
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except Exception:
        return None


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


def _norm_order_type_okx_to_astras(t: str) -> str:
    s = (t or "").lower()
    if s.startswith("market"):
        return "market"
    if s.startswith("limit"):
        return "limit"
    return "0"


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


def astras_order_simple_from_okx_neutral(
    o: dict,
    exchange: str,
    portfolio: str,
    existing: bool,
) -> dict:
    symbol = o.get("symbol", "0")
    price = o.get("price", 0) or 0
    qty = o.get("qty", 0) or 0
    filled = o.get("filled", 0) or 0

    order_type = _norm_order_type_okx_to_astras(str(o.get("type", "0")))
    order_status = _norm_order_status_okx_to_astras(str(o.get("status", "0")))
    time_in_force = _norm_tif_okx_to_astras(str(o.get("tif", "")))

    broker_symbol = None
    if exchange != "0" and symbol != "0":
        broker_symbol = f"{exchange}:{symbol}"

    trans_time = iso_from_unix_ms(int(o.get("ts_create", 0) or 0))
    update_time = iso_from_unix_ms(int(o.get("ts_update", 0) or 0))

    volume = 0

    return {
        "id": o.get("id", "0"),
        "symbol": symbol,
        "brokerSymbol": broker_symbol,
        "portfolio": portfolio,
        "exchange": exchange,
        "comment": None,
        "type": order_type,
        "side": o.get("side", "0"),
        "status": order_status,
        "transTime": trans_time,
        "updateTime": update_time,
        "endTime": None,
        "qtyUnits": 0,
        "qtyBatch": 0,
        "qty": qty,
        "filledQtyUnits": 0,
        "filledQtyBatch": 0,
        "filled": filled,
        "price": price,
        "existing": bool(existing),
        "timeInForce": time_in_force,
        "iceberg": None,
        "volume": volume,
    }


async def astras_instruments_with_price_limits(
    raw_items: list[dict],
    instrument_group: str | None = None,
    load_ticker_map: Callable[[list[str]], Awaitable[dict[str, dict]]] | None = None,
) -> list[dict]:
    if instrument_group:
        inst_types = [instrument_group]
    else:
        inst_types = ["SPOT", "FUTURES", "SWAP"]

    if load_ticker_map is None:
        ticker_map = await instruments_cache.load_ticker_map_for_types(inst_types)
    else:
        ticker_map = await load_ticker_map(inst_types)

    out: list[dict] = []
    for raw in raw_items:
        x = astras_instrument_simple(raw)
        sym = raw.get("symbol")
        t = ticker_map.get(sym) if sym else None
        if t:
            x["priceMax"] = t.get("high24h")
            x["priceMin"] = t.get("low24h")
        out.append(x)
    return out
