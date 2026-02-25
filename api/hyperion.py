import re
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from api import instruments_cache

router = APIRouter()


def _hyperion_root_field(query_str: str) -> str:
    q = str(query_str or "")
    if re.search(r"\binstruments\s*\(", q):
        return "instruments"
    if re.search(r"\binstrument\s*\(", q):
        return "instrument"
    return ""


@router.post("/hyperion")
async def hyperion(request: Request):
    body = await request.json()
    query_str = str(body.get("query") or "")
    variables = body.get("variables") or {}

    root_kind = _hyperion_root_field(query_str)
    if root_kind not in ("instrument", "instruments"):
        return JSONResponse(
            {
                "data": None,
                "errors": [{"message": "Unsupported Hyperion query. Use instrument(...) or instruments(...)."}],
            }
        )

    first = variables.get("first", 20)
    try:
        first = int(first)
    except Exception:
        first = 20
    first = max(1, min(first, 500))

    after = variables.get("after")
    where = variables.get("where") or {}
    and_filters = where.get("and") or []
    order_spec = variables.get("order") or []

    await instruments_cache.ensure_instr_cache()
    items = list(instruments_cache.get_instruments_cache().values())
    ticker_map = await instruments_cache.load_ticker_map_for_types(["SPOT", "FUTURES", "SWAP"])

    def _daily_vals(last, open24h):
        if last is None or open24h in (None, 0):
            return None, None
        daily = last - open24h
        return daily, (daily / open24h) * 100.0

    def _node_trading_fields(raw: dict) -> dict:
        t = ticker_map.get(raw.get("symbol")) or {}
        last = t.get("last")
        open24h = t.get("open24h")
        high24h = t.get("high24h")
        low24h = t.get("low24h")
        vol24h = t.get("vol24h")
        daily_growth, daily_growth_percent = _daily_vals(last, open24h)
        return {
            "price": last,
            "priceMax": high24h,
            "priceMin": low24h,
            "dailyGrowth": daily_growth,
            "dailyGrowthPercent": daily_growth_percent,
            "tradeVolume": vol24h,
            "tradeAmount": 0,
        }

    def _make_node(raw: dict) -> dict:
        symbol = raw.get("symbol")
        inst_type = raw.get("instType")
        quote_ccy = raw.get("quoteCcy")
        settle_ccy = raw.get("settleCcy")
        fi_currency = settle_ccy or quote_ccy
        tf = _node_trading_fields(raw)
        price = tf.get("price")
        return {
            "__typename": "InstrumentModel",
            "additionalInformation": {
                "__typename": "InstrumentAdditionalInformation",
                "cancellation": raw.get("cancellation"),
                "complexProductCategory": raw.get("complexProductCategory"),
                "priceMultiplier": raw.get("priceMultiplier"),
                "priceShownUnits": raw.get("priceShownUnits"),
            },
            "basicInformation": {
                "__typename": "InstrumentBasicInformation",
                "complexProductCategory": raw.get("complexProductCategory"),
                "description": raw.get("description"),
                "exchange": "OKX",
                "fullDescription": raw.get("fullDescription"),
                "fullName": raw.get("fullName"),
                "gicsSector": None,
                "market": inst_type,
                "readableType": raw.get("readableType"),
                "sector": raw.get("sector"),
                "shortName": symbol,
                "symbol": symbol,
                "type": raw.get("type"),
            },
            "boardInformation": {
                "__typename": "InstrumentBoardInformation",
                "board": inst_type,
                "isPrimaryBoard": True,
                "primaryBoard": inst_type,
            },
            "currencyInformation": {
                "__typename": "InstrumentCurrencyInformation",
                "nominal": quote_ccy,
                "settlement": None,
            },
            "financialAttributes": {
                "__typename": "InstrumentFinancialAttributes",
                "cfiCode": raw.get("cfiCode"),
                "currency": fi_currency,
                "isin": raw.get("ISIN"),
                "tradingStatus": raw.get("tradingStatus"),
                "tradingStatusInfo": raw.get("state"),
            },
            "tradingDetails": {
                "__typename": "InstrumentTradingDetails",
                "capitalization": None,
                "closingPrice": price,
                "dailyGrowth": tf.get("dailyGrowth"),
                "dailyGrowthPercent": tf.get("dailyGrowthPercent"),
                "lotSize": raw.get("lotSz"),
                "minStep": raw.get("tickSz"),
                "price": price,
                "priceMax": tf.get("priceMax"),
                "priceMin": tf.get("priceMin"),
                "priceStep": 0,
                "rating": None,
                "tradeAmount": tf.get("tradeAmount"),
                "tradeVolume": tf.get("tradeVolume"),
            },
        }

    def _match(item: dict, cond: dict) -> bool:
        bi = cond.get("basicInformation")
        if bi:
            if "symbol" in bi and "contains" in (bi.get("symbol") or {}):
                val = str((bi.get("symbol") or {}).get("contains") or "").upper()
                if val not in str(item.get("symbol") or "").upper():
                    return False
            if "shortName" in bi and "contains" in (bi.get("shortName") or {}):
                val = str((bi.get("shortName") or {}).get("contains") or "").upper()
                if val not in str(item.get("symbol") or "").upper():
                    return False

        ci = cond.get("currencyInformation")
        if ci and "nominal" in ci and "contains" in (ci.get("nominal") or {}):
            val = str((ci.get("nominal") or {}).get("contains") or "").upper()
            if val not in str(item.get("quoteCcy") or "").upper():
                return False

        td = cond.get("tradingDetails")
        if td:
            fields = _node_trading_fields(item)
            for field, rules in (td or {}).items():
                value = fields.get(field)
                if value is None:
                    return False
                if "gte" in rules and value < rules["gte"]:
                    return False
                if "lte" in rules and value > rules["lte"]:
                    return False
        return True

    if and_filters:
        items = [it for it in items if all(_match(it, f) for f in and_filters)]

    sort_cache: dict[str, dict] = {}

    def _sort_vals(raw: dict) -> dict:
        key = f"{raw.get('symbol') or ''}:{raw.get('instType') or ''}"
        if key in sort_cache:
            return sort_cache[key]
        tf = _node_trading_fields(raw)
        vals = {
            "symbol": raw.get("symbol"),
            "shortName": raw.get("symbol"),
            "market": raw.get("instType"),
            "nominal": raw.get("quoteCcy"),
            "board": raw.get("instType"),
            "minStep": raw.get("tickSz"),
            "priceStep": 0,
            "lotSize": raw.get("lotSz"),
            "price": tf.get("price"),
            "priceMax": tf.get("priceMax"),
            "priceMin": tf.get("priceMin"),
            "dailyGrowth": tf.get("dailyGrowth"),
            "dailyGrowthPercent": tf.get("dailyGrowthPercent"),
            "tradeVolume": tf.get("tradeVolume"),
            "tradeAmount": tf.get("tradeAmount"),
        }
        sort_cache[key] = vals
        return vals

    def _apply_sort(field: str, direction: str):
        rev = direction == "DESC"
        items.sort(
            key=lambda x: (_sort_vals(x).get(field) is None, _sort_vals(x).get(field)),
            reverse=rev,
        )

    for block in reversed(order_spec):
        b = block or {}
        for field, direction in (b.get("basicInformation") or {}).items():
            if direction in ("ASC", "DESC") and field in ("symbol", "shortName", "market"):
                _apply_sort(field, direction)
        for field, direction in (b.get("currencyInformation") or {}).items():
            if direction in ("ASC", "DESC") and field in ("nominal",):
                _apply_sort(field, direction)
        for field, direction in (b.get("boardInformation") or {}).items():
            if direction in ("ASC", "DESC") and field in ("board",):
                _apply_sort(field, direction)
        for field, direction in (b.get("tradingDetails") or {}).items():
            if direction in ("ASC", "DESC") and field in (
                "price",
                "priceMax",
                "priceMin",
                "minStep",
                "priceStep",
                "dailyGrowth",
                "dailyGrowthPercent",
                "tradeVolume",
                "tradeAmount",
                "lotSize",
            ):
                _apply_sort(field, direction)

    if root_kind == "instrument":
        symbol = variables.get("symbol")
        raw = instruments_cache.get_instruments_cache().get(symbol) if symbol else None
        if raw is None:
            return JSONResponse(
                {
                    "data": {root_kind: None},
                    "errors": [{"message": f"Instrument '{symbol}' not found on OKX"}],
                }
            )
        return JSONResponse({"data": {root_kind: _make_node(raw)}})

    total_count = len(items)
    try:
        start_idx = int(after) if after is not None else 0
    except Exception:
        start_idx = 0
    start_idx = max(0, min(start_idx, total_count))

    sliced = items[start_idx:start_idx + first]
    end_idx = start_idx + len(sliced)
    nodes = [_make_node(x) for x in sliced]
    edges = [
        {
            "__typename": "InstrumentModelEdge",
            "cursor": str(start_idx + i + 1),
            "node": n,
        }
        for i, n in enumerate(nodes)
    ]
    page_info = {
        "__typename": "PageInfo",
        "startCursor": str(start_idx) if total_count > 0 else None,
        "endCursor": str(end_idx) if total_count > 0 else None,
        "hasNextPage": end_idx < total_count,
        "hasPreviousPage": start_idx > 0,
    }
    return JSONResponse(
        {
            "data": {
                root_kind: {
                    "__typename": "InstrumentModelConnection",
                    "nodes": nodes,
                    "totalCount": total_count,
                    "edges": edges,
                    "pageInfo": page_info,
                }
            }
        }
    )
