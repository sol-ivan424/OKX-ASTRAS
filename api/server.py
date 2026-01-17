# api/server.py

"""Вопросы:


1. в заявках отдаем qty дробным, хотя Astras ждет int32
2. bids[].volume и asks[].volume отдаются float, Astras ждет int64
3. volume, ask_vol, bid_vol в котировках может быть дробный, астрас ждет int64 (в базовой валюте)
4. open_price в котировках нет, можно сделать pen24h в tickers (цена 24h назад)
5. total_bid_vol, total_ask_vol пока 0. Строго можно посчитать сумму объёмов по тем уровням стакана, которые реально получаешь (например, depth=10/20/50). Это будет “сумма по полученной глубине”, а не “по всему стакану”, потому что полный стакан OKX не отдаёт. Если Astras ожидает именно “по всем уровням”, тогда строго это невозможно и надо ставить 0. 
Если допускается “по стакану, который вы получаете по подписке” (как в OrderBookGetAndSubscribe), тогда считаем сумму по полученным уровням.
6.  отдается за 24 часа "high_price": high_price, "low_price": low_price
7. qtyUnits в сделках по портфелю вместо int отдается float
8. все заявки/сделки по портфелю отдают все заявки/сделки по аккаунту, так как в okx нет портфелей. внутри аккаунта можно различать типы счетов (instType: SPOT / SWAP / FUTURES / OPTION)
9. реализовываем только SPOT. FUTURES и SWAP можно добавить позже
10. OKX рекомендует использовать For other users, please use WS / Order channel
- к private WebSocket-каналу `fills` (исполнения/сделки) подписка разрешена только для аккаунтов с trading fee tier VIP6+ (ошибка `code=60029`).
- На обычных аккаунтах real-time сделки через `fills` недоступны; для проверки/получения сделок используйте приватный REST (fills-history) либо private WS канал `orders`, где могут приходить события об исполнениях.
11. соощение об ошибке в httpcode передаем реальный код okx


1. нужно ли делать возмоность множественных одинаковых подписок?
2. 389, 474, 453, 122, 447, 400, 429 строка - костыль
"""


import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from typing import List
import asyncio
import datetime
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState
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

# FOR ASTRAS
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost",
        "http://127.0.0.1",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import Body
USER_SETTINGS: dict[str, str] = {}
@app.post("/commandapi/observatory/subscriptions/actions/addToken")
def add_token(payload: dict = Body(default={})):
    # Заглушка: Astras пытается “зарегистрировать” токен в observatory.
    return {"ok": True}
#app = FastAPI()

@app.get("/identity/v5/UserSettings")
def user_settings(serviceName: str = Query(default="Astras"), key: str | None = None):
    v = USER_SETTINGS.get(key or "", "{}")
    return JSONResponse({"serviceName": serviceName, "key": key, "value": v})

@app.get("/identity/v5/UserSettings/group/widget-settings")
def widget_settings(serviceName: str = Query(default="Astras")):
    # Пустой набор настроек виджетов
    return JSONResponse([])

@app.get("/md/v2/clients/{client_id}/positions")
def positions(client_id: str):
    # Пустые позиции
    return JSONResponse([])

@app.post("/identity/v5/UserSettings")
@app.put("/identity/v5/UserSettings")
def user_settings_write(
    serviceName: str = Query(default="Astras"),
    key: str | None = None,
    payload: dict = Body(default={})
):
    # Astras обычно шлёт {"value": "..."} или сразу строку внутри payload
    value = payload.get("value", payload)
    if not isinstance(value, str):
        import json
        value = json.dumps(value, ensure_ascii=False)
    USER_SETTINGS[key or ""] = value
    return {"ok": True}

@app.delete("/identity/v5/UserSettings")
def user_settings_delete(
    serviceName: str = Query(default="Astras"),
    key: str | None = None,
):
    if key is not None:
        USER_SETTINGS.pop(key, None)
    return {"ok": True}

@app.post("/identity/v5/UserSettings/group/widget-settings")
@app.put("/identity/v5/UserSettings/group/widget-settings")
def widget_settings_write(payload: dict = Body(default={})):
    return {"ok": True}

@app.get("/client/v1.0/users/{user_id}/all-portfolios")
def all_portfolios(user_id: str):
    # Astras ожидает список портфелей пользователя.
    # Для DEV режима вернём один виртуальный портфель.
    return JSONResponse([
        {
            "portfolio": "DEV",
            "exchange": "OKX",
            "name": "DEV",
            "isDefault": True
        }
    ])

@app.get("/identity/v5/UserSettings/group/watchlist-collection")
def watchlist_collection(serviceName: str = Query(default="Astras")):
    # Пустой набор списков наблюдения
    return JSONResponse([])

@app.get("/commandapi/observatory/subscriptions")
def list_subscriptions():
    # Astras UI запрашивает список подписок observatory. В DEV режиме вернём пусто.
    return JSONResponse([])

from fastapi import Request

@app.post("/hyperion")
async def hyperion(request: Request):
    """
    GraphQL endpoint для Astras (виджет «Все инструменты»).

    Главная цель: НЕ ловить 504.
    Поэтому здесь нельзя синхронно ждать OKX REST бесконечно.
    """
    body = await request.json()
    variables = body.get("variables", {}) or {}
    where = variables.get("where") or {}

    first = variables.get("first", 20)
    try:
        first = int(first)
    except Exception:
        first = 20
    first = max(1, min(first, 500))

    after = variables.get("after")

    # Быстро "попробовать" обновить кеши (с TTL + timeout внутри)
    await _ensure_instr_cache()
    await _ensure_quote_cache()

    items = list(_INSTR_CACHE.values())

    # --- поиск по symbol.contains ---
    symbol_contains = (
        (where.get("basicInformation") or {})
        .get("symbol", {})
        .get("contains")
    )
    if symbol_contains:
        q = str(symbol_contains).upper()
        items = [x for x in items if q in str(x.get("symbol", "")).upper()]

    total_count = len(items)

    # --- пагинация по after (cursor = индекс) ---
    start_idx = 0
    if after:
        try:
            start_idx = int(after)
        except Exception:
            start_idx = 0
    start_idx = max(0, min(start_idx, total_count))

    sliced = items[start_idx:start_idx + first]
    end_idx = start_idx + len(sliced)

    has_next = end_idx < total_count
    has_prev = start_idx > 0

    start_cursor = str(start_idx) if total_count > 0 else None
    end_cursor = str(end_idx) if total_count > 0 else None

    def _make_node(raw: dict) -> dict:
        symbol = raw.get("symbol")
        inst_type = raw.get("instType") or "SPOT"
        quote_ccy = raw.get("quoteCcy")

        q = _get_quote(symbol)

        last_price = q.get("last_price")
        high_price = q.get("high_price")
        low_price = q.get("low_price")
        open_price = q.get("open_price")
        volume = q.get("volume")
        change = q.get("change")
        change_percent = q.get("change_percent")

        return {
            "__typename": "InstrumentModel",
            "basicInformation": {
                "__typename": "InstrumentBasicInformation",
                "symbol": symbol,
                "exchange": "OKX",
                "shortName": symbol,
                "market": inst_type,
                "gicsSector": None,
            },
            "currencyInformation": {
                "__typename": "InstrumentCurrencyInformation",
                "nominal": quote_ccy,
            },
            "boardInformation": {
                "__typename": "InstrumentBoardInformation",
                "board": inst_type,
            },
            "tradingDetails": {
                "__typename": "InstrumentTradingDetails",

                # цены/шаги из instruments + подмешиваем котировки строго по symbol
                "minStep": raw.get("tickSz"),
                "priceStep": raw.get("tickSz"),
                "lotSize": raw.get("lotSz"),

                "price": last_price if last_price is not None else raw.get("price"),
                "priceMax": high_price if high_price is not None else raw.get("priceMax"),
                "priceMin": low_price if low_price is not None else raw.get("priceMin"),

                # рост/объем — из котировок (если не успели получить → null)
                "dailyGrowth": change,
                "dailyGrowthPercent": change_percent,
                "tradeVolume": volume,
                "tradeAmount": None,

                # остальное можно оставлять null (Astras допускает)
                "capitalization": None,
                "closingPrice": last_price if last_price is not None else None,
                "rating": None,
            },
            "financialAttributes": {
                "__typename": "InstrumentFinancialAttributes",
                "cfiCode": raw.get("cfiCode"),
                "currency": quote_ccy,
                "isin": raw.get("ISIN"),
                "tradingStatus": raw.get("tradingStatus"),
                "tradingStatusInfo": raw.get("state"),
            },
        }

    nodes = [_make_node(x) for x in sliced]

    edges = []
    for i, n in enumerate(nodes):
        edges.append({
            "__typename": "InstrumentModelEdge",
            "cursor": str(start_idx + i + 1),
            "node": n,
        })

    page_info = {
        "__typename": "PageInfo",
        "startCursor": start_cursor,
        "endCursor": end_cursor,
        "hasNextPage": has_next,
        "hasPreviousPage": has_prev,
    }

    return JSONResponse({
        "data": {
            "instruments": {
                "__typename": "InstrumentModelConnection",
                "nodes": nodes,
                "totalCount": total_count,
                "edges": edges,
                "pageInfo": page_info,
            }
        }
    })

# FOR ASTRAS


def _astras_instrument_simple(d: dict) -> dict:
    """
    Преобразует нейтральный формат инструмента от адаптера в Astras md/v2/Securities.

    Правила:
    - ничего не придумываем
    - если данных нет → None (в JSON это null)
    - типы берём как есть из адаптера
    """

    symbol = d.get("symbol")
    exchange = d.get("exchange")

    inst_type = d.get("instType")
    state = d.get("state")

    lot_sz = d.get("lotSz")
    tick_sz = d.get("tickSz")

    quote_ccy = d.get("quoteCcy")

    return {
        "symbol": symbol,
        "shortname": symbol,
        "description": d.get("description"),

        "exchange": exchange,
        "market": inst_type,
        "type": d.get("type"),

        "lotsize": lot_sz,
        "facevalue": d.get("facevalue"),
        "cfiCode": d.get("cfiCode"),
        "cancellation": d.get("cancellation"),

        "minstep": tick_sz,
        "rating": d.get("rating"),
        "marginbuy": d.get("marginbuy"),
        "marginsell": d.get("marginsell"),
        "marginrate": d.get("marginrate"),

        "pricestep": tick_sz,
        "priceMax": d.get("priceMax"),
        "priceMin": d.get("priceMin"),
        "theorPrice": d.get("theorPrice"),
        "theorPriceLimit": d.get("theorPriceLimit"),
        "volatility": d.get("volatility"),

        "currency": quote_ccy,
        "ISIN": d.get("ISIN"),
        "yield": d.get("yield"),

        "board": inst_type,
        "primary_board": inst_type,

        "tradingStatus": d.get("tradingStatus"),
        "tradingStatusInfo": state,

        "complexProductCategory": d.get("complexProductCategory"),
        "priceMultiplier": d.get("priceMultiplier"),
        "priceShownUnits": d.get("priceShownUnits"),
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


# FOR ASTRAS
from typing import Optional

_INSTR_CACHE: dict[str, dict] = {}
# Ключ: symbol (например, BTC-USDT)
# Кеш последних котировок по symbol (ИСТОЧНИК: REST tickers OKX)
_QUOTE_CACHE: dict[str, dict] = {}

# Чтобы не дергать OKX на каждый GraphQL запрос (Astras шлет их пачкой)
_INSTR_CACHE_TS: float = 0.0
_QUOTE_CACHE_TS: float = 0.0
_INSTR_LOCK = asyncio.Lock()
_QUOTE_LOCK = asyncio.Lock()

# TTL кешей (сек)
_INSTR_TTL_SEC = 60.0   # инструменты редко меняются
_QUOTE_TTL_SEC = 3.0    # котировки можно чаще

def _get_quote(symbol: str) -> dict:
    return _QUOTE_CACHE.get(symbol, {})

async def _refresh_instr_cache():
    global _INSTR_CACHE, _INSTR_CACHE_TS
    raw = await adapter.list_instruments()
    _INSTR_CACHE = {x.get("symbol"): x for x in (raw or []) if x.get("symbol")}
    _INSTR_CACHE_TS = time.time()

async def _refresh_quote_cache_from_rest():
    global _QUOTE_CACHE, _QUOTE_CACHE_TS
    raw = await adapter.list_tickers(inst_type="SPOT")

    new_cache: dict[str, dict] = {}

    for t in (raw or []):
        symbol = t.get("symbol")
        if not symbol:
            continue

        def _to_float(v):
            try:
                return float(v)
            except Exception:
                return None

        last_price = _to_float(t.get("last"))
        open_price = _to_float(t.get("open24h"))
        high24h = _to_float(t.get("high24h"))
        low24h = _to_float(t.get("low24h"))
        vol24h = _to_float(t.get("vol24h"))

        if open_price and last_price is not None:
            change = last_price - open_price
            change_percent = (change / open_price) * 100.0
        else:
            change = None
            change_percent = None

        new_cache[symbol] = {
            "last_price": last_price,
            "high_price": high24h,
            "low_price": low24h,
            "open_price": open_price,
            "volume": vol24h,
            "change": change,
            "change_percent": change_percent,
        }

    _QUOTE_CACHE = new_cache
    _QUOTE_CACHE_TS = time.time()

async def _ensure_instr_cache():
    if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
        return

    async with _INSTR_LOCK:
        if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
            return
        # не висим на OKX
        try:
            await asyncio.wait_for(_refresh_instr_cache(), timeout=3.0)
        except Exception:
            return

async def _ensure_quote_cache():
    if _QUOTE_CACHE and (time.time() - _QUOTE_CACHE_TS) < _QUOTE_TTL_SEC:
        return

    async with _QUOTE_LOCK:
        if _QUOTE_CACHE and (time.time() - _QUOTE_CACHE_TS) < _QUOTE_TTL_SEC:
            return
        # не висим на OKX
        try:
            await asyncio.wait_for(_refresh_quote_cache_from_rest(), timeout=3.0)
        except Exception:
            return

async def _get_instr(symbol: str) -> Optional[dict]:
    if not _INSTR_CACHE:
        await _refresh_instr_cache()
    return _INSTR_CACHE.get(symbol)

async def _astras_instruments() -> list[dict]:
    # Единый источник инструментов (как и /v2/instruments)
    raw = await adapter.list_instruments()
    return [_astras_instrument_simple(x) for x in raw]

@app.get("/md/v2/Securities/{exchange}/{symbol}")
async def md_security(exchange: str, symbol: str, instrumentGroup: str | None = None):
    # Гарантируем, что UI никогда не получит 404
    if not _INSTR_CACHE:
        await _refresh_instr_cache()

    instr = _INSTR_CACHE.get(symbol)

    # Если Astras запросил MOEX/IMOEX — отдаём любой реальный OKX-инструмент
    if instr is None:
        instr = next(iter(_INSTR_CACHE.values()), None)

    if instr is None:
        raise HTTPException(status_code=404, detail="No instruments")

    x = _astras_instrument_simple(instr)

    # Подменяем только routing-поля (это ожидает Astras)
    #x["exchange"] = exchange
    if instrumentGroup:
        x["board"] = instrumentGroup
        x["primary_board"] = instrumentGroup

    return JSONResponse(x)


@app.get("/md/v2/Securities/{exchange}")
async def md_securities(
    exchange: str,
    query: str | None = None,
    limit: int = 200,
    instrumentGroup: str | None = None,
):
    if not _INSTR_CACHE:
        await _refresh_instr_cache()

    items = list(_INSTR_CACHE.values())

    if query:
        q = str(query).upper()
        items = [x for x in items if q in str(x.get("symbol", "")).upper()]

    items = items[: max(1, min(limit, 1000))]

    out: list[dict] = []
    for raw in items:
        x = _astras_instrument_simple(raw)
        #x["exchange"] = exchange
        if instrumentGroup:
            x["board"] = instrumentGroup
            x["primary_board"] = instrumentGroup
        out.append(x)

    return JSONResponse(out)

@app.get("/md/v2/Securities/{exchange}/{symbol}/availableBoards")
async def md_security_available_boards(
    exchange: str,
    symbol: str,
    instrumentGroup: str | None = None,
):
    # Проверяем, что инструмент существует
    instr = await _get_instr(symbol)

    # Astras ждёт массив строк с board
    return JSONResponse([
        instrumentGroup or "SPOT"
    ])

@app.get("/md/v2/boards")
def md_boards():
    return JSONResponse(["SPOT"])

@app.get("/instruments/v1/TreeMap")
async def instruments_treemap(market: str | None = None, limit: int = 50):
    if not _INSTR_CACHE:
        await _refresh_instr_cache()

    items = list(_INSTR_CACHE.values())
    items = items[: max(1, min(limit, 1000))]

    # Astras ожидает массив объектов инструментов
    out = []
    for raw in items:
        x = _astras_instrument_simple(raw)

        # важно: чтобы не было "MOEX/TQBR" мусора в treemap
        x["exchange"] = "OKX"
        x["board"] = "SPOT"
        x["primary_board"] = "SPOT"

        out.append(x)

    return JSONResponse({"displayItems": out})

@app.get("/md/v2/history")
async def md_history(
    symbol: str,
    exchange: str,
    from_: int = Query(alias="from"),
    to: int = Query(...),
    tf: str = "D",
    countBack: int = 300,
):
    # Astras ожидает объект: {history:[...], next:<int|null>, prev:<int|null>}
    # Используем ту же историю, что и в WS (adapter.get_bars_history)

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
    if hasattr(adapter, "get_bars_history"):
        try:
            raw = await adapter.get_bars_history(
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

                items.append({
                    "time": t_sec,
                    "close": b.get("close", 0),
                    "open": b.get("open", 0),
                    "high": b.get("high", 0),
                    "low": b.get("low", 0),
                    "volume": v,
                })
        except Exception:
            items = []

    try:
        cb = int(countBack or 0)
    except Exception:
        cb = 0
    if cb > 0 and len(items) > cb:
        items = items[-cb:]

    if items and step:
        first_t = items[0].get("time")
        last_t = items[-1].get("time")
        prev_t = (int(first_t) - step) if first_t else None
        next_t = (int(last_t) + step) if last_t else None
    else:
        prev_t = None
        next_t = None

    return JSONResponse({
        "history": items,
        "next": next_t,
        "prev": prev_t,
    })

@app.get("/md/v2/Securities/{broker_symbol}/quotes")
def md_quotes_stub(broker_symbol: str):
    return JSONResponse([])

@app.get("/md/v2/Securities")
async def md_securities_root(
    exchange: str | None = None,
    query: str | None = None,
    limit: int = 200,
    instrumentGroup: str | None = None,
):
    if not _INSTR_CACHE:
        await _refresh_instr_cache()

    items = list(_INSTR_CACHE.values())

    if query:
        q = query.upper()
        items = [x for x in items if q in x.get("symbol", "").upper()]

    items = items[: max(1, min(limit, 1000))]

    out = []
    for raw in items:
        x = _astras_instrument_simple(raw)

        if instrumentGroup:
            x["board"] = instrumentGroup
            x["primary_board"] = instrumentGroup

        out.append(x)

    return JSONResponse(out)

# FOR ASTRAS

@app.get("/md/v2/time")
def md_time():
    return int(time.time())

# все торговые инструменты
@app.get("/v2/instruments")
async def get_instruments(exchange: str = "OKX", format: str = "Simple", token: str = None):

#    if not token:
#        raise HTTPException(400, detail="TokenRequired")

    raw = await adapter.list_instruments()
    return [_astras_instrument_simple(x) for x in raw]


@app.websocket("/stream")
async def stream(ws: WebSocket):
    try:
        await ws.accept()
    except Exception:
        return

    async def safe_send_json(payload: dict):
        if ws.client_state != WebSocketState.CONNECTED:
            return
        try:
            await ws.send_json(payload)
        except Exception:
            return

    await safe_send_json({
        "message": "Connected",
        "httpCode": 200,
    })

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
    
    # guid -> stop_event (чтобы уметь корректно отписываться)
    subs: dict[str, asyncio.Event] = {}

    # throttling per subscription guid (frequency in ms)
    last_sent_ms_book: dict[str, int] = {}
    last_sent_ms_quotes: dict[str, int] = {}

    async def send_wrapped(name: str, payload, guid: str | None = None):
        data = payload.dict() if hasattr(payload, "dict") else payload
        await safe_send_json({"data": data, "guid": guid or f"{name}:req"})
    
    #сообщение об успешной подписке. код 200
    async def send_ack_200(guid: str | None):
        await safe_send_json(
            {
                "message": "Handled successfully",
                "httpCode": 200,
                "requestGuid": guid,
            }
        )

    # Astras сообщение: ошибка + закрыть WS
    async def send_error_and_close(guid: str | None, http_code: int, message: str):
        try:
            await safe_send_json(
                {
                    "message": message,
                    "httpCode": http_code,
                    "requestGuid": guid,
                }
            )
        finally:
            await ws.close()
        
    # OKX -> Astras: передаём код ошибки OKX как есть.
    def _okx_code_as_int(code):
        try:
            return int(code)
        except Exception:
            # если кода нет/не парсится - внутренняя ошибка
            return 500

    async def _handle_okx_ws_error(guid: str | None, ev: dict):
        okx_code = ev.get("code")
        okx_msg = ev.get("msg") or ev.get("message") or "OKX WS error"

        http_code = _okx_code_as_int(okx_code)

        # Формат Astras ошибки (requestGuid,httpCode,message)
        # httpCode = реальный код OKX
        await send_error_and_close(
            guid,
            http_code,
            okx_msg,
        )

    try:
        while True:

            try:
                msg = await ws.receive_json()
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception:
                break

            opcode = msg.get("opcode")
            token = msg.get("token")
            symbols: List[str] = msg.get("symbols", [])
            req_guid = msg.get("guid")

            if opcode == "ping":
                await safe_send_json({
                    "opcode": "ping",
                    "guid": req_guid,
                    "confirm": True
                })
                continue           

            # Astras просит отписаться от подписки по guid
            if opcode == "unsubscribe":
                unsub_guid = msg.get("guid") or req_guid

                ev = subs.pop(unsub_guid, None)
                if ev:
                    ev.set()

                # чистим throttling по guid, чтобы не копился мусор
                last_sent_ms_book.pop(unsub_guid, None)
                last_sent_ms_quotes.pop(unsub_guid, None)

                await safe_send_json({
                    "message": "Handled successfully",
                    "httpCode": 200,
                    "requestGuid": unsub_guid,
                })
                continue

            if opcode and (not isinstance(token, str) or not token.strip()):
                await safe_send_json(
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
                # Astras может переиспользовать один и тот же guid при смене инструмента в том же виджете.
                # Поэтому если такой guid уже есть — останавливаем старую подписку и запускаем новую.
                sub_guid = msg.get("guid") or req_guid

                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                stop = asyncio.Event()
                active["bars"].append(stop)
                subs[sub_guid] = stop

                code = msg.get("code")
                tf = str(msg.get("tf", "60"))
                from_ts = int(msg.get("from", int(time.time())))
                skip_history = bool(msg.get("skipHistory", False))
                split_adjust = bool(msg.get("splitAdjust", True))  #адаптер игнорирует

                exchange = msg.get("exchange")  #адаптер игнорирует
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует
                frequency = msg.get("frequency")  #адаптер игнорирует

                async def send_bar_astras(bar: dict, _guid: str):
                    o = bar.get("open")
                    h = bar.get("high")
                    l = bar.get("low")
                    c = bar.get("close")

                    # плоские свечи ломают шкалу Astras
                    if o == h == l == c:
                        return

                    ts_ms = bar.get("ts", 0)
                    try:
                        t_sec = int(int(ts_ms) / 1000)
                    except Exception:
                        return

                    vol = bar.get("volume", 0)

                    payload = {
                        "time": t_sec,
                        "close": c,
                        "open": o,
                        "high": h,
                        "low": l,
                        "volume": vol,
                    }

                    await safe_send_json({
                        "data": payload,
                        "guid": _guid,
                    })

                if code:
                    # запускаем подписку и ждём подтверждение/ошибку от OKX 
                    subscribed_evt = asyncio.Event()
                    error_evt = asyncio.Event()

                    # Пока отправляется история, онлайн в буфер,
                    # чтобы история шла после ACK(200) и до online-стрима.
                    history_done = False
                    live_buffer: list[dict] = []

                    def _on_subscribed(_ev: dict):
                        subscribed_evt.set()

                    def _on_error(ev: dict):
                        # OKX event:error -> Astras error (реальный code/msg) + close WS
                        error_evt.set()
                        return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                    async def _on_live_bar(b: dict, _guid: str):
                        nonlocal history_done
                        if not history_done:
                            live_buffer.append(b)
                            return
                        await send_bar_astras(b, _guid)

                    asyncio.create_task(
                        adapter.subscribe_bars(
                            symbol=code,
                            tf=tf,
                            from_ts=from_ts,
                            skip_history=skip_history,
                            split_adjust=split_adjust,
                            on_data=lambda b, _g=sub_guid: asyncio.create_task(_on_live_bar(b, _g)),
                            stop_event=stop,
                            on_subscribed=_on_subscribed,
                            on_error=_on_error,
                        )
                    )

                    # Ждём либо subscribe, либо error (без таймаута)
                    done, pending = await asyncio.wait(
                        [
                            asyncio.create_task(subscribed_evt.wait()),
                            asyncio.create_task(error_evt.wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()

                    # Если была ошибка — _handle_okx_ws_error уже отправил ответ и закрыл WS
                    if error_evt.is_set():
                        raise WebSocketDisconnect

                    # Подписка подтверждена, ACK 200
                    await send_ack_200(sub_guid)

                    # история
                    if (not skip_history) and hasattr(adapter, "get_bars_history"):
                        try:
                            history = await adapter.get_bars_history(
                                symbol=code,
                                tf=tf,
                                from_ts=from_ts,
                            )

                            # обязательно сортируем по времени
                            history.sort(key=lambda x: int(x.get("ts", 0)))

                            for hbar in history:
                                await send_bar_astras(hbar, sub_guid)
                        except Exception:
                            pass

                    # live-данные
                    history_done = True

                    # Сначала отдадим то, что успело прийти буфер
                    if live_buffer:
                        for b in live_buffer:
                            await send_bar_astras(b, sub_guid)
                        live_buffer.clear()

                continue


            # все заявки по портфелю (история + подписка) в Astras Simple
            if opcode == "OrdersGetAndSubscribeV2":
                stop = asyncio.Event()
                active["orders"].append(stop)

                exchange = msg.get("exchange") or "0"
                portfolio = msg.get("portfolio") or "0"

                statuses = msg.get("orderStatuses") or []
                skip_history = bool(msg.get("skipHistory", False))
                sub_guid = msg.get("guid") or req_guid

                # Если Astras переиспользовал guid (например, при смене инструмента в том же виджете)
                # — останавливаем старую подписку с этим guid и запускаем новую.
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                subs[sub_guid] = stop
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует
                frequency = msg.get("frequency")
                try:
                    frequency = int(frequency) if frequency is not None else 25
                except Exception:
                    frequency = 25

                async def send_order_astras(order_any: dict, existing_flag: bool, _guid=sub_guid):
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

                    await safe_send_json({"data": payload, "guid": _guid})

                # Сначала запускается подписка и ждём подтверждение/ошибку от OKX
                subscribed_evt = asyncio.Event()
                error_evt = asyncio.Event()

                # Пока отправляется историю, лайв события в буфер
                # чтобы порядок был: ACK(200)  история  live
                history_done = False
                live_buffer: list[dict] = []

                def _on_subscribed(_ev: dict):
                    subscribed_evt.set()

                def _on_error(ev: dict):
                    # OKX event:error -> Astras error (реальный code/msg) + close WS
                    error_evt.set()
                    return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                async def _on_live_order(o: dict, _guid: str):
                    nonlocal history_done
                    if not history_done:
                        live_buffer.append(o)
                        return
                    await send_order_astras(o, False, _guid)  # existing=False — новые события (private WS orders)

                asyncio.create_task(
                    adapter.subscribe_orders(
                        symbols,
                        lambda ord_, _g=sub_guid: asyncio.create_task(_on_live_order(ord_, _g)),
                        stop,
                        inst_type="SPOT",
                        on_subscribed=_on_subscribed,
                        on_error=_on_error,
                    )
                )

                # Ждём либо subscribe, либо error (без таймаута)
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(subscribed_evt.wait()),
                        asyncio.create_task(error_evt.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()

                # Если была ошибка, _handle_okx_ws_error уже отправил ответ и закрыл WS
                if error_evt.is_set():
                    raise WebSocketDisconnect

                # Подписка подтверждена, отправляем ACK 200
                await send_ack_200(sub_guid)

                # история заявок (pending) через REST
                if (not skip_history) and hasattr(adapter, "get_orders_pending"):
                    try:
                        inst_id = symbols[0] if symbols else None
                        history_orders = await adapter.get_orders_pending(
                            inst_type="SPOT",
                            inst_id=inst_id,
                        )
                        for ho in history_orders:
                            await send_order_astras(ho, True, sub_guid)  # existing=True — данные из снепшота/истории (pending REST)
                    except Exception:
                        pass

                # live-данные
                history_done = True

                # Сначала то, что успело прийти в буфер
                if live_buffer:
                    for o in live_buffer:
                        await send_order_astras(o, False, sub_guid)
                    live_buffer.clear()

                continue

            # стакан (Astras Simple)
            if opcode == "OrderBookGetAndSubscribe":
                stop = asyncio.Event()
                active["book"].append(stop)

                code = msg.get("code")
                depth = int(msg.get("depth", 20) or 20)

                frequency = msg.get("frequency")
                try:
                    frequency = int(frequency) if frequency is not None else 25
                except Exception:
                    frequency = 25

                sub_guid = msg.get("guid") or req_guid

                # если Astras переиспользовал guid — остановим старую подписку с этим guid
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                subs[sub_guid] = stop

                # эти поля Astras сейчас не влияют на OKX (оставляем как есть)
                exchange = msg.get("exchange")  #адаптер игнорирует
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует

                async def on_book(book: dict, _guid: str):
                    now_ms = int(time.time() * 1000)
                    freq_ms = frequency if isinstance(frequency, int) else 25

                    prev_ms = last_sent_ms_book.get(_guid, 0)
                    if freq_ms > 0 and (now_ms - prev_ms) < freq_ms:
                        return

                    last_sent_ms_book[_guid] = now_ms

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

                    bids = [{"price": p, "volume": v} for (p, v) in bids_in]
                    asks = [{"price": p, "volume": v} for (p, v) in asks_in]

                    payload = {
                        # snapshot и timestamp устаревшие
                        "snapshot": existing_flag,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": ts_sec,
                        "ms_timestamp": ms_ts,
                        "existing": existing_flag,
                    }

                    await safe_send_json({"data": payload, "guid": _guid})

                if code:
                    # Сначала запускается подписка и ждём подтверждение/ошибку от OKX
                    subscribed_evt = asyncio.Event()
                    error_evt = asyncio.Event()

                    # Пока отправляется ACK, лайв события в буфер
                    # чтобы порядок был: ACK(200)  live
                    history_done = False
                    live_buffer: list[dict] = []

                    def _on_subscribed(_ev: dict):
                        subscribed_evt.set()

                    def _on_error(ev: dict):
                        # OKX event:error -> Astras error (реальный code/msg) + close WS
                        error_evt.set()
                        return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                    async def _on_live_book(b: dict, _guid: str):
                        nonlocal history_done
                        if not history_done:
                            live_buffer.append(b)
                            return
                        await on_book(b, _guid)

                    asyncio.create_task(
                        adapter.subscribe_order_book(
                            symbol=code,
                            depth=depth,
                            on_data=lambda b, _g=sub_guid: asyncio.create_task(_on_live_book(b, _g)),
                            stop_event=stop,
                            on_subscribed=_on_subscribed,
                            on_error=_on_error,
                        )
                    )

                    # Ждём либо subscribe, либо error (без таймаута)
                    done, pending = await asyncio.wait(
                        [
                            asyncio.create_task(subscribed_evt.wait()),
                            asyncio.create_task(error_evt.wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()

                    # Если была ошибка, _handle_okx_ws_error уже отправил ответ и закрыл WS
                    if error_evt.is_set():
                        raise WebSocketDisconnect

                    # Подписка подтверждена, отправляем ACK 200
                    await send_ack_200(sub_guid)

                    # live-данные
                    history_done = True

                    # Сначала то, что успело прийти в буфер
                    if live_buffer:
                        for b in live_buffer:
                            await on_book(b, sub_guid)
                        live_buffer.clear()

                continue

            # котировки инструмента (Astras Simple)
            if opcode == "QuotesSubscribe":
                stop = asyncio.Event()
                active["quotes"].append(stop)

                code = msg.get("code")
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует

                frequency = msg.get("frequency")
                try:
                    frequency = int(frequency) if frequency is not None else 25
                except Exception:
                    frequency = 25

                sub_guid = msg.get("guid") or req_guid
                # если Astras переиспользовал guid — остановим старую подписку с этим guid
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                subs[sub_guid] = stop
                exchange_out = "OKX"

                symbol = code or (symbols[0] if symbols else "")

                async def send_quote_astras(t: dict, _guid: str):
                    now_ms = int(time.time() * 1000)
                    freq_ms = frequency if isinstance(frequency, int) else 25

                    prev_ms = last_sent_ms_quotes.get(_guid, 0)
                    if freq_ms > 0 and (now_ms - prev_ms) < freq_ms:
                        return

                    last_sent_ms_quotes[_guid] = now_ms

                    ts_ms = int(t.get("ts", 0) or 0)
                    ts_sec = int(ts_ms / 1000) if ts_ms else 0

                    last_price = t.get("last", 0)
                    bid = t.get("bid", 0) or 0
                    ask = t.get("ask", 0) or 0

                    bid_sz = t.get("bid_sz", 0) or 0
                    ask_sz = t.get("ask_sz", 0) or 0

                    high_price = t.get("high24h", 0) or 0
                    low_price = t.get("low24h", 0) or 0
                    open_price = t.get("open24h", 0) or 0
                    # volume: в базовой валюте (OKX vol24h). Если OKX отдаёт дробь — отдаём дробь.
                    volume = t.get("vol24h", 0) or 0

                    # change / change_percent считаем строго от open24h (цена 24 часа назад).

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

                        # high/low: 24h high/low от OKX
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

                        "open_price": open_price,  # цена 24 часа назад (OKX open24h)
                        "yield": None,

                        "lotsize": 0,
                        "lotvalue": 0,
                        "facevalue": 0,
                        "type": "0",

                        "total_bid_vol": 0,
                        "total_ask_vol": 0,

                        "accrued_interest": 0,

                        # change/change_percent зависят от prev_close_price; при null -> 0
                        "change": change,
                        "change_percent": change_percent,
                    }

                    await safe_send_json({"data": payload, "guid": _guid})

                if symbol:
                    # Сначала запускается подписка и ждём подтверждение/ошибку от OKX
                    subscribed_evt = asyncio.Event()
                    error_evt = asyncio.Event()

                    # Пока отправляется ACK, лайв события в буфер
                    # чтобы порядок был: ACK(200)  live
                    history_done = False
                    live_buffer: list[dict] = []

                    def _on_subscribed(_ev: dict):
                        subscribed_evt.set()

                    def _on_error(ev: dict):
                        # OKX event:error -> Astras error (реальный code/msg) + close WS
                        error_evt.set()
                        return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                    async def _on_live_quote(q: dict, _guid: str):
                        nonlocal history_done
                        if not history_done:
                            live_buffer.append(q)
                            return
                        await send_quote_astras(q, _guid)

                    asyncio.create_task(
                        adapter.subscribe_quotes(
                            symbol=symbol,
                            on_data=lambda q, _g=sub_guid: asyncio.create_task(_on_live_quote(q, _g)),
                            stop_event=stop,
                            on_subscribed=_on_subscribed,
                            on_error=_on_error,
                        )
                    )

                    # Ждём либо subscribe, либо error (без таймаута)
                    done, pending = await asyncio.wait(
                        [
                            asyncio.create_task(subscribed_evt.wait()),
                            asyncio.create_task(error_evt.wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()

                    # Если была ошибка, _handle_okx_ws_error уже отправил ответ и закрыл WS
                    if error_evt.is_set():
                        raise WebSocketDisconnect

                    # Подписка подтверждена, отправляем ACK 200
                    await send_ack_200(sub_guid)

                    # live-данные
                    history_done = True

                    # Сначала то, что успело прийти в буфер
                    if live_buffer:
                        for q in live_buffer:
                            await send_quote_astras(q, sub_guid)
                        live_buffer.clear()

                continue

            # все сделки по портфелю (Astras Simple)
            # все сделки по портфелю (history + подписка). если OKX WS trades недоступен — придёт ошибка от OKX (например 60029)
            if opcode == "TradesGetAndSubscribeV2":
                stop = asyncio.Event()
                active["fills"].append(stop)

                portfolio = msg.get("portfolio")  #адаптер игнорирует (в OKX нет такого понятия)
                exchange_in = msg.get("exchange")  #адаптер игнорирует
                data_format = msg.get("format")  #адаптер игнорирует

                skip_history = bool(msg.get("skipHistory", False))
                sub_guid = msg.get("guid") or req_guid

                # если Astras переиспользовал guid — остановим старую подписку с этим guid
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                subs[sub_guid] = stop
                exchange_out = "OKX"

                async def send_trade_astras(t: dict, existing_flag: bool, _guid: str):
                    ts_ms = int(t.get("ts", 0) or 0)
                    date_iso = _iso_from_unix_ms(ts_ms) if ts_ms else None

                    symbol = t.get("symbol") or "N/A"

                    payload = {
                        "id": str(t.get("id", "0")),
                        "orderno": str(t.get("orderno", "0")),
                        "comment": None,  # comment всегда null

                        "symbol": symbol,
                        "brokerSymbol": f"{exchange_out}:{symbol}",
                        "exchange": exchange_out,

                        "date": date_iso,
                        "board": None,  # у OKX нет board

                        # qtyUnits: float
                        "qtyUnits": t.get("qtyUnits", 0) or 0,

                        # qtyBatch: лотов у OKX нет
                        "qtyBatch": 0,

                        # qty: "Количество" по Astras -> используем qtyUnits (fill size), как float
                        "qty": t.get("qty", 0) or 0,

                        "price": t.get("price", 0) or 0,
                        "accruedInt": 0,

                        "side": t.get("side", "0") or "0",

                        # existing: True для истории (snapshot), False для онлайна
                        "existing": bool(existing_flag),

                        # комиссия: abs(fee) уже нормализована в адаптере
                        "commission": t.get("commission", 0) or 0,

                        "repoSpecificFields": None,

                        # volume/value: price * qtyUnits (посчитано в адаптере)
                        "volume": t.get("volume", 0) or 0,
                        "value": t.get("value", 0) or 0,
                    }

                    await safe_send_json({"data": payload, "guid": _guid})

                # Сначала запускается подписка и ждём подтверждение/ошибку от OKX
                subscribed_evt = asyncio.Event()
                error_evt = asyncio.Event()

                # Пока отправляется историю, лайв события в буфер
                # чтобы порядок был: ACK(200)  история  live
                history_done = False
                live_buffer: list[dict] = []

                def _on_subscribed(_ev: dict):
                    subscribed_evt.set()

                def _on_error(ev: dict):
                    # OKX event:error -> Astras error (реальный code/msg) + close WS
                    error_evt.set()
                    return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                async def _on_live_trade(tr: dict, _guid: str):
                    nonlocal history_done
                    if not history_done:
                        live_buffer.append(tr)
                        return
                    await send_trade_astras(tr, False, _guid)  # existing=False — новые события (OKX WS)

                asyncio.create_task(
                    adapter.subscribe_trades(
                        on_data=lambda tr, _g=sub_guid: asyncio.create_task(_on_live_trade(tr, _g)),
                        stop_event=stop,
                        inst_type="SPOT",
                        on_subscribed=_on_subscribed,
                        on_error=_on_error,
                    )
                )

                # Ждём либо subscribe, либо error (без таймаута)
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(subscribed_evt.wait()),
                        asyncio.create_task(error_evt.wait()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()

                # Если была ошибка, _handle_okx_ws_error уже отправил ответ и закрыл WS
                if error_evt.is_set():
                    raise WebSocketDisconnect

                # Подписка подтверждена, отправляем ACK 200
                await send_ack_200(sub_guid)

                # история (если skipHistory == false)
                if (not skip_history) and hasattr(adapter, "get_trades_history"):
                    try:
                        history = await adapter.get_trades_history(inst_type="SPOT", limit=100)
                        for tr in history:
                            await send_trade_astras(tr, True, sub_guid)  # existing=True — данные из снепшота/истории (REST)
                    except Exception:
                        pass

                # live-данные
                history_done = True

                # Сначала то, что успело прийти в буфер
                if live_buffer:
                    for tr in live_buffer:
                        await send_trade_astras(tr, False, sub_guid)
                    live_buffer.clear()

                continue
            

            # все сделки по инструменту
            """if opcode == "AllTradesGetAndSubscribe":"""
                # нужно добавить

            # все позиции портфеля
            """if opcode == "PositionsGetAndSubscribeV2":
                stop = asyncio.Event()
                active["positions"].append(stop)

                exchange = msg.get("exchange")
                portfolio = msg.get("portfolio")
                guid = msg.get("guid") or req_guid
                subs[guid] = stop
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

                    await safe_send_json({"data": payload, "guid": guid})

                asyncio.create_task(
                    adapter.subscribe_positions(
                        symbols,
                        lambda p: asyncio.create_task(on_pos_opcode(p)),
                        stop,
                    )
                )
                continue"""


    except WebSocketDisconnect:
        pass
    finally:
        # остановить все активные подписки, чтобы адаптер прекратил коллбеки
        for lst in active.values():
            for ev in lst:
                ev.set()

        # остановить подписки, которые храним по guid (unsubscribe-механизм)
        for ev in subs.values():
            try:
                ev.set()
            except Exception:
                pass
        subs.clear()