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
2. 389, 474, 453, 122, 447, 400, 429 строка - костыль (устарели номера строк)
3. ошибки таймаута сами придумали
4. документация для карты рынка
5. размер и структура файлов

0. пинг у нас - загрушка. в okx пинга нет
1. сейчас открытие=закрытие=open24h - скользящее окно 24 часа назад. можно использовать sodUtc0 - open дня по UTC0
при этом low_price high_price можно только за сутки
2. заявки (пока рыночная): SPOT: размер в базовой валюте (sz = amount_base). FUTURES/SWAP: размер в контрактах (sz = number_of_contracts)
quantity у нас может быть float (в доках int)
3. лимитная заявка пока без айсбергов
4. oneday лимитных заявок нет. ставится обычная limit как goodtillcancelled
attheclose - отклоняется, так как нет закрытия торгов. 
5. стоп-заявка реализована как условная. okx сама хранит заявку
"""


import os
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Header
from typing import List, Optional
import asyncio
import datetime
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState
from adapters.okx_adapter import OkxAdapter

from dotenv import load_dotenv
load_dotenv()

"""
def _astras_error(request_guid: str | None, http_code: int, message: str, status_code: int | None = None):
    return JSONResponse(
        {
            "requestGuid": request_guid or "",
            "httpCode": int(http_code),
            "message": str(message),
        },
        status_code=int(status_code if status_code is not None else http_code),
    )
"""

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

@app.get("/identity/v5/UserSettings")
def user_settings(serviceName: str = Query(default="Astras"), key: str | None = None):
    v = USER_SETTINGS.get(key or "", "{}")
    return JSONResponse({"serviceName": serviceName, "key": key, "value": v})

@app.get("/identity/v5/UserSettings/group/widget-settings")
def widget_settings(serviceName: str = Query(default="Astras")):
    # Пустой набор настроек виджетов
    return JSONResponse([])

@app.get("/md/v2/clients/{client_id}/positions")
async def md_client_positions(
    client_id: str,
    exchange: str | None = None,
    portfolio: str | None = None,
    format: str = "Simple",
    withoutCurrency: bool = False,
    jsonResponse: bool = False,
):
    #Используем тот же snapshot, что и в WS PositionsGetAndSubscribeV2.
    #exchange_out = exchange or "OKX"
    exchange_out = exchange or "UNITED"
    portfolio_out = portfolio or client_id
    result: list[dict] = []
    try:
        snapshot = []
        if hasattr(adapter, "get_positions_snapshot"):
            snapshot = await adapter.get_positions_snapshot(inst_type=None)
        elif hasattr(adapter, "get_positions"):
            snapshot = await adapter.get_positions(inst_type=None)
        for p in snapshot or []:
            # фильтр валют (если нужно)
            if withoutCurrency and p.get("isCurrency"):
                continue
            symbol = p.get("symbol")
            qty_units = float(p.get("qtyUnits", 0) or 0)
            avg_price = float(p.get("avgPrice", 0) or 0)
            cur_price = p.get("currentPrice")
            cur_price = float(cur_price) if cur_price is not None else None
            # volume считаем ТОЛЬКО если можем
            volume = None
            if avg_price and qty_units:
                volume = avg_price * qty_units
            current_volume = None
            if cur_price is not None and qty_units:
                current_volume = cur_price * qty_units
            result.append({
                "volume": volume,
                "currentVolume": current_volume,
                "symbol": symbol,
                "brokerSymbol": f"{exchange_out}:{symbol}",
                "portfolio": "DEV_portfolio",
                "exchange": exchange_out,
                "avgPrice": avg_price,
                "qtyUnits": qty_units,
                "openUnits": 0,
                "lotSize": float(p.get("lotSize", 0) or 0),
                "shortName": p.get("shortName") or symbol,
                "qtyT0": 0,
                "qtyT1": 0,
                "qtyT2": 0,
                "qtyTFuture": 0,
                "qtyT0Batch": 0,
                "qtyT1Batch": 0,
                "qtyT2Batch": 0,
                "qtyTFutureBatch": 0,
                "qtyBatch": 0,
                "openQtyBatch": 0,
                "qty": qty_units,
                "open": 0,
                "dailyUnrealisedPl": 0,
                "unrealisedPl": 0,
                "isCurrency": bool(p.get("isCurrency", False)),
                "existing": True,  # REST = всегда snapshot
            })
    except Exception:
        result = []
    return JSONResponse(result)

@app.get("/md/v2/Clients/{exchange}/{portfolio}/orders")
async def md_client_orders(
    exchange: str,
    portfolio: str,
):
    result: list[dict] = []
    try:
        inst_types = ["SPOT", "FUTURES", "SWAP"]
        # 1) история заявок
        if hasattr(adapter, "get_orders_history"):
            for it in inst_types:
                history = await adapter.get_orders_history(
                    inst_type=it,
                    limit=100,
                )
                for o in history or []:
                    result.append(
                        _astras_order_simple_from_okx_neutral(
                            o,
                            exchange=exchange,
                            portfolio=portfolio,
                            existing=True,   # REST = snapshot
                        )
                    )
        # 2) активные заявки (pending)
        if hasattr(adapter, "get_orders_pending"):
            for it in inst_types:
                pending = await adapter.get_orders_pending(
                    inst_type=it,
                    inst_id=None,
                )
                for o in pending or []:
                    result.append(
                        _astras_order_simple_from_okx_neutral(
                            o,
                            exchange=exchange,
                            portfolio=portfolio,
                            existing=True,   # REST = snapshot
                        )
                    )
    except Exception:
        pass
    return JSONResponse(result)

@app.get("/md/v2/Clients/{exchange}/{portfolio}/trades")
async def md_client_trades(
    exchange: str,
    portfolio: str,
    format: str = "heavy",
):
    fmt = (format or "heavy").strip().lower()
    if fmt not in ("heavy", "simple", "slim"):
        fmt = "heavy"
    result: list[dict] = []
    try:
        inst_types = ["SPOT", "FUTURES", "SWAP"]
        if hasattr(adapter, "get_trades_history"):
            for it in inst_types:
                trades = await adapter.get_trades_history(inst_type=it, limit=100)
                for t in trades or []:
                    symbol = t.get("symbol") or t.get("instId") or "[N/A]"
                    inst_id = t.get("instId") or t.get("inst_id") or t.get("symbol")
                    currency = None
                    if inst_id and "-" in str(inst_id):
                        parts = [p for p in str(inst_id).split("-") if p]
                        if len(parts) >= 2:
                            currency = parts[1].strip() or None
                    date_iso = t.get("date")
                    if not date_iso:
                        ts_ms = t.get("ts") or t.get("fillTime") or t.get("ts_fill")
                        try:
                            date_iso = _iso_from_unix_ms(int(ts_ms)) if ts_ms else None
                        except Exception:
                            date_iso = None
                    price = t.get("price")
                    qty_units = t.get("qtyUnits")
                    if qty_units is None:
                        qty_units = t.get("qty")

                    try:
                        qty_units_f = float(qty_units) if qty_units is not None else 0.0
                    except Exception:
                        qty_units_f = 0.0
                    try:
                        price_f = float(price) if price is not None else 0.0
                    except Exception:
                        price_f = 0.0

                    volume = t.get("volume")
                    value = t.get("value")
                    if volume is None:
                        volume = price_f * qty_units_f
                    if value is None:
                        value = volume
                    board = t.get("board") or t.get("instType") or t.get("inst_type") or "0"
                    result.append(
                        {
                            "id": str(t.get("id") or "0"),
                            "orderNo": str(t.get("orderNo") or t.get("orderno") or t.get("orderId") or "0"),
                            "comment": t.get("comment"),
                            "symbol": symbol,
                            "shortName": symbol,
                            "brokerSymbol": f"{exchange}:{symbol}",
                            "exchange": exchange,
                            "date": date_iso,
                            "board": board,
                            "qtyUnits": qty_units_f,
                            "qtyBatch": t.get("qtyBatch", 0) or 0,
                            "qty": t.get("qty", qty_units_f),
                            "price": price_f,
                            "currency": currency,
                            "accruedInt": t.get("accruedInt", 0) or 0,
                            "side": t.get("side") or "0",
                            "existing": bool(t.get("existing", True)),
                            "commission": t.get("commission"),
                            "repoSpecificFields": None,
                            "volume": volume,
                            "settleDate": t.get("settleDate"),  # пока null, не даёт OKX
                            "value": value,
                        }
                    )
    except Exception:
        pass
    return JSONResponse(result)

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
    # OKX: считаем весь аккаунт одним "портфелем" для Astras
    portfolio_id = "DEV_portfolio"

    return JSONResponse([
            {
            "agreement": "39004",
            "portfolio": "7500GHC",
            "tks": "7500GHC",
            "market": "Срочный рынок",
            "isVirtual": False
        },
        {
            "agreement": "61022",
            "portfolio": "7500NVC",
            "tks": "7500NVC",
            "market": "Срочный рынок",
            "isVirtual": False 
        },
        {
            "agreement": "test",
            "portfolio": portfolio_id,
            "tks": portfolio_id,
            "market": "OKX",
            "isVirtual": True,
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

    query_str = body.get("query") or ""
    # Если Astras запрашивает instrument(...) — нужно вернуть data.instrument
    want_single_instrument = ("instrument(" in query_str) and ("instruments" not in query_str)

    first = variables.get("first", 20)
    try:
        first = int(first)
    except Exception:
        first = 20
    first = max(1, min(first, 500))

    after = variables.get("after")

    await _ensure_instr_cache()
    # Всегда свежие котировки для Hyperion:
    # В таблице «Все инструменты» одновременно есть SPOT/FUTURES/SWAP,
    # поэтому собираем tickers сразу для всех типов и объединяем по symbol.
    async def _load_ticker_map() -> dict:
        inst_types = ("SPOT", "FUTURES", "SWAP")

        results = await asyncio.gather(
            *(adapter.list_tickers(inst_type=it) for it in inst_types),
            return_exceptions=True,
        )

        merged: list[dict] = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if r:
                merged.extend(r)

        return {t.get("symbol"): t for t in merged if t.get("symbol")}

    ticker_map = await _load_ticker_map()


    items = list(_INSTR_CACHE.values())

    # --- универсальный поиск (where.and[]) ---
    and_filters = (where.get("and") or [])

    def _match(item: dict, cond: dict) -> bool:
        # --- basicInformation ---
        bi = cond.get("basicInformation")
        if bi:
            if "symbol" in bi and "contains" in bi["symbol"]:
                if bi["symbol"]["contains"].upper() not in (item.get("symbol") or "").upper():
                    return False

            if "shortName" in bi and "contains" in bi["shortName"]:
                if bi["shortName"]["contains"].upper() not in (item.get("symbol") or "").upper():
                    return False

        # --- currencyInformation ---
        ci = cond.get("currencyInformation")
        if ci:
            if "nominal" in ci and "contains" in ci["nominal"]:
                if ci["nominal"]["contains"].upper() not in (item.get("quoteCcy") or "").upper():
                    return False


        # --- tradingDetails (ФИЛЬТРУЕМ ПО TICKERS) ---
        td = cond.get("tradingDetails")
        if td:
            symbol = item.get("symbol")
            t = ticker_map.get(symbol)

            # если нет тикера — фильтр не проходит
            if not t:
                return False

            # вычисляем значения так же, как в _make_node
            last = t.get("last")
            open24h = t.get("open24h")
            high24h = t.get("high24h")
            low24h = t.get("low24h")
            vol24h = t.get("vol24h")

            if open24h is not None and open24h != 0 and last is not None:
                daily_growth = last - open24h
                daily_growth_percent = (daily_growth / open24h) * 100.0
            else:
                daily_growth = None
                daily_growth_percent = None

            field_map = {
                "price": last,
                "priceMax": high24h,
                "priceMin": low24h,
                "dailyGrowth": daily_growth,
                "dailyGrowthPercent": daily_growth_percent,
                "tradeVolume": vol24h,
                "tradeAmount": (vol24h * last) if (vol24h is not None and last is not None) else None,
            }

            for field, rules in td.items():
                value = field_map.get(field)

                if value is None:
                    return False

                if "gte" in rules and value < rules["gte"]:
                    return False

                if "lte" in rules and value > rules["lte"]:
                    return False

        return True

    if and_filters:
        filtered = []
        for item in items:
            if all(_match(item, f) for f in and_filters):
                filtered.append(item)
        items = filtered

    total_count = len(items)


    # --- сортировка (Astras передаёт variables.order) ---
    order_spec = variables.get("order") or []

    # Предварительно считаем "значения сортировки" 1 раз на инструмент (оптимизация)
    sort_cache: dict[str, dict] = {}

    def _get_sort_vals(item: dict) -> dict:
        sym = item.get("symbol") or ""
        cached = sort_cache.get(sym)
        if cached is not None:
            return cached

        t = ticker_map.get(sym, {})  # котировки строго из REST tickers
        last = t.get("last")
        open24h = t.get("open24h")
        high24h = t.get("high24h")
        low24h = t.get("low24h")
        vol24h = t.get("vol24h")

        if open24h not in (None, 0) and last is not None:
            daily_growth = last - open24h
            daily_growth_percent = (daily_growth / open24h) * 100.0
        else:
            daily_growth = None
            daily_growth_percent = None

        inst_type = item.get("instType")
        tick_sz = item.get("tickSz")
        lot_sz = item.get("lotSz")
        quote_ccy = item.get("quoteCcy")

        cached = {
            # basicInformation
            "symbol": sym,
            "shortName": sym,
            "market": inst_type,            # basicInformation.market

            # currencyInformation
            "nominal": quote_ccy,           # currencyInformation.nominal

            # boardInformation
            "board": inst_type,             # boardInformation.board

            # tradingDetails (из instruments)
            "minStep": tick_sz,
            "priceStep": tick_sz,
            "lotSize": lot_sz,

            # tradingDetails (из tickers)
            "price": last,
            "priceMax": high24h,
            "priceMin": low24h,
            "dailyGrowth": daily_growth,
            "dailyGrowthPercent": daily_growth_percent,
            "tradeVolume": vol24h,
            "tradeAmount": (vol24h * last) if (vol24h is not None and last is not None) else None,
        }

        sort_cache[sym] = cached
        return cached

    def _apply_sort(field: str, direction: str):
        rev = (direction == "DESC")
        # None всегда уходит вниз (и при ASC, и при DESC)
        items.sort(
            key=lambda x: (
                _get_sort_vals(x).get(field) is None,
                _get_sort_vals(x).get(field),
            ),
            reverse=rev,
        )

    # В Astras может быть несколько сортировок: применяем стабильной сортировкой
    # Сначала второстепенные, потом главные (поэтому reverse)
    for o in reversed(order_spec):
        o = o or {}

        # basicInformation: symbol / shortName / market
        bi = o.get("basicInformation") or {}
        for field, direction in bi.items():
            if direction in ("ASC", "DESC") and field in ("symbol", "shortName", "market"):
                _apply_sort(field, direction)

        # currencyInformation: nominal
        ci = o.get("currencyInformation") or {}
        for field, direction in ci.items():
            if direction in ("ASC", "DESC") and field in ("nominal",):
                _apply_sort(field, direction)

        # boardInformation: board
        bo = o.get("boardInformation") or {}
        for field, direction in bo.items():
            if direction in ("ASC", "DESC") and field in ("board",):
                _apply_sort(field, direction)

        # tradingDetails: price/priceMax/priceMin/minStep/priceStep/dailyGrowth/...
        td = o.get("tradingDetails") or {}
        for field, direction in td.items():
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
        inst_type = raw.get("instType")
        quote_ccy = raw.get("quoteCcy")

        # Котировки строго по symbol из REST tickers (okx_adapter.list_tickers)
        t = ticker_map.get(symbol, {})

        last = t.get("last")
        open24h = t.get("open24h")
        high24h = t.get("high24h")
        low24h = t.get("low24h")
        vol24h = t.get("vol24h")
        

        # Рост за день считаем от open24h (цена 24 часа назад)
        if open24h is not None and open24h != 0 and last is not None:
            daily_growth = last - open24h
            daily_growth_percent = (daily_growth / open24h) * 100.0
        else:
            daily_growth = None
            daily_growth_percent = None


        return {
            "__typename": "InstrumentModel",

            # Нужные Astras поля (даже если в списке не запрашиваются)
            "additionalInformation": {
                "__typename": "InstrumentAdditionalInformation",
                "cancellation": raw.get("cancellation"),
                "complexProductCategory": raw.get("complexProductCategory"),
                "priceMultiplier": raw.get("priceMultiplier"),
                "priceShownUnits": raw.get("priceShownUnits"),
            },

            "basicInformation": {
                "__typename": "InstrumentBasicInformation",

                # ключевые поля
                "symbol": symbol,
                "exchange": "OKX",
                "shortName": symbol,
                "market": inst_type,
                "gicsSector": None,

                # Astras может запрашивать расширенные поля — отдаём что можем, остальное null
                "type": raw.get("type"),
                "complexProductCategory": raw.get("complexProductCategory"),
                "description": raw.get("description"),
                "fullDescription": raw.get("fullDescription"),
                "fullName": raw.get("fullName"),
                "readableType": raw.get("readableType"),
                "sector": raw.get("sector"),
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
                # settlement в крипте по смыслу совпадает с валютой котирования
                "settlement": quote_ccy,
            },

            "tradingDetails": {
                "__typename": "InstrumentTradingDetails",

                # шаги/лотность — из instruments
                "minStep": raw.get("tickSz"),
                "priceStep": raw.get("tickSz"),
                "lotSize": raw.get("lotSz"),

                # котировки — из REST tickers (строго по symbol)
                "price": last if last is not None else None,
                "priceMax": high24h if high24h is not None else None,
                "priceMin": low24h if low24h is not None else None,

                # рост за день
                "dailyGrowth": daily_growth,
                "dailyGrowthPercent": daily_growth_percent,

                # объёмы (24h, базовая валюта)
                "tradeVolume": vol24h,
                "tradeAmount": (vol24h * last) if (vol24h is not None and last is not None) else None,

                # остальное Astras допускает как null
                "capitalization": None,
                "closingPrice": last if last is not None else None,
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

    # --- SINGLE instrument (GraphQL instrument(...)) ---
    if want_single_instrument:
        sym = variables.get("symbol")
        await _ensure_instr_cache()

        board_req = variables.get("board")
        board_req = (str(board_req).strip().upper() if board_req is not None else "")

        raw = _INSTR_CACHE.get(sym) if sym else None

        if sym and board_req:
            for it in _INSTR_CACHE.values():
                if it.get("symbol") == sym and (it.get("instType") or "").upper() == board_req:
                    raw = it
                    break

        if raw is None:
            return JSONResponse(
                {
                    "requestGuid": "",
                    "httpCode": 404,
                    "message": f"Instrument '{sym}' not found on OKX",
                },
                status_code=404,
            )

        node = _make_node(raw)

        return JSONResponse({
            "data": {
                "instrument": node
            }
        })
    
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

def _astras_instrument_simple(d: dict) -> dict:
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

        "pricestep": d.get("tickSz"),
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
        "portfolio": "DEV_portfolio",
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

_INSTR_CACHE: dict[str, dict] = {} # Ключ: symbol (например, BTC-USDT)
_INSTR_CACHE_TS: float = 0.0 # Чтобы не дергать OKX на каждый GraphQL запрос
_INSTR_LOCK = asyncio.Lock()
_INSTR_TTL_SEC = 60.0 # TTL кешей (сек)
SUPPORTED_BOARDS = ["SPOT", "FUTURES", "SWAP"]
_ORDER_IDEMPOTENCY: dict[str, dict] = {} # cache for market orders: X-REQID -> response json
_ORDER_IDEMPOTENCY_LIMIT: dict[str, dict] = {} # limit
_ORDER_IDEMPOTENCY_STOP: dict[str, dict] = {} # stop

async def _load_ticker_map_for_types(inst_types: list[str]) -> dict[str, dict]:
    # Грузим tickers для нужных instType и объединяем в symbol -> ticker
    results = await asyncio.gather(
        *(adapter.list_tickers(inst_type=it) for it in inst_types),
        return_exceptions=True,
    )

    merged: list[dict] = []
    for r in results:
        if isinstance(r, Exception):
            continue
        if r:
            merged.extend(r)

    return {t.get("symbol"): t for t in merged if t.get("symbol")}

async def _astras_instruments_with_price_limits(
    raw_items: list[dict],
    instrumentGroup: str | None = None,
) -> list[dict]:
    # какие instType грузим в tickers
    if instrumentGroup:
        inst_types = [instrumentGroup]
    else:
        inst_types = ["SPOT", "FUTURES", "SWAP"]

    ticker_map = await _load_ticker_map_for_types(inst_types)

    out: list[dict] = []
    for raw in raw_items:
        x = _astras_instrument_simple(raw)

        sym = raw.get("symbol")
        t = ticker_map.get(sym) if sym else None

        if t:
            # priceMax/priceMin берём из тикера (24h high/low)
            x["priceMax"] = t.get("high24h")
            x["priceMin"] = t.get("low24h")

        # если тикера нет — оставляем как было (None)
        out.append(x)

    return out

async def _refresh_instr_cache():
    global _INSTR_CACHE, _INSTR_CACHE_TS
    raw_all: list[dict] = []

    for inst_type in ("SPOT", "FUTURES", "SWAP"):
        try:
            part = await adapter.list_instruments(inst_type=inst_type)
            if part:
                raw_all.extend(part)
        except Exception:
            continue

    _INSTR_CACHE = {x.get("symbol"): x for x in (raw_all or []) if x.get("symbol")}
    _INSTR_CACHE_TS = time.time()

async def _ensure_instr_cache():
    if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
        return

    async with _INSTR_LOCK:
        if _INSTR_CACHE and (time.time() - _INSTR_CACHE_TS) < _INSTR_TTL_SEC:
            return
        # не висим на OKX
        try:
            await _refresh_instr_cache()
        except Exception:
            return

async def _get_instr(symbol: str) -> Optional[dict]:
    if not _INSTR_CACHE:
        await _refresh_instr_cache()
    return _INSTR_CACHE.get(symbol)

async def _astras_instruments() -> list[dict]:
    # Единый источник инструментов (как и /v2/instruments)
    await _ensure_instr_cache()
    return [_astras_instrument_simple(x) for x in list(_INSTR_CACHE.values())]

@app.get("/md/v2/Securities/{broker_symbol}/quotes")
async def md_quotes(broker_symbol: str):
    # broker_symbol приходит как "OKX:BTC-USDT"
    symbol = broker_symbol
    if ":" in broker_symbol:
        symbol = broker_symbol.split(":", 1)[1]

    exchange_out = "OKX"

    t = await adapter.get_quote_snapshot_rest(symbol=symbol)

    ts_ms = int(t.get("ts", 0) or 0)
    ts_sec = int(ts_ms / 1000) if ts_ms else 0

    last_price = t.get("last", 0) or 0
    bid = t.get("bid", 0) or 0
    ask = t.get("ask", 0) or 0

    bid_sz = t.get("bid_sz", 0) or 0
    ask_sz = t.get("ask_sz", 0) or 0

    high_price = t.get("high24h", 0) or 0
    low_price = t.get("low24h", 0) or 0

    # open24h = "цена 24 часа назад"
    open_price = t.get("open24h", 0) or 0

    # volume = объём за 24ч в базовой валюте
    volume = t.get("vol24h", 0) or 0

    # change / change_percent строго от open24h
    if open_price > 0:
        change = last_price - open_price
        change_percent = (change / open_price) * 100.0
    else:
        change = 0.0
        change_percent = 0.0

    # ob_ms_timestamp + totals из REST books
    ob_ms_timestamp = t.get("ob_ts")
    total_bid_vol = t.get("total_bid_vol", 0) or 0
    total_ask_vol = t.get("total_ask_vol", 0) or 0

    payload = {
        "symbol": symbol,
        "exchange": exchange_out,
        "description": None,

        # по документации это "цена предыдущего закрытия",
        # но строго её нет — используем open24h
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

        # open_price = open24h
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

@app.get("/md/v2/Securities/{exchange}/{symbol}")
async def md_security(exchange: str, symbol: str, instrumentGroup: str | None = None):
    # Гарантируем, что UI никогда не получит 404
    symbol = symbol.replace("_", "-")
    if not _INSTR_CACHE:
        await _refresh_instr_cache()
    instr = _INSTR_CACHE.get(symbol)

    if instr is None:
        raise HTTPException(
            status_code=404,
            detail=f"Instrument '{symbol}' not found on OKX"
        )

    x = _astras_instrument_simple(instr)
    try:
        q = await adapter.get_quote_snapshot_rest(symbol=symbol)
        x["priceMax"] = q.get("high24h")
        x["priceMin"] = q.get("low24h")
    except Exception:
        pass

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
    out = await _astras_instruments_with_price_limits(items, instrumentGroup)
    if instrumentGroup:
        for x in out:
            x["board"] = instrumentGroup
            x["primary_board"] = instrumentGroup

    return JSONResponse(out)

@app.get("/md/v2/Securities/{exchange}/{symbol}/availableBoards")
async def md_security_available_boards(
    exchange: str,
    symbol: str,
):
    return JSONResponse(SUPPORTED_BOARDS)

@app.get("/md/v2/boards")
def md_boards():
    return JSONResponse(SUPPORTED_BOARDS)

@app.get("/instruments/v1/TreeMap")
async def instruments_treemap(market: str | None = None, limit: int = 50):
    if not _INSTR_CACHE:
        await _refresh_instr_cache()

    items = list(_INSTR_CACHE.values())
    items = items[: max(1, min(limit, 1000))]
    out = []
    for raw in items:
        x = _astras_instrument_simple(raw)

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

@app.get("/md/v2/time")
def md_time():
    return int(time.time())

@app.post("/commandapi/warptrans/TRADE/v2/client/orders/actions/market")
async def cmd_market_order(
    request: Request,
    x_reqid: str = Header(..., alias="X-REQID"),
):
    body = await request.json()

    if x_reqid in _ORDER_IDEMPOTENCY:
        old_body = _ORDER_IDEMPOTENCY[x_reqid]
        return JSONResponse(
            {
                "message": "Request with such X-REQID was already handled.",
                "oldResponse": {
                    "statusCode": 200,
                    "body": old_body
                }
            },
            status_code=400
        )

    side = (body.get("side") or "").lower()
    qty = body.get("quantity")
    instr = body.get("instrument") or {}

    symbol = instr.get("symbol")
    inst_type = instr.get("instrumentGroup") or instr.get("board")

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Invalid side")
    if symbol is None or str(symbol).strip() == "":
        raise HTTPException(status_code=400, detail="Instrument symbol is required")
    if qty is None:
        raise HTTPException(status_code=400, detail="quantity is required")
    try:
        qty_f = float(qty)
    except Exception:
        raise HTTPException(status_code=400, detail="quantity must be number")
    if inst_type is None or str(inst_type).strip() == "":
        raise HTTPException(status_code=400, detail="instrumentGroup/board is required")
    inst_type_s = str(inst_type).strip().upper()
    if inst_type_s not in ("SPOT", "FUTURES", "SWAP"):
        raise HTTPException(status_code=400, detail="Unsupported instrumentGroup/board")
    tgt_ccy = None
    if inst_type_s == "SPOT" and side == "buy": # SPOT: размер в базовой валюте (для BUY нужно tgtCcy="base_ccy")
        tgt_ccy = "base_ccy"

    try:
        res = await adapter.place_market_order(
            symbol=symbol,
            side=side,
            quantity=qty_f,
            inst_type=inst_type_s,
            tgt_ccy=tgt_ccy,
            cl_ord_id=x_reqid,
        )
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=502)

    out = {
        "message": "success",
        "orderNumber": str(res.get("ordId") or "0"),
    }

    _ORDER_IDEMPOTENCY[x_reqid] = out
    return JSONResponse(out)

@app.post("/commandapi/warptrans/TRADE/v2/client/orders/actions/limit")
async def cmd_limit_order(
    request: Request,
    x_reqid: str = Header(..., alias="X-REQID"),
):
    body = await request.json()

    if x_reqid in _ORDER_IDEMPOTENCY_LIMIT:
        old_body = _ORDER_IDEMPOTENCY_LIMIT[x_reqid]
        return JSONResponse(
            {
                "message": "Request with such X-REQID was already handled.",
                "oldResponse": {
                    "statusCode": 200,
                    "body": old_body
                }
            },
            status_code=400
        )

    side = (body.get("side") or "").lower()
    qty = body.get("quantity")
    price = body.get("price")
    instr = body.get("instrument") or {}

    symbol = instr.get("symbol")
    inst_type = instr.get("instrumentGroup") or instr.get("board")

    # валидация (по аналогии с market, но +price)
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Invalid side. Allowed values: buy, sell")
    if symbol is None or str(symbol).strip() == "":
        raise HTTPException(status_code=400, detail="Instrument symbol is required")
    if qty is None:
        raise HTTPException(status_code=400, detail="quantity is required")
    if price is None:
        raise HTTPException(status_code=400, detail="price is required")
    try:
        qty_f = float(qty)
    except Exception:
        raise HTTPException(status_code=400, detail="quantity must be number")
    try:
        price_f = float(price)
    except Exception:
        raise HTTPException(status_code=400, detail="price must be number")
    if inst_type is None or str(inst_type).strip() == "":
        raise HTTPException(status_code=400, detail="instrumentGroup/board is required")
    inst_type_s = str(inst_type).strip().upper()
    if inst_type_s not in ("SPOT", "FUTURES", "SWAP"):
        raise HTTPException(status_code=400, detail="Unsupported instrumentGroup/board")   
    
    tif = (body.get("timeInForce") or "oneday").lower()
    if tif in ("oneday", "goodtillcancelled"):
        okx_ord_type = "limit"
    elif tif == "immediateorcancel":
        okx_ord_type = "ioc"
    elif tif == "fillorkill":
        okx_ord_type = "fok"
    elif tif == "bookorcancel":
        okx_ord_type = "post_only"
    elif tif == "attheclose":
        raise HTTPException(status_code=400, detail="timeInForce attheclose is not supported for OKX")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported timeInForce: {tif}")

    try:
        res = await adapter.place_limit_order(
            symbol=symbol,
            side=side,
            quantity=qty_f,
            price=price_f,
            inst_type=inst_type_s,
            ord_type=okx_ord_type,
            cl_ord_id=x_reqid,  # как и в market
        )
    except Exception as e:
        return JSONResponse({"message": str(e)}, status_code=502)

    out = {
        "message": "success",
        "orderNumber": str(res.get("ordId") or "0"),
    }

    _ORDER_IDEMPOTENCY_LIMIT[x_reqid] = out
    return JSONResponse(out)

@app.post("/commandapi/warptrans/TRADE/v2/client/orders/actions/stop")
async def create_stop_order(request: Request):

    body = await request.json()
    x_reqid = request.headers.get("X-REQID")
    if not x_reqid:
        raise HTTPException(status_code=400, detail="X-REQID header is required")
    if x_reqid in _ORDER_IDEMPOTENCY_STOP:
        old_body = _ORDER_IDEMPOTENCY_STOP[x_reqid]
        return JSONResponse(
            {
                "message": "Request with such X-REQID was already handled.",
                "oldResponse": {
                    "statusCode": 200,
                    "body": old_body
                }
            },
            status_code=400
        )

    side = body.get("side")
    condition = body.get("condition")
    trigger_price = body.get("triggerPrice")
    qty = body.get("quantity")
    instrument = body.get("instrument") or {}
    allow_margin = bool(body.get("allowMargin", False))

    symbol = instrument.get("symbol")
    inst_type = instrument.get("instrumentGroup") or instrument.get("board")
    inst_type_s = str(inst_type).strip().upper()

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Invalid side")
    if not symbol:
        raise HTTPException(status_code=400, detail="Instrument symbol is required")
    
    if trigger_price is None:
        raise HTTPException(status_code=400, detail="triggerPrice is required")
    try:
        trigger_price_f = float(trigger_price)
    except Exception:
        raise HTTPException(status_code=400, detail="triggerPrice must be number")
    if trigger_price_f <= 0:
        raise HTTPException(status_code=400, detail="triggerPrice must be > 0")
    
    if qty is None:
        raise HTTPException(status_code=400, detail="quantity is required")
    try:
        qty_f = float(qty)
    except Exception:
        raise HTTPException(status_code=400, detail="quantity must be number")
    if qty_f <= 0:
        raise HTTPException(status_code=400, detail="quantity must be > 0")
    if inst_type_s not in ("SPOT", "FUTURES", "SWAP"):
            raise HTTPException(status_code=400, detail="Unsupported instrumentGroup/boar")

    allowed = ("more", "less", "moreorequal", "lessorequal")
    if condition not in allowed:
        raise HTTPException(status_code=400, detail=f"Invalid condition. Allowed values: {', '.join(allowed)}")
    if side == "buy" and condition not in ("more", "moreorequal"):
        raise HTTPException(status_code=400, detail="For side=buy condition must be more/moreorequal")
    if side == "sell" and condition not in ("less", "lessorequal"):
        raise HTTPException(status_code=400, detail="For side=sell condition must be less/lessorequal")

    td_mode = "cross" if allow_margin else "cash"

    # OKX не различает more / >= и less / <=
    trigger_px_type = "last"

    try:
        res = await adapter.place_stop_market_order(
            inst_id=symbol,
            inst_type=inst_type_s,
            side=side,
            sz=str(qty),
            trigger_px=str(trigger_price),
            trigger_px_type=trigger_px_type,
            td_mode=td_mode,
            algo_cl_ord_id=x_reqid,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    out = {
        "message": "success",
        "orderNumber": res.get("algoId") or res.get("orderId"),
    }
    _ORDER_IDEMPOTENCY_STOP[x_reqid] = out
    return out

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

    """await safe_send_json({
        "message": "Connected",
        "httpCode": 200,
    })"""

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

    async def wait_okx_subscribed_or_error(
        subscribed_ev: asyncio.Event,
        error_ev: asyncio.Event,
        guid: str,
        stop_ev: asyncio.Event,
        timeout_s: float = 5.0,
    ) -> bool:
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(subscribed_ev.wait()),
                asyncio.create_task(error_ev.wait()),
            ],
            timeout=timeout_s,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

        # если пришла ошибка — _handle_okx_ws_error уже отправляет ошибку и закрывает WS
        if error_ev.is_set():
            stop_ev.set()
            return False
        # если подписка подтверждена
        if subscribed_ev.is_set():
            return True
        # таймаут ожидания subscribe/error
        await safe_send_json(
            {
                "message": "OKX subscribe timeout",
                "httpCode": 504,
                "requestGuid": guid,
            }
        )
        stop_ev.set()
        return False

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
            # inst_type может приходить как instrumentGroup или как board
            inst_type = msg.get("instrumentGroup") or msg.get("board")

            if opcode == "authorize":
                auth_guid = msg.get("guid") or req_guid

                await safe_send_json(
                    {
                        "requestGuid": auth_guid,
                        "httpCode": 200,
                        "message": "The connection has been initialized.",
                    }
                )
                continue

            if opcode == "ping":
                ping_guid = msg["guid"]

                ev = subs.get(ping_guid)
                is_alive = ev is not None and not ev.is_set()

                await safe_send_json({
                    "opcode": "ping",
                    "guid": ping_guid,
                    "confirm": is_alive,
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
                    if o is None or h is None or l is None or c is None:
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

                async def send_order_astras(order_any: dict, existing_flag: bool, _guid=sub_guid, apply_status_filter: bool = False):
                    payload = _astras_order_simple_from_okx_neutral(
                        order_any,
                        exchange=exchange,
                        portfolio=portfolio,
                        existing=existing_flag,
                    )
                    # orderStatuses по документации Astras влияет только на первичную историю
                    if apply_status_filter and statuses:
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

                inst_type_msg = msg.get("instrumentGroup") or msg.get("board")
                inst_type_ws = str(inst_type_msg).strip().upper() if inst_type_msg else ""
                inst_type_rest = str(inst_type_msg).strip().upper() if inst_type_msg else None

                asyncio.create_task(
                    adapter.subscribe_orders(
                        symbols,
                        lambda ord_, _g=sub_guid: asyncio.create_task(_on_live_order(ord_, _g)),
                        stop,
                        inst_type=inst_type_ws,
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

                # REST history: orders-history (SPOT + FUTURES + SWAP)
                if not skip_history and hasattr(adapter, "get_orders_history"):
                    try:
                        inst_types = ["SPOT", "FUTURES", "SWAP"] if not inst_type_rest else [inst_type_rest]
                        for it in inst_types:
                            history = await adapter.get_orders_history(
                                inst_type=it,
                                limit=100,
                            )
                            for ho in history:
                                await send_order_astras(ho, True, sub_guid, apply_status_filter=True)
                    except Exception:
                        pass

                # история заявок (pending) через REST
                if (not skip_history) and hasattr(adapter, "get_orders_pending"):
                    try:
                        inst_types = ["SPOT", "FUTURES", "SWAP"] if not inst_type_rest else [inst_type_rest]
                        for it in inst_types:
                            history_orders = await adapter.get_orders_pending(
                                inst_type=it,
                                inst_id=(symbols[0] if symbols else None),
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

                data_format = msg.get("format")  # Astras присылает всегда: "Simple"/"Slim"/...
                data_format_norm = str(data_format).strip().lower()

                frequency = msg.get("frequency")
                try:
                    frequency = int(frequency) if frequency is not None else None
                except Exception:
                    frequency = None

                # если frequency не указан — ставим минимум по формату
                min_freq = 10 if data_format_norm == "slim" else 25

                if frequency is None:
                    frequency = min_freq
                elif frequency < min_freq:
                    frequency = min_freq


                sub_guid = msg.get("guid") or req_guid

                # если Astras переиспользовал guid — остановим старую подписку с этим guid
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()

                subs[sub_guid] = stop

                # эти поля Astras сейчас не влияют на OKX (оставляем как есть)
                exchange = msg.get("exchange")  #адаптер игнорирует
                instrument_group = msg.get("instrumentGroup")  #адаптер игнорирует

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

                    if data_format_norm == "slim":
                        payload = {
                            "b": [{"p": float(p), "v": float(v)} for (p, v) in bids_in],
                            "a": [{"p": float(p), "v": float(v)} for (p, v) in asks_in],
                            "t": ms_ts,
                            "h": bool(existing_flag),
                        }

                    elif data_format_norm == "simple":
                        payload = {
                            "snapshot": existing_flag,
                            "bids": [{"price": float(p), "volume": float(v)} for (p, v) in bids_in],
                            "asks": [{"price": float(p), "volume": float(v)} for (p, v) in asks_in],
                            "timestamp": ts_sec,
                            "ms_timestamp": ms_ts,
                            "existing": existing_flag,
                        }

                    await safe_send_json({"data": payload, "guid": _guid})

                if code:
                    # Сначала запускается подписка и ждём подтверждение/ошибку от OKX
                    subscribed_evt = asyncio.Event()
                    error_evt = asyncio.Event()

                    def _on_subscribed(_ev: dict):
                        subscribed_evt.set()

                    def _on_error(ev: dict):
                        # OKX event:error -> Astras error (реальный code/msg) + close WS
                        error_evt.set()
                        return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                    asyncio.create_task(
                        adapter.subscribe_order_book(
                            symbol=code,
                            depth=depth,
                            on_data=lambda b, _g=sub_guid: asyncio.create_task(on_book(b, _g)),
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

                    last_raw = t.get("last")
                    bid_raw = t.get("bid")
                    ask_raw = t.get("ask")

                    # пока OKX не прислал реальные цены — ничего не отправляем
                    if last_raw is None and bid_raw is None and ask_raw is None:
                        return
                    now_ms = int(time.time() * 1000)
                    freq_ms = frequency if isinstance(frequency, int) else 25

                    prev_ms = last_sent_ms_quotes.get(_guid, 0)
                    if freq_ms > 0 and (now_ms - prev_ms) < freq_ms:
                        return

                    last_sent_ms_quotes[_guid] = now_ms

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

            # все сделки по портфелю (Astras Heavy)
            if opcode == "TradesGetAndSubscribeV2":
                stop = asyncio.Event()
                active["fills"].append(stop)
                portfolio = msg.get("portfolio")  # у OKX виртуальный
                exchange_out = "OKX"
                skip_history = bool(msg.get("skipHistory", False))
                sub_guid = msg.get("guid") or req_guid
                # если Astras переиспользовал guid — остановим старую подписку
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()
                subs[sub_guid] = stop

                async def send_trade_astras(t: dict, existing_flag: bool, _guid: str):
                    ts_ms = int(t.get("ts", 0) or 0)
                    date_iso = _iso_from_unix_ms(ts_ms) if ts_ms else None
                    symbol = t.get("symbol") or "N/A"
                    qty = float(t.get("qty", 0) or 0)
                    price = float(t.get("price", 0) or 0)
                    payload = {
                        "id": str(t.get("id", "0")),
                        "orderno": str(t.get("orderno", "0")),
                        "comment": None,
                        "symbol": symbol,
                        "brokerSymbol": symbol,
                        "exchange": exchange_out,
                        "date": date_iso,
                        "board": None,
                        "qtyUnits": 0,
                        "qtyBatch": 0,
                        "qty": qty,
                        "price": price,
                        "accruedInt": 0,
                        "side": t.get("side", "0"),
                        "existing": bool(existing_flag),
                        "commission": t.get("commission", 0) or 0,
                        "repoSpecificFields": None,
                        "settleDate": None,
                        "volume": t.get("volume", 0) or 0, # volume / value = price * qty
                        "value": 0,
                    }
                    await safe_send_json({"data": payload, "guid": _guid})
                # события подписки OKX
                subscribed_evt = asyncio.Event()
                error_evt = asyncio.Event()
                history_done = False
                live_buffer: list[dict] = []

                def _on_subscribed(_ev: dict):
                    subscribed_evt.set()
                def _on_error(ev: dict):
                    error_evt.set()
                    return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))
                
                async def _on_live_trade(tr: dict, _guid: str):
                    nonlocal history_done
                    if not history_done:
                        live_buffer.append(tr)
                        return
                    await send_trade_astras(tr, False, _guid)

                inst_type_msg = msg.get("instrumentGroup") or msg.get("board")
                inst_type_ws = str(inst_type_msg).strip().upper() if inst_type_msg else ""
                inst_type_rest = str(inst_type_msg).strip().upper() if inst_type_msg else None

                asyncio.create_task(
                    adapter.subscribe_trades(
                        on_data=lambda tr, _g=sub_guid: asyncio.create_task(_on_live_trade(tr, _g)),
                        stop_event=stop,
                        inst_type=inst_type_ws,
                        on_subscribed=_on_subscribed,
                        on_error=_on_error,
                    )
                )

                # ждём реальный subscribe/error от OKX, и только потом шлём ACK(200)
                if not await wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
                    continue
                await send_ack_200(sub_guid)

                # история сделок (REST)
                if (not skip_history) and hasattr(adapter, "get_trades_history"):
                    try:
                        history = await adapter.get_trades_history(inst_type=inst_type_rest, limit=100)
                        for tr in history:
                            await send_trade_astras(tr, True, sub_guid)
                    except Exception:
                        pass

                history_done = True

                # буфер
                if live_buffer:
                    for tr in live_buffer:
                        await send_trade_astras(tr, False, sub_guid)
                    live_buffer.clear()

                continue
            
            # текущие позиции по инструментам и деньгам (Astras Simple)
            if opcode == "PositionsGetAndSubscribeV2":
                stop = asyncio.Event()
                active["positions"].append(stop)

                exchange_out = "UNITED"
                portfolio = msg.get("portfolio") or "0"
                skip_history = bool(msg.get("skipHistory", False))
                sub_guid = msg.get("guid") or req_guid

                inst_type = msg.get("instrumentGroup") or msg.get("board")
                inst_type_s = str(inst_type).strip().upper() if inst_type else None

                # если Astras переиспользовал guid — остановим старую подписку
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()
                subs[sub_guid] = stop

                async def send_pos_astras(p: dict, existing_flag: bool, _guid: str):
                    symbol = p.get("symbol")

                    # qtyUnits/avgPrice могут отсутствовать у OKX; тогда оставляем 0
                    qty_units_raw = p.get("qtyUnits")
                    avg_price_raw = p.get("avgPrice")

                    qty_units = float(qty_units_raw) if qty_units_raw is not None else 0.0
                    avg_price = float(avg_price_raw) if avg_price_raw is not None else 0.0

                    # volume/currentVolume: если OKX не дал — НЕ подставляем 0.0
                    # volume можем посчитать как avgPrice * qtyUnits, только если оба реально пришли
                    vol_raw = p.get("volume")
                    cur_vol_raw = p.get("currentVolume")

                    if vol_raw is not None:
                        volume = float(vol_raw)
                    elif qty_units_raw is not None and avg_price_raw is not None:
                        volume = float(avg_price_raw) * float(qty_units_raw)
                    else:
                        volume = None

                    current_volume = float(cur_vol_raw) if cur_vol_raw is not None else None

                    payload = {
                        "volume": volume,
                        "currentVolume": current_volume,

                        "symbol": symbol,
                        "brokerSymbol": f"{exchange_out}:{symbol}",
                        "portfolio": "DEV_portfolio",
                        "exchange": exchange_out,
                        "avgPrice": avg_price,
                        "qtyUnits": qty_units,
                        "openUnits": 0,
                        "lotSize": float(p.get("lotSize", 0) or 0),
                        "shortName": p.get("shortName") or symbol,

                        "qtyT0": 0,
                        "qtyT1": 0,
                        "qtyT2": 0,
                        "qtyTFuture": 0,

                        "qtyT0Batch": 0,
                        "qtyT1Batch": 0,
                        "qtyT2Batch": 0,
                        "qtyTFutureBatch": 0,

                        "qtyBatch": 0,
                        "openQtyBatch": 0,
                        "qty": qty_units,
                        "open": 0,

                        "dailyUnrealisedPl": 0,
                        "unrealisedPl": 0,

                        "isCurrency": bool(p.get("isCurrency", False)),
                        "existing": bool(existing_flag),
                    }

                    await safe_send_json({"data": payload, "guid": _guid})

                # события подписки OKX
                subscribed_evt = asyncio.Event()
                error_evt = asyncio.Event()

                # чтобы порядок был: ACK(200) -> snapshot -> live
                history_done = False
                live_buffer: list[dict] = []

                # live
                def _on_error(ev: dict):
                    error_evt.set()
                    return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                async def _on_live_pos(pos: dict, _guid: str):
                    nonlocal history_done
                    if not history_done:
                        live_buffer.append(pos)
                        return
                    await send_pos_astras(pos, False, _guid)

                def _on_subscribed(_ev: dict):
                    subscribed_evt.set()

                asyncio.create_task(
                    adapter.subscribe_positions(
                        on_data=lambda p, _g=sub_guid: asyncio.create_task(_on_live_pos(p, _g)),
                        stop_event=stop,
                        inst_type=inst_type_s,
                        on_error=_on_error,
                        on_subscribed=_on_subscribed,
                    )
                )
                if not await wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
                    continue
                await send_ack_200(sub_guid)

                # snapshot (existing=True)
                if not skip_history:
                    try:
                        snap = await adapter.get_positions_snapshot(inst_type=inst_type_s)
                        for pos in (snap or []):
                            await send_pos_astras(pos, True, sub_guid)
                    except Exception:
                        pass

                history_done = True
                # live из буфера
                if live_buffer:
                    for pos in live_buffer:
                        await send_pos_astras(pos, False, sub_guid)
                    live_buffer.clear()
                continue
        
            # сводная информация о портфеле/аккаунте (Astras Simple)
            if opcode == "SummariesGetAndSubscribeV2":
                stop = asyncio.Event()
                active["summaries"].append(stop)

                portfolio = msg.get("portfolio") or "0"
                skip_history = bool(msg.get("skipHistory", False))
                sub_guid = msg.get("guid") or req_guid

                # если Astras переиспользовал guid — остановим старую подписку
                old = subs.pop(sub_guid, None)
                if old:
                    old.set()
                subs[sub_guid] = stop

                async def send_summary_astras(s: dict, _guid: str):
                    total_eq = float(s.get("totalEq", 0) or 0)
                    avail_eq = float(s.get("availEq", 0) or 0)
                    imr = float(s.get("imr", 0) or 0)

                    by_ccy = s.get("byCcy") or []
                    buying_power_by_ccy = []
                    for x in by_ccy:
                        ccy = x.get("ccy")
                        if not ccy:
                            continue
                        buying_power_by_ccy.append({
                            "currency": str(ccy),
                            "buyingPower": float(x.get("availEq", 0) or 0),
                        })

                    payload = {
                        "buyingPowerAtMorning": 0,
                        "buyingPower": avail_eq,
                        "profit": 0,
                        "profitRate": 0,
                        "portfolioEvaluation": total_eq,
                        "portfolioLiquidationValue": total_eq,

                        "initialMargin": imr,
                        "correctedMargin": imr,
                        "riskBeforeForcePositionClosing": 0,

                        "commission": 0,
                        "buyingPowerByCurrency": buying_power_by_ccy,
                    }

                    await safe_send_json({"data": payload, "guid": _guid})

                subscribed_evt = asyncio.Event()
                error_evt = asyncio.Event()
                history_done = False
                live_buffer: list[dict] = []

                def _on_subscribed(_ev: dict):
                    subscribed_evt.set()
                def _on_error(ev: dict):
                    error_evt.set()
                    return asyncio.create_task(_handle_okx_ws_error(sub_guid, ev))

                async def _on_live_summary(s: dict, _guid: str):
                    nonlocal history_done
                    if not history_done:
                        live_buffer.append(s)
                        return
                    await send_summary_astras(s, _guid)

                # запуск подписки OKX (ошибки подписки должны приходить через on_error)
                asyncio.create_task(
                    adapter.subscribe_summaries(
                        on_data=lambda s, _g=sub_guid: asyncio.create_task(_on_live_summary(s, _g)),
                        stop_event=stop,
                        on_subscribed=_on_subscribed,
                        on_error=_on_error,
                    )
                )

                # ACK(200) только после реального subscribe от OKX
                if not await wait_okx_subscribed_or_error(subscribed_evt, error_evt, sub_guid, stop):
                    continue
                await send_ack_200(sub_guid)

                # snapshot
                if not skip_history:
                    try:
                        snap = await adapter.get_summaries_snapshot()
                        await send_summary_astras(snap or {}, sub_guid)
                    except Exception:
                        pass

                history_done = True

                # live из буфера
                if live_buffer:
                    for s in live_buffer:
                        await send_summary_astras(s, sub_guid)
                    live_buffer.clear()

                continue

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