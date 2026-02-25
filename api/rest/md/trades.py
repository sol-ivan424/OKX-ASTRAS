import datetime
import time
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from api import astras
from api import core

router = APIRouter()


@router.get("/md/v2/Clients/{exchange}/{portfolio}/trades")
async def md_client_trades(exchange: str, portfolio: str, format: str = "heavy"):
    result: list[dict] = []
    cutoff_ms = int(time.time() * 1000) - 24 * 60 * 60 * 1000
    inst_types = ["SPOT", "FUTURES", "SWAP"]
    for it in inst_types:
        trades = await core.adapter.get_trades_history(inst_type=it, limit=100)
        for t in trades or []:
            ts_ms = t.get("ts") or t.get("fillTime") or t.get("ts_fill")
            try:
                ts_ms_i = int(ts_ms) if ts_ms is not None else 0
            except Exception:
                ts_ms_i = 0
            if ts_ms_i <= 0 or ts_ms_i < cutoff_ms:
                continue

            symbol = t.get("symbol") or t.get("instId") or "[N/A]"
            inst_id = t.get("instId") or t.get("inst_id") or t.get("symbol")
            currency = None
            if inst_id and "-" in str(inst_id):
                parts = [p for p in str(inst_id).split("-") if p]
                if len(parts) >= 2:
                    currency = parts[1].strip() or None

            date_iso = t.get("date") or astras.iso_from_unix_ms(ts_ms_i)
            price = t.get("price")
            qty_units = t.get("qtyUnits")
            if qty_units is None:
                qty_units = t.get("qty")

            try:
                qty_units_f = float(qty_units) if qty_units is not None else 0.0
            except Exception:
                qty_units_f = 0.0
            if qty_units_f <= 0:
                continue

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
                    "settleDate": t.get("settleDate"),
                    "value": value,
                }
            )
    return JSONResponse(result)


@router.get("/md/v2/Stats/{exchange}/{portfolio}/history/trades")
@router.get("/md/v2/stats/{exchange}/{portfolio}/history/trades")
async def md_stats_history_trades(
    exchange: str,
    portfolio: str,
    instrumentGroup: str | None = None,
    dateFrom: str | None = None,
    ticker: str | None = None,
    from_: int | None = Query(default=None, alias="from"),
    limit: int = 50,
    orderByTradeDate: bool = False,
    descending: bool = False,
    withRepo: bool = False,
    side: str | None = None,
    format: str = "heavy",
    jsonResponse: bool = False,
):
    _ = withRepo, format, jsonResponse
    limit_i = max(1, min(limit, 1000))
    inst_group = str(instrumentGroup or "").strip().upper()
    inst_types = [inst_group] if inst_group in ("SPOT", "FUTURES", "SWAP") else ["SPOT", "FUTURES", "SWAP"]
    ticker_s = str(ticker or "").strip().upper().replace("_", "-")
    side_s = str(side or "").strip().lower()
    from_i = from_
    cutoff_ms = int(time.time() * 1000) - 24 * 60 * 60 * 1000

    date_from_ms: int | None = None
    if dateFrom:
        s = str(dateFrom).strip()
        try:
            if "T" not in s:
                s = f"{s}T00:00:00"
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            date_from_ms = int(dt.timestamp() * 1000)
        except Exception:
            date_from_ms = None

    rows: list[tuple[int, int, dict]] = []
    for it in inst_types:
        after_cursor: str | None = None
        while len(rows) < limit_i:
            page, next_after = await core.adapter.get_fills_history_page(
                inst_type=it,
                limit=100,
                after=after_cursor,
            )
            if not page:
                break

            for t in page:
                ts_ms = t.get("ts") or t.get("fillTime") or t.get("ts_fill")
                try:
                    ts_ms_i = int(ts_ms) if ts_ms is not None else 0
                except Exception:
                    ts_ms_i = 0
                if ts_ms_i <= 0:
                    continue
                if ts_ms_i >= cutoff_ms:
                    continue
                if date_from_ms is not None and ts_ms_i < date_from_ms:
                    continue

                symbol = t.get("symbol") or t.get("instId") or "[N/A]"
                symbol_s = str(symbol).upper().replace("_", "-")
                if ticker_s and symbol_s != ticker_s:
                    continue
                if side_s and str(t.get("side") or "").lower() != side_s:
                    continue

                try:
                    trade_id_i = int(str(t.get("id") or "0"))
                except Exception:
                    trade_id_i = 0
                if from_i is not None:
                    if descending and trade_id_i > from_i:
                        continue
                    if (not descending) and trade_id_i < from_i:
                        continue

                inst_id = t.get("instId") or t.get("inst_id") or t.get("symbol")
                currency = None
                if inst_id and "-" in str(inst_id):
                    parts = [p for p in str(inst_id).split("-") if p]
                    if len(parts) >= 2:
                        currency = parts[1].strip() or None

                date_iso = t.get("date") or astras.iso_from_unix_ms(ts_ms_i)
                price = t.get("price")
                qty_units = t.get("qtyUnits")
                if qty_units is None:
                    qty_units = t.get("qty")
                try:
                    qty_units_f = float(qty_units) if qty_units is not None else 0.0
                except Exception:
                    qty_units_f = 0.0
                if qty_units_f <= 0:
                    continue

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

                payload = {
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
                    "settleDate": t.get("settleDate"),
                    "value": value,
                }
                rows.append((ts_ms_i, trade_id_i, payload))
                if len(rows) >= limit_i:
                    break

            if len(rows) >= limit_i:
                break
            after_cursor = next_after
            if len(page) < 100 or not after_cursor:
                break
        if len(rows) >= limit_i:
            break

    key_idx = 0 if orderByTradeDate else 1
    rows.sort(key=lambda x: x[key_idx], reverse=bool(descending))
    out = [x[2] for x in rows[:limit_i]]
    return JSONResponse(out)
