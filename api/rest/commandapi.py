from decimal import Decimal, InvalidOperation, ROUND_DOWN
from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse
from api import core
from api import instruments_cache

router = APIRouter()


@router.post("/commandapi/observatory/subscriptions/actions/addToken")
def add_token(payload: dict = Body(default={})):
    return {"ok": True}


@router.post("/commandapi/warptrans/FX1/v2/client/orders/estimate")
@router.post("/commandapi/warptrans/TRADE/v2/client/orders/estimate")
async def cmd_orders_estimate(request: Request):
    body = await request.json()

    portfolio = str(body.get("portfolio") or "").strip()
    ticker = str(body.get("ticker") or "").strip()
    exchange = str(body.get("exchange") or "OKX").strip()
    board = str(body.get("board") or "").strip().upper()

    if not portfolio:
        raise HTTPException(status_code=400, detail="portfolio is required")
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")

    instr = await instruments_cache.get_instr(ticker, adapter=core.adapter)
    inst_type_s = (
        board
        if board in core.SUPPORTED_BOARDS
        else str((instr or {}).get("instType") or "").strip().upper()
    )
    if inst_type_s not in core.SUPPORTED_BOARDS:
        raise HTTPException(status_code=400, detail="Unsupported board/instrumentGroup")

    price_raw = body.get("price")
    lot_qty_raw = body.get("lotQuantity")
    price_f: float | None = None
    lot_qty_f: float | None = None
    if price_raw is not None and str(price_raw).strip() != "":
        try:
            price_f = float(price_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="price must be number")
    if lot_qty_raw is not None and str(lot_qty_raw).strip() != "":
        try:
            lot_qty_f = float(lot_qty_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="lotQuantity must be number")

    lot_step = str((instr or {}).get("lotSz") or "0")

    def _round_to_step(v: float, step: str) -> float:
        try:
            dv = Decimal(str(v))
            ds = Decimal(str(step))
        except (InvalidOperation, ValueError, TypeError):
            return float(v)
        if ds <= 0:
            return float(str(dv))
        q = (dv / ds).to_integral_value(rounding=ROUND_DOWN) * ds
        scale = max(0, -ds.as_tuple().exponent)
        return float(f"{q:.{scale}f}")

    quantity_to_buy = 0.0
    quantity_to_sell = 0.0
    not_margin_quantity_to_buy = 0.0
    not_margin_quantity_to_sell = 0.0

    px_arg = None if price_f is None else str(price_f)

    async def _safe_max_size(td_mode: str, ccy: str | None = None) -> dict:
        try:
            return await core.adapter.get_max_order_size(
                inst_id=ticker,
                td_mode=td_mode,
                ccy=ccy,
                px=px_arg,
            )
        except Exception:
            return {}

    if inst_type_s == "SPOT":
        parts = [p.strip().upper() for p in ticker.split("-")]
        base_ccy = parts[0] if len(parts) >= 1 else None
        quote_ccy = parts[1] if len(parts) >= 2 else None

        if quote_ccy:
            cross_buy = await _safe_max_size("cross", quote_ccy)
            quantity_to_buy = float(cross_buy.get("maxBuy", 0) or 0)
        if base_ccy:
            cross_sell = await _safe_max_size("cross", base_ccy)
            quantity_to_sell = float(cross_sell.get("maxSell", 0) or 0)

        cash_sz = await _safe_max_size("cash")
        not_margin_quantity_to_buy = float(cash_sz.get("maxBuy", 0) or 0)
        if base_ccy:
            try:
                balances = await core.adapter.get_account_balances()
                not_margin_quantity_to_sell = sum(
                    float(x.get("availBal", 0) or 0)
                    for x in (balances or [])
                    if str(x.get("ccy") or "").upper() == base_ccy
                )
            except Exception:
                pass
    else:
        der_sz = await _safe_max_size("cross")
        quantity_to_buy = float(der_sz.get("maxBuy", 0) or 0)
        quantity_to_sell = float(der_sz.get("maxSell", 0) or 0)

    quantity_to_buy = _round_to_step(quantity_to_buy, lot_step)
    quantity_to_sell = _round_to_step(quantity_to_sell, lot_step)
    not_margin_quantity_to_buy = _round_to_step(not_margin_quantity_to_buy, lot_step)
    not_margin_quantity_to_sell = _round_to_step(not_margin_quantity_to_sell, lot_step)

    if price_f is not None and lot_qty_f is not None:
        if inst_type_s == "SPOT":
            order_evaluation = price_f * lot_qty_f
        elif inst_type_s in ("FUTURES", "SWAP"):
            ct_val = float((instr or {}).get("ctVal") or 0)
            ct_val_ccy = str((instr or {}).get("ctValCcy") or "").strip().upper()
            quote_ccy = str((instr or {}).get("quoteCcy") or "").strip().upper()
            if ct_val > 0:
                order_evaluation = (
                    (ct_val * lot_qty_f)
                    if (ct_val_ccy and quote_ccy and ct_val_ccy == quote_ccy)
                    else (price_f * ct_val * lot_qty_f)
                )
            else:
                order_evaluation = 0.0
        else:
            order_evaluation = 0.0
    else:
        order_evaluation = 0.0
    commission = 0.0
    buy_price = price_f if price_f is not None else None

    out = {
        "portfolio": portfolio,
        "ticker": ticker,
        "exchange": exchange,
        "board": board or inst_type_s,
        "quantityToSell": quantity_to_sell,
        "quantityToBuy": quantity_to_buy,
        "notMarginQuantityToSell": not_margin_quantity_to_sell,
        "notMarginQuantityToBuy": not_margin_quantity_to_buy,
        "orderEvaluation": order_evaluation,
        "commission": commission,
        "buyPrice": buy_price,
        "isUnitedPortfolio": False,
    }
    return JSONResponse(out)


@router.get("/commandapi/warptrans/FX1/v2/client/orders/clientsRisk")
@router.get("/commandapi/warptrans/TRADE/v2/client/orders/clientsRisk")
async def cmd_orders_clients_risk(
    portfolio: str,
    ticker: str,
    exchange: str = "OKX",
    board: str | None = None,
):
    portfolio = str(portfolio or "").strip()
    ticker = str(ticker or "").strip()
    exchange = str(exchange or "OKX").strip()
    board_s = str(board or "").strip().upper()

    instr = await instruments_cache.get_instr(ticker, adapter=core.adapter)
    inst_type_s = (
        board_s
        if board_s in core.SUPPORTED_BOARDS
        else str((instr or {}).get("instType") or "SPOT").strip().upper()
    )

    is_marginal = False
    is_short_sell_possible = False
    long_multiplier = 0.0
    short_multiplier = 0.0

    if inst_type_s == "SPOT":
        base_ccy, quote_ccy = [p.strip().upper() for p in ticker.split("-", 1)]
        try:
            await core.adapter.get_max_order_size(
                inst_id=ticker,
                td_mode="cross",
                ccy=quote_ccy,
            )
            is_marginal = True

            max_loan = await core.adapter.get_max_loan(
                inst_id=ticker,
                mgn_mode="cross",
                mgn_ccy=base_ccy,
            )
            is_short_sell_possible = max_loan > 0
        except Exception:
            pass
    else:
        is_marginal = True
        is_short_sell_possible = True

    try:
        lev_rows = await core.adapter.get_leverage_info(ticker)
        lev_long = None
        lev_short = None
        for row in lev_rows:
            pos_side = str((row or {}).get("posSide") or "").strip().lower()
            lever_raw = (row or {}).get("lever")
            if lever_raw is None or str(lever_raw).strip() == "":
                continue
            lever = float(lever_raw)
            if pos_side == "long":
                lev_long = lever
            elif pos_side == "short":
                lev_short = lever
        if lev_long is not None and lev_short is not None:
            long_multiplier = lev_long
            short_multiplier = lev_short
    except Exception:
        pass

    out = {
        "portfolio": portfolio,
        "ticker": ticker,
        "exchange": exchange,
        "isMarginal": is_marginal,
        "isShortSellPossible": is_short_sell_possible,
        "longMultiplier": long_multiplier,
        "shortMultiplier": short_multiplier,
        "currencyLongMultiplier": 0.0,
        "currencyShortMultiplier": 0.0,
        "isUnitedPortfolio": False,
    }
    return JSONResponse(out)
