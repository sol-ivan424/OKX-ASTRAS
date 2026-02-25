from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api import core

router = APIRouter()


@router.get("/md/v2/Clients/{exchange}/{portfolio}/summary")
async def md_client_summary(exchange: str, portfolio: str):
    snap = await core.adapter.get_summaries_snapshot()

    total_eq = float((snap or {}).get("totalEq", 0) or 0)
    adj_eq = float((snap or {}).get("adjEq", 0) or 0)
    avail_eq = float((snap or {}).get("availEq", 0) or 0)
    imr = float((snap or {}).get("imr", 0) or 0)

    by_ccy = (snap or {}).get("byCcy") or []
    buying_power_by_ccy = []
    details_imr_sum = 0.0
    for x in by_ccy:
        ccy = x.get("ccy")
        if not ccy:
            continue
        details_imr_sum += float(x.get("imr", 0) or 0)
        buying_power_by_ccy.append(
            {
                "currency": str(ccy),
                "buyingPower": float(x.get("availEq", 0) or 0),
            }
        )
    initial_margin = imr if imr != 0 else details_imr_sum

    payload = {
        "buyingPowerAtMorning": 0,
        "buyingPower": avail_eq,
        "profit": 0,
        "profitRate": 0,
        "portfolioEvaluation": adj_eq,
        "portfolioLiquidationValue": total_eq,
        "initialMargin": initial_margin,
        "correctedMargin": 0,
        "riskBeforeForcePositionClosing": 0,
        "commission": 0,
        "buyingPowerByCurrency": buying_power_by_ccy,
    }
    return JSONResponse(payload)


@router.get("/md/v2/Clients/{exchange}/{portfolio}/risk")
async def md_client_risk(exchange: str, portfolio: str):
    snap = await core.adapter.get_summaries_snapshot()

    total_eq = float((snap or {}).get("totalEq", 0) or 0)
    imr = float((snap or {}).get("imr", 0) or 0)
    mmr = float((snap or {}).get("mmr", 0) or 0)

    by_ccy = (snap or {}).get("byCcy") or []
    details_imr_sum = 0.0
    for x in by_ccy:
        details_imr_sum += float(x.get("imr", 0) or 0)
    initial_margin = imr if imr != 0 else details_imr_sum

    payload = {
        "portfolio": portfolio,
        "exchange": exchange,
        "portfolioEvaluation": total_eq,
        "portfolioLiquidationValue": 0,
        "initialMargin": initial_margin,
        "minimalMargin": mmr,
        "correctedMargin": 0,
        "riskCoverageRatioOne": 0,
        "riskCoverageRatioTwo": 0,
        "riskCategoryId": 0,
        "clientType": None,
        "hasForbiddenPositions": False,
        "hasNegativeQuantity": False,
        "riskStatus": None,
        "calculationTime": None,
    }
    return JSONResponse(payload)


@router.get("/md/v2/clients/{client_id}/positions")
async def md_client_positions(
    client_id: str,
    exchange: str | None = None,
    portfolio: str | None = None,
    format: str = "Simple",
    withoutCurrency: bool = False,
    jsonResponse: bool = False,
):
    exchange_out = exchange or "OKX"
    portfolio_out = portfolio or "DEV_portfolio"
    result: list[dict] = []
    try:
        snapshot = await core.adapter.get_positions_snapshot(inst_type=None)
        for p in snapshot or []:
            if withoutCurrency and p.get("isCurrency"):
                continue
            symbol = p.get("symbol")
            qty_units = float(p.get("qtyUnits", 0.00) or 0.00)
            avg_price = float(p.get("avgPrice", 0) or 0)
            volume = 0.00
            current_volume = 0
            result.append(
                {
                    "volume": volume,
                    "currentVolume": current_volume,
                    "symbol": symbol,
                    "brokerSymbol": f"{exchange_out}:{symbol}",
                    "portfolio": portfolio_out,
                    "exchange": exchange_out,
                    "avgPrice": avg_price,
                    "qtyUnits": qty_units,
                    "openUnits": 0.00,
                    "lotSize": float(p.get("lotSize", 0) or 0),
                    "shortName": p.get("shortName") or symbol,
                    "qtyT0": qty_units,
                    "qtyT1": qty_units,
                    "qtyT2": qty_units,
                    "qtyTFuture": qty_units,
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
                    "existing": True,
                }
            )
    except Exception:
        result = []
    return JSONResponse(result)
