from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from api import astras
from api import core
from api import instruments_cache

router = APIRouter()


@router.get("/md/v2/Clients/{exchange}/{portfolio}/orders")
@router.get("/md/v2/clients/{exchange}/{portfolio}/orders")
async def md_client_orders(exchange: str, portfolio: str):
    result: list[dict] = []
    try:
        inst_types = ["SPOT", "FUTURES", "SWAP"]
        for it in inst_types:
            history = await core.adapter.get_orders_history(
                inst_type=it,
                limit=100,
            )
            for o in history or []:
                result.append(
                    astras.astras_order_simple_from_okx_neutral(
                        o,
                        exchange=exchange,
                        portfolio=portfolio,
                        existing=True,
                    )
                )
        for it in inst_types:
            pending = await core.adapter.get_orders_pending(
                inst_type=it,
                inst_id=None,
            )
            for o in pending or []:
                result.append(
                    astras.astras_order_simple_from_okx_neutral(
                        o,
                        exchange=exchange,
                        portfolio=portfolio,
                        existing=True,
                    )
                )
    except Exception:
        pass
    return JSONResponse(result)


@router.get("/md/v2/Clients/{exchange}/{portfolio}/orders/{orderId}")
@router.get("/md/v2/clients/{exchange}/{portfolio}/orders/{orderId}")
async def md_client_order_by_id(exchange: str, portfolio: str, orderId: str):
    found: dict | None = None
    try:
        inst_types = ["SPOT", "FUTURES", "SWAP"]

        for it in inst_types:
            pending = await core.adapter.get_orders_pending(inst_type=it, inst_id=None)
            for o in pending or []:
                if str(o.get("id") or "") == str(orderId):
                    found = astras.astras_order_simple_from_okx_neutral(
                        o,
                        exchange=exchange,
                        portfolio=portfolio,
                        existing=True,
                    )
                    break
            if found is not None:
                break

        if found is None:
            for it in inst_types:
                history = await core.adapter.get_orders_history(inst_type=it, limit=100)
                for o in history or []:
                    if str(o.get("id") or "") == str(orderId):
                        found = astras.astras_order_simple_from_okx_neutral(
                            o,
                            exchange=exchange,
                            portfolio=portfolio,
                            existing=True,
                        )
                        break
                if found is not None:
                    break
    except Exception:
        found = None

    if found is None:
        raise HTTPException(status_code=404, detail=f"Order '{orderId}' not found")

    instr = await instruments_cache.get_instr(found.get("symbol", "") or "", adapter=core.adapter)
    found["board"] = (instr or {}).get("instType") or "0"

    tif = found.get("timeInForce")
    if isinstance(tif, str) and tif:
        found["timeInForce"] = tif.lower()

    return JSONResponse(found)
