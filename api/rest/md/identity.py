import json
from fastapi import APIRouter, Body, Query
from fastapi.responses import JSONResponse

from api import core

router = APIRouter()


@router.get("/identity/v5/UserSettings")
def user_settings(serviceName: str = Query(default="Astras"), key: str | None = None):
    v = core.USER_SETTINGS.get(key or "", "{}")
    return JSONResponse({"serviceName": serviceName, "key": key, "value": v})


@router.get("/identity/v5/UserSettings/group/widget-settings")
def widget_settings(serviceName: str = Query(default="Astras")):
    return JSONResponse([])


@router.post("/identity/v5/UserSettings")
@router.put("/identity/v5/UserSettings")
def user_settings_write(
    serviceName: str = Query(default="Astras"),
    key: str | None = None,
    payload: dict = Body(default={}),
):
    value = payload.get("value", payload)
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False)
    core.USER_SETTINGS[key or ""] = value
    return {"ok": True}


@router.delete("/identity/v5/UserSettings")
def user_settings_delete(
    serviceName: str = Query(default="Astras"),
    key: str | None = None,
):
    if key is not None:
        core.USER_SETTINGS.pop(key, None)
    return {"ok": True}


@router.post("/identity/v5/UserSettings/group/widget-settings")
@router.put("/identity/v5/UserSettings/group/widget-settings")
def widget_settings_write(payload: dict = Body(default={})):
    return {"ok": True}


@router.get("/client/v1.0/users/{user_id}/all-portfolios")
def all_portfolios(user_id: str):
    portfolio_id = "DEV_portfolio"
    return JSONResponse(
        [
            {
                "agreement": "424",
                "portfolio": portfolio_id,
                "tks": portfolio_id,
                "market": "OKX",
                "isVirtual": False,
            }
        ]
    )
