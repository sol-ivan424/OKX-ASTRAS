from typing import Any, Dict, List, Optional


class OkxRestOrderRiskMixin:
    async def get_max_order_size(
        self,
        inst_id: str,
        td_mode: str,
        ccy: Optional[str] = None,
        px: Optional[Any] = None,
    ) -> Dict[str, float]:
        params: Dict[str, Any] = {
            "instId": str(inst_id),
            "tdMode": str(td_mode),
        }
        if ccy is not None and str(ccy).strip():
            params["ccy"] = str(ccy).strip()
        if px is not None and str(px).strip():
            params["px"] = str(px).strip()
        raw = await self._request_private("GET", "/account/max-size", params=params)
        items = raw.get("data") or []
        it0 = items[0] if items else {}
        return {
            "maxBuy": self._to_float((it0 or {}).get("maxBuy")),
            "maxSell": self._to_float((it0 or {}).get("maxSell")),
        }

    async def get_max_loan(
        self,
        inst_id: str,
        mgn_mode: str,
        mgn_ccy: str,
    ) -> float:
        params: Dict[str, Any] = {
            "instId": str(inst_id),
            "mgnMode": str(mgn_mode),
            "mgnCcy": str(mgn_ccy),
        }
        raw = await self._request_private("GET", "/account/max-loan", params=params)
        items = raw.get("data") or []
        it0 = items[0] if items else {}
        return self._to_float((it0 or {}).get("maxLoan"))

    async def get_leverage_info(self, inst_id: str) -> List[Dict[str, Any]]:
        raw = await self._request_private(
            "GET",
            "/account/leverage-info",
            params={"instId": str(inst_id)},
        )
        return raw.get("data") or []
