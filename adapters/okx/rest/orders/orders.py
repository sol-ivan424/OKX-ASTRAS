from typing import Any, Dict, List, Optional


class OkxRestOrderMethodsMixin:
    async def get_orders_pending(
        self,
        inst_type: str = "SPOT",
        inst_id: Optional[str] = None,
        ord_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)
        if it == "SPOT":
            inst_types = ["SPOT", "MARGIN"]
        elif it in ("SWAP", "FUTURES", "MARGIN"):
            inst_types = [it]
        else:
            inst_types = ["SPOT", "SWAP", "FUTURES", "MARGIN"]

        out: List[Dict[str, Any]] = []
        for one_type in inst_types:
            params: Dict[str, Any] = {"instType": one_type, "limit": str(int(limit))}
            if inst_id:
                params["instId"] = inst_id
            if ord_type:
                params["ordType"] = ord_type
            if state:
                params["state"] = state
            raw = await self._request_private("GET", "/trade/orders-pending", params=params)
            for item in raw.get("data", []) or []:
                out.append(self._parse_okx_order_any(item))
        out.sort(key=lambda x: int(x.get("ts_update", 0) or 0))
        return out

    async def get_orders_history(
        self,
        inst_type: Optional[str] = None,
        inst_id: Optional[str] = None,
        ord_type: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)
        if it is None:
            inst_types = ["SPOT", "SWAP", "FUTURES", "MARGIN"]
        elif it == "SPOT":
            inst_types = ["SPOT", "MARGIN"]
        else:
            inst_types = [it]

        out: List[Dict[str, Any]] = []
        for one_type in inst_types:
            params: Dict[str, Any] = {
                "instType": one_type,
                "limit": str(int(limit)),
            }
            if inst_id:
                params["instId"] = str(inst_id)
            if ord_type:
                params["ordType"] = str(ord_type)
            if state:
                params["state"] = str(state)
            raw = await self._request_private("GET", "/trade/orders-history", params=params)
            for item in raw.get("data", []) or []:
                out.append(self._parse_okx_order_any(item))

        out.sort(key=lambda x: int(x.get("ts_update", 0) or 0))
        return out
