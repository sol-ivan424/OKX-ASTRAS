from typing import Any, Dict, List, Optional


class OkxRestOrderTradesMixin:
    async def get_trades_history(
        self,
        inst_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        def _norm_inst_type(x: Optional[str]) -> Optional[str]:
            if x is None:
                return None
            s = str(x).strip().upper()
            return s or None

        it = _norm_inst_type(inst_type)
        if it is None:
            inst_types = ["SPOT", "SWAP", "FUTURES"]
        else:
            inst_types = [it]

        out: List[Dict[str, Any]] = []
        for one_type in inst_types:
            params: Dict[str, str] = {"instType": one_type, "limit": str(int(limit))}
            raw = await self._request_private("GET", "/trade/fills-history", params=params)
            for item in raw.get("data", []) or []:
                t = self._parse_okx_trade_any(item, is_history=True, inst_type=one_type)
                out.append(t)
        out.sort(key=lambda x: int(x.get("ts", 0) or 0))
        return out
